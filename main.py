#!/usr/bin/env python

import os

from dotenv import load_dotenv

from ctrader_open_api import Client, Protobuf, TcpProtocol, Auth, EndPoints
from ctrader_open_api.endpoints import EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
from twisted.internet import reactor
import utility
import fileinput
import threading
import time

load_dotenv(".env_demo")
utility.read_config_file()

# From .env file, get the variable
APP_CLIENT_ID = os.getenv('APP_CLIENT_ID')
APP_CLIENT_SECRET = os.getenv('APP_CLIENT_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCOUNT_TYPE = os.getenv('ACCOUNT_TYPE')
CURRENT_CTIDTRADERACCOUNTID = int(os.getenv('CURRENT_ACCOUNT_ID'))

# To decide to write into CSV or not, for subscribe function
g_write_CSV = False
# To print "Heartbeat received"
g_heartbeat = True

# List of payload to ignore
gPayloadIgnoreList = [
    ProtoOASubscribeSpotsRes().payloadType,
    ProtoOAAccountLogoutRes().payloadType,
    # ProtoHeartbeatEvent().payloadType,
    ProtoOAExecutionEvent().payloadType
]
gTimer = utility.Timer(30)  # Set timer for 1 min
gData = []

if __name__ == "__main__":
    hostType = ACCOUNT_TYPE
    hostType = hostType.lower()
    appClientId = APP_CLIENT_ID
    appClientSecret = APP_CLIENT_SECRET

    client = Client(EndPoints.PROTOBUF_LIVE_HOST if hostType.lower() == "live" else EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)

    def connected(client): # Callback for client connection
        print(f"\nConnected. ACCOUNT_TYPE:{ACCOUNT_TYPE}")
        request = ProtoOAApplicationAuthReq()
        request.clientId = appClientId
        request.clientSecret = appClientSecret
        deferred = client.send(request)
        deferred.addErrback(onError)

    def disconnected(client, reason): # Callback for client disconnection
        print(f"\nDisconnected: {reason}")

    def onMessageReceived(client, message): # Callback for receiving all messages
        if message.payloadType in gPayloadIgnoreList:
            return
        elif message.payloadType == ProtoHeartbeatEvent().payloadType:
            if g_heartbeat:
                print(f"[{time.time()}] Heartbeat Received.")
            return
        elif message.payloadType == ProtoOAApplicationAuthRes().payloadType:
            print(f"API Application authorized")
            if CURRENT_CTIDTRADERACCOUNTID is not None:
                sendProtoOAAccountAuthReq()
                return
        elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
            protoOAAccountAuthRes = Protobuf.extract(message)
            print(f"Account {protoOAAccountAuthRes.ctidTraderAccountId} has been authorized")

        elif message.payloadType == ProtoOASymbolsListRes().payloadType:
            res = Protobuf.extract(message)
            symbol_data = res.symbol
            filename = "symbolList_" + ACCOUNT_TYPE + ".txt"
            with open(filename, "w") as file:
                file.write(str(symbol_data))
            utility.convert_txt_to_json(filename, ACCOUNT_TYPE)

        elif message.payloadType == ProtoOARefreshTokenRes().payloadType:
            res = Protobuf.extract(message)
            updates = {"ACCESS_TOKEN":res.accessToken, "REFRESH_TOKEN":res.refreshToken}
            res = None
            with fileinput.FileInput(".env", inplace=True) as file:
                for line in file:
                    key, seperator, value = line.partition("=")  # Extract key-value pairs
                    if key in updates:
                        print(f"{key}=\"{updates[key]}\"")  # Replace the line with new value
                    else:
                        print(line, end="")  # Keep other lines unchanged
            updates = None
            refresh_RAM()
            print("New accessToken & refreshToken updated")

        elif message.payloadType == ProtoOASpotEvent().payloadType:
            global gData
            global g_write_CSV
            res = Protobuf.extract(message)
            symbol = utility.read_symbol_id(res.symbolId, ACCOUNT_TYPE)["symbolName"]
            
            # gData.append([res.symbolId, symbol, res.bid, res.ask, res.timestamp])
            # while len(gData) > 51:
            #     gData.pop(0)
            # print(f"gData size : {len(gData)}")
            # if gTimer.timer_expired():
            #     utility.write_csv(gData)
            #     gData.clear()
            #     print(f"gData size : {len(gData)}")
                
            gData = [
                [res.symbolId, symbol, res.bid, res.ask, res.timestamp]
            ]
            if g_write_CSV:
                utility.write_csv(gData)
            else:
                print(gData)
            gData.clear()

        else:
            payloadName = ProtoOAPayloadType.Name(message.payloadType)
            print(f"Message received: payloadType = {message.payloadType} ({payloadName})")
            print("\n", Protobuf.extract(message))

    def onError(failure): # Call back for errors
        print("Message Error: ", failure)

    def showHelp():
        print("im too lazy to write, you should know better")

    def sendProtoOAVersionReq(clientMsgId = None):
        request = ProtoOAVersionReq()
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAUnsubscribeSpotsReq(symbolId, clientMsgId = None):
        request = ProtoOAUnsubscribeSpotsReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.symbolId.append(int(symbolId))
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOASubscribeSpotsReq(symbolId, write_CSV = False, timeInSeconds=1, subscribeToSpotTimestamp = True, clientMsgId = None):
        """
        Subscribe = Get
        Spot = the current price
        Output of this function

        ctidTraderAccountId: xxxxx
        symbolId: 41
        bid: 318645000
        ask: 318677000
        
        41 = XAUUSD
        135 = NDXUSD
        133 = DJIUSD
        127 = DAX
        """
        global g_write_CSV
        g_write_CSV = int(write_CSV)
            
        request = ProtoOASubscribeSpotsReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.symbolId.append(int(symbolId))
        request.subscribeToSpotTimestamp = subscribeToSpotTimestamp if type(subscribeToSpotTimestamp) is bool else bool(subscribeToSpotTimestamp)
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)
        # reactor.callLater(int(timeInSeconds), sendProtoOAUnsubscribeSpotsReq, symbolId)

    def disconnect(clientMsgId=None): # Disconnect the client
        client._disconnected("User exited the connection")

    def sendProtoOAAccountAuthReq(clientMsgId = None):
        request = ProtoOAAccountAuthReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.accessToken = ACCESS_TOKEN
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def getSymbolList(clientMsgId=None):
        request = ProtoOASymbolsListReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def refresh_RAM():
        """
        To reload the config.ini & .env into the RAM
        """
        global ACCESS_TOKEN
        utility.read_config_file(True)
        load_dotenv(override=True)
        ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')

    def renewAccessToken(clientMsgId=None):
        request = ProtoOARefreshTokenReq()
        request.refreshToken = os.getenv("REFRESH_TOKEN")
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)
        
    def setPrintHeartbeat(value, clientMsgId=None):
        global g_heartbeat
        g_heartbeat = int(value)

    def test(clientMsgId=None):
        request = ProtoHeartbeatEvent()
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    commands = {
        "help": showHelp,
        "ver": sendProtoOAVersionReq, # Show version
        "sub": sendProtoOASubscribeSpotsReq, # subscribe to asset, get bid & ask price, call like this `sub 41 1`
        "unsub": sendProtoOAUnsubscribeSpotsReq, # Unsubscribe asset, stop getting data, call like this `unsub 41`
        "renew": renewAccessToken, # Renew access & refresh token
        "hb" : setPrintHeartbeat, # set print hearbeat True or false, call like this `hb 1`
        "qq": disconnect,
        "s": getSymbolList, # Update symbol files
        "r": refresh_RAM, # Refresh global variable with latest value
        "test": test,
    }

    def executeUserCommand():
        while True:
            print("\n=====================================\n")
            userInput = input("Command (ex help): ")
            userInputSplit = userInput.split(" ")
            if not userInputSplit:
                print("Command split error: ", userInput)
                continue
            command = userInputSplit[0]
            try:
                parameters = [parameter if parameter[0] != "*" else parameter[1:] for parameter in userInputSplit[1:]]
            except:
                print("Invalid parameters: ", userInput)
                continue
            if command in commands:
                commands[command](*parameters)
            else:
                print("Invalid Command: ", userInput)
                continue

    # Start user console command
    thread = threading.Thread(target=executeUserCommand)
    thread.start()
    
    # Setting optional client callbacks
    client.setConnectedCallback(connected)
    client.setDisconnectedCallback(disconnected)
    client.setMessageReceivedCallback(onMessageReceived)
    # Starting the client service
    client.startService()
    reactor.run()