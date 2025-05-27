#!/usr/bin/env python

import os

from dotenv import load_dotenv

from ctrader_open_api import Client, Protobuf, TcpProtocol, Auth, EndPoints
from ctrader_open_api.endpoints import EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
from twisted.internet import reactor
import datetime
from datetime import datetime, timezone
import pytz
import utility
from enum import Enum
import fileinput
import threading
import time
import running_position

# In an order, it has relative stop loss or absolute stop loss
# YOu have to choose one side
class StopLossTakeProfit(Enum):
    RELATIVE = 1
    ABSOLUTE = 2

    @classmethod
    def getName(cls, value):
        for key in cls:
            if key.value == value:
                return key.name
        return None

load_dotenv()
utility.read_config_file()

g_heartbeat = True
g_mytimezone = pytz.timezone("Asia/Singapore")

# For RunningPosition class objects
g_running_position_obj_threads = []



# From .env file, get the variable
APP_CLIENT_ID = os.getenv('APP_CLIENT_ID')
APP_CLIENT_SECRET = os.getenv('APP_CLIENT_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCOUNT_TYPE = os.getenv('ACCOUNT_TYPE')
CURRENT_CTIDTRADERACCOUNTID = int(os.getenv('CURRENT_ACCOUNT_ID'))

# List of payload to ignore
gPayloadIgnoreList = [
    ProtoOASubscribeSpotsRes().payloadType,
    ProtoOAAccountLogoutRes().payloadType,
    ProtoHeartbeatEvent().payloadType,
    # ProtoOAExecutionEvent().payloadType
]

# When you run `acc`, it will set this to TRUE
# Then it will set it back to false
# When you run `auth`, it wont modify this variable,
# leads to authenticating your acc
gAuthPrintOnly = False
# For my conveniences of `set 1`, `set 2`, set accounts by just typing 1 num
g_auth_acc = []

hostType = ACCOUNT_TYPE
hostType = hostType.lower()
appClientId = APP_CLIENT_ID
appClientSecret = APP_CLIENT_SECRET

client = Client(EndPoints.PROTOBUF_LIVE_HOST if hostType.lower() == "live" else EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)

def sendProtoOASubscribeSpotsReq(symbolId, clientMsgId = None):
    """
    ctidTraderAccountId: xxxxx
    symbolId: 41
    bid: 318645000
    ask: 318677000

    If already subscribed, dont subscribe again, subscribe once,
    the server will keep sending you data already
    """

    request = ProtoOASubscribeSpotsReq()
    request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
    request.symbolId.append(int(symbolId))
    request.subscribeToSpotTimestamp = False
    deferred = client.send(request, clientMsgId = clientMsgId)
    deferred.addErrback(onError)

def sendProtoOAUnsubscribeSpotsReq(symbolId, clientMsgId = None):
    """
    This is UNSUBSCRIBE
    """
    request = ProtoOAUnsubscribeDepthQuotesReq()
    request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
    request.symbolId.append(int(symbolId))
    deferred = client.send(request, clientMsgId = clientMsgId)
    deferred.addErrback(onError)


def onError(failure): # Call back for errors
    print("Message Error: ", failure)

if __name__ == "__main__":

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
        elif message.payloadType == ProtoOAExecutionEvent().payloadType:
            """
            To detect whether buy/sell limit is hit & entered trade
            And to detect whether the position is still running
            !Note! If you have 0.05 lot, you tpp 0.03 lot
            !Note! Then you tpp 0.01 lot
            !Note! What's left is 0.01 running
            !Note! It will still running
            """
            res = Protobuf.extract(message)
            executionType = res.executionType
            positionStatus = res.position.positionStatus
            if executionType == ProtoOAExecutionType.Value('ORDER_ACCEPTED') and positionStatus == ProtoOAPositionStatus.Value('POSITION_STATUS_OPEN'):
                getRunningPositions()
            if positionStatus == ProtoOAPositionStatus.Value('POSITION_STATUS_CLOSED'):
                stopRunningPosition(res.position.positionId)
            return
        elif message.payloadType == ProtoHeartbeatEvent().payloadType:
            if g_heartbeat:
                # Get the current time in seconds since the epoch
                current_time = time.time()

                # Convert to a datetime object
                dt = datetime.fromtimestamp(current_time, g_mytimezone)

                # Format the time as "HHMM", GMT+8
                formatted_time = dt.strftime("%H%M")

                print(f"[{formatted_time}] Heartbeat Received.")

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

        elif message.payloadType == ProtoOAGetAccountListByAccessTokenRes().payloadType:
            global gAuthPrintOnly

            res = Protobuf.extract(message)
            accounts = res.ctidTraderAccount
            g_auth_acc.clear()
            for index, acc in enumerate(accounts):
                traderLogin = acc.traderLogin
                ctidTraderAccountId = acc.ctidTraderAccountId
                key = f"A_{traderLogin}"
                nickname = "None"
                if key in utility.gConfigData:
                    nickname = utility.gConfigData[key]
                g_auth_acc.append({"no": index, "traderLogin": traderLogin, "ctidTraderAccountId": ctidTraderAccountId, "nickname": nickname})
                # print(f"Authenticating traderLogin:{traderLogin} ctidTraderAccountId:{ctidTraderAccountId} Nickname:{nickname}")
                if gAuthPrintOnly == False:
                    setAccount(index)
            print("\n")
            for acc in g_auth_acc:
                print(acc)
            gAuthPrintOnly = False

        # My problem with this, how to get `res.symbolId` lol
        # elif message.payloadType == ProtoOAUnsubscribeDepthQuotesRes().payloadType:
        #     global g_subscribe
        #     payloadName = ProtoOAPayloadType.Name(message.payloadType)
        #     print(f"Message received: payloadType = {message.payloadType} ({payloadName})")
        #     print("\n", Protobuf.extract(message))

        #     running_position.g_subscribe[res.symbolId]["symbolId"] = None
        #     running_position.g_subscribe[res.symbolId]["symbol"] = None
        #     running_position.g_subscribe[res.symbolId]["bid"] = None
        #     running_position.g_subscribe[res.symbolId]["ask"] = None
        #     running_position.g_subscribe[res.symbolId]["NumOfUser"] = None

        elif message.payloadType == ProtoOASpotEvent().payloadType:
            global g_subscribe

            res = Protobuf.extract(message)
            # For now, let's try ignore getting real symbol name
            symbol = "demo"
            # symbol = utility.read_symbol_id(res.symbolId, ACCOUNT_TYPE)["symbolName"]

            # If data is 0, dont insert, later disrupt my script miscalculate or mistaken that can breakeven now
            if res.bid == 0 or res.ask == 0:
                return

            # If exists, just update the bid/ask price
            if running_position.g_subscribe[res.symbolId]["symbolId"] is not None:
                running_position.g_subscribe[res.symbolId]["bid"] = res.bid
                running_position.g_subscribe[res.symbolId]["ask"] = res.ask
            else:
                running_position.g_subscribe[res.symbolId]["symbolId"] = res.symbolId
                running_position.g_subscribe[res.symbolId]["symbol"] = symbol
                running_position.g_subscribe[res.symbolId]["bid"] = res.bid
                running_position.g_subscribe[res.symbolId]["ask"] = res.ask
                running_position.g_subscribe[res.symbolId]["NumOfUser"] = int(1)

        # Get list of pending orders and running positions of account
        elif message.payloadType == ProtoOAReconcileRes().payloadType:
            global g_positions
            res = Protobuf.extract(message)
            positionList = []
            if len(res.position) != 0:
                positionList = res.position
            else:
                print("No running order")
                return

            for position in positionList:
                # Check if exists in list
                if any(p["positionId"] == position.positionId for p in running_position.g_positions) and len(running_position.g_positions) != 0:
                    continue
                if position.stopLoss == 0:
                    print(f"PositionId:{position.positionId}, stopLoss is 0. Abort.")
                    continue
                symbol = utility.read_symbol_id(position.tradeData.symbolId, ACCOUNT_TYPE)["symbolName"]
                # Safety check, make sure config has the asset
                # I will only check one for the sake of simplicity,
                # Please, be full of integrity, if you add one symbol config,
                # please remember to add all
                if f"SPREAD_{symbol}" not in utility.gConfigData:
                    print(f"Symbol {symbol} has incomplete config.ini. Abort.")
                    continue
                
                volume_to_pip_converter = 0.01 / float(utility.gConfigData[f"VOLUME_PER_LOT_{symbol}"])
                lotsize = round(position.tradeData.volume * volume_to_pip_converter, 2)
                # You can't TPP with lotsize 0.01
                if lotsize == 0.01:
                    continue
                print(f"PositionId:{position.positionId} Symbol:{symbol} Position created.")
                obj = running_position.RunningPosition(position.positionId, position.tradeData.symbolId, symbol, position.tradeData.volume, position.tradeData.tradeSide, position.price, position.stopLoss, position.takeProfit)
                obj.getBidAndAsk()
                thread = threading.Thread(target=obj.run, name = str(position.positionId))
                thread.start()
                # Keeping the object is no application at the moment
                # I just keep it in case future need use
                running_position.g_positions.append({"positionId": position.positionId, "Object": obj})

        else:
            payloadName = ProtoOAPayloadType.Name(message.payloadType)
            print(f"Message received: payloadType = {message.payloadType} ({payloadName})")
            print("\n", Protobuf.extract(message))

    def setAccount(index):
        """
        index is g_auth_acc index
        call `acc` and you know what 7 im saying
        """
        global CURRENT_CTIDTRADERACCOUNTID

        if len(g_auth_acc) == 0:
            print("Call `acc` first, to get account list")
            return

        # if CURRENT_CTIDTRADERACCOUNTID is not None:
        #     sendProtoOAAccountLogoutReq()
        CURRENT_CTIDTRADERACCOUNTID = g_auth_acc[int(index)]["ctidTraderAccountId"]
        sendProtoOAAccountAuthReq()

    def sendProtoOAVersionReq(clientMsgId = None):
        request = ProtoOAVersionReq()
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAGetAccountListByAccessTokenReq(clientMsgId = None):
        request = ProtoOAGetAccountListByAccessTokenReq()
        request.accessToken = ACCESS_TOKEN
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def getAllAccounts(clientMsgId = None):
        """
        The account it displays, depends on the permission you set here
        Click on `sandbox` and you know what 7 im talking ady
        https://openapi.ctrader.com/apps
        """
        global gAuthPrintOnly
        gAuthPrintOnly = True
        request = ProtoOAGetAccountListByAccessTokenReq()
        request.accessToken = ACCESS_TOKEN
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAAccountLogoutReq(clientMsgId = None):
        request = ProtoOAAccountLogoutReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAAccountAuthReq(clientMsgId = None):
        request = ProtoOAAccountAuthReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.accessToken = ACCESS_TOKEN
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAUnsubscribeSpotsReq(symbolId, clientMsgId = None):
        request = ProtoOAUnsubscribeSpotsReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.symbolId.append(int(symbolId))
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def disconnect(clientMsgId=None): # Disconnect the client
        client._disconnected("User exited the connection")

    def getRunningPositions(clientMsgId=None):
        """
        This is for pending orders
        """
        request = ProtoOAReconcileReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)
        
    def stopRunningPosition(positionId, clientMsgId=None):
        """
        Remove position from g_position since it hit SL
        """
        global g_positions
        # Find index of entry with id 4
        index_to_remove = next((i for i, p in enumerate(running_position.g_positions) if p["positionId"] == positionId), None)

        # Remove entry if found
        if index_to_remove is not None:
            # Destroy the object
            running_position.g_positions[index_to_remove]["Object"].alive = False
            # Remove from the list
            running_position.g_positions.pop(index_to_remove)
            print(f"PositionId:{positionId} has been removed from g_positions.")
        

    def getSymbolList(clientMsgId=None):
        request = ProtoOASymbolsListReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def getSymbolDetail(symbolId, clientMsgId=None):
        """
        Example output

        ctidTraderAccountId: xxxx
        symbol {
        symbolId: 41
        digits: 2
        pipPosition: 1
        enableShortSelling: true
        guaranteedStopLoss: false
        swapRollover3Days: XXDAY
        swapLong: -xxx
        swapShort: xxx
        maxVolume: 1000000
        minVolume: 100
        stepVolume: 100
        maxExposure: 10000000000000
        schedule {
            startSecond: XX
            endSecond: XX
        }
        schedule {
            startSecond: XX
            endSecond: XX
        }
        schedule {
            startSecond: XX
            endSecond: XX
        }
        schedule {
            startSecond: XX
            endSecond: XX
        }
        schedule {
            startSecond: XX
            endSecond: XX
        }
        commission: 0
        commissionType: USD_PER_LOT
        slDistance: XX
        tpDistance: XX
        gslDistance: 0
        gslCharge: 0
        distanceSetIn: SYMBOL_DISTANCE_IN_POINTS
        minCommission: 0
        minCommissionType: CURRENCY
        minCommissionAsset: "USD (Demo)"
        rolloverCommission: 0
        skipRolloverDays: 0
        scheduleTimeZone: "GMT"
        tradingMode: ENABLED
        rolloverCommission3Days: XXDAY
        swapCalculationType: PIPS
        lotSize: 10000
        preciseTradingCommissionRate: 0
        preciseMinCommission: 0
        pnlConversionFeeRate: 0
        leverageId: XXXX
        swapPeriod: XX
        swapTime: XXXX
        skipSWAPPeriods: 0
        chargeSwapAtWeekends: false
        }
        """
        request = ProtoOASymbolByIdReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.symbolId.append(symbolId)
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def sendCloseReq(positionId, volume, clientMsgId=None):
        """
        Take partial profit
        """
        request = ProtoOAClosePositionReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.positionId = int(positionId)
        request.volume = int(volume)
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def printRunningList():
        """
        """
        print("\n")
        print("Subscription list now :")
        for s in running_position.g_subscribe:
            if s["symbolId"] is None:
                continue
            print(f"{s}")
            
    def printSubscriptionList():
        """
        """
        print("\n")
        print("Running list now :")
        for p in running_position.g_positions:
            print(f"{p}")

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

    def setHeartbeat(value, clientMsgId=None):
        global g_heartbeat
        g_heartbeat = int(value)

    def showHelp():
        print()
        print("help: showHelp,")
        print("set: setAccount, # Set global variable account ID")
        print("ver: sendProtoOAVersionReq, # Show version")
        print("auth: sendProtoOAGetAccountListByAccessTokenReq, # Authenticate all accounts")
        print("acc: getAllAccounts, # Get all account details")
        print("renew: renewAccessToken, # Renew access & refresh token")
        print("hb: setHeartbeat, # Set print heartbeat true or false. Call it like this `hb 1`")
        print("qq: disconnect,")
        print("m: monitorAndTPP, # m = monitor, to monitor your running position, and TPP if necessary")
        print("s: getSymbolList, # Update symbol files")
        print("r: refresh_RAM, # Refresh global variable with latest value")
        print("test: test,")

    def test(clientMsgId=None):
        request = ProtoOAGetPositionUnrealizedPnLReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    commands = {
        "help": showHelp,
        "set": setAccount, # Set global variable account ID
        "ver": sendProtoOAVersionReq, # Show version
        "auth": sendProtoOAGetAccountListByAccessTokenReq, # Authenticate all accounts
        "acc": getAllAccounts, # Get all account details
        "renew": renewAccessToken, # Renew access & refresh token
        "hb": setHeartbeat, # Set print heartbeat true or false. Call it like this `hb 1`
        "qq": disconnect,
        "sub": sendProtoOASubscribeSpotsReq, # subscribe to asset, call it like this `sub 41`
        "unsub": sendProtoOAUnsubscribeSpotsReq, # UNsubscribe to asset, call it like this `unsub 41`
        "tpp": sendCloseReq, # Take partial profit, call like this `tpp positionid volume` (In volume, check VOLUME_PER_PIP_SYMBOL in config.ini)
        "m": getRunningPositions, # m = monitor, to monitor your running position, and TPP if necessary
        "pp": printRunningList, # p = print running list
        "p": printSubscriptionList, # p = print subscription list
        "s": getSymbolList, # Update symbol files
        "r": refresh_RAM, # Refresh global variable with latest value
        "test": test,
    }

    def executeUserCommand():
        while True:
            print("\n=====================================\n")
            userInput = input("Command (ex help): ")
            running_position.g_command_queue.put(userInput)
            
    def processCommand():
        while True:
            userInput = running_position.g_command_queue.get() # Get command from queue
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
    thread_user_input = threading.Thread(target=executeUserCommand)
    thread_user_input.start()
    thread_process_command = threading.Thread(target=processCommand)
    thread_process_command.start()
    # Check for running positions
    # thread2 = threading.Thread(target=monitorAndTPP)
    # thread2.start()

    # Setting optional client callbacks
    client.setConnectedCallback(connected)
    client.setDisconnectedCallback(disconnected)
    client.setMessageReceivedCallback(onMessageReceived)
    # Starting the client service
    client.startService()
    reactor.run()

