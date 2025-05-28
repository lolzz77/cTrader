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
from datetime import datetime
import pytz
import utility
import fileinput
import threading
import time
import running_position

load_dotenv()
utility.read_config_file()

g_heartbeat = True
g_mytimezone = pytz.timezone("Asia/Singapore")

# For RunningPosition class objects
g_running_position_obj_threads = []

FIRST_TIME_BOOT_UP = True
UPDATING_SYMBOL = False

g_subscribe_count = 0

# Lock for multithread
g_lock = threading.Lock()

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
        current_time = time.time()
        dt = datetime.fromtimestamp(current_time, g_mytimezone)

        # Format the time as "HHMM", GMT+8
        formatted_time = dt.strftime("%H%M")
        print(f"\n[{formatted_time}]Connected. ACCOUNT_TYPE:{ACCOUNT_TYPE}")
        request = ProtoOAApplicationAuthReq()
        request.clientId = appClientId
        request.clientSecret = appClientSecret
        deferred = client.send(request)
        deferred.addErrback(onError)

    def disconnected(client, reason): # Callback for client disconnection
        current_time = time.time()
        dt = datetime.fromtimestamp(current_time, g_mytimezone)

        # Format the time as "HHMM", GMT+8
        formatted_time = dt.strftime("%H%M")

        print(f"\n[{formatted_time}] Disconnected: {reason}")

    def onMessageReceived(client, message): # Callback for receiving all messages
        # Initially i put at `if elif`
        # Just realized, it is within function
        global UPDATING_SYMBOL

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
                """
                New position created & running
                Entered a trade
                """
                getRunningPositions()
            if positionStatus == ProtoOAPositionStatus.Value('POSITION_STATUS_CLOSED'):
                """
                Position closed, either hit TP or SL
                """
                stopRunningPosition(res.position.positionId)
                # Call again to make it run "No running order" & clears g_subscribe
                # In case g_subscribe is not cleared
                # Maybe no need first? I dk
                # getRunningPositions()
            return

        elif message.payloadType == ProtoHeartbeatEvent().payloadType:
            if g_heartbeat:
                current_time = time.time()
                dt = datetime.fromtimestamp(current_time, g_mytimezone)

                # Format the time as "HHMM", GMT+8
                formatted_time = dt.strftime("%H%M")

                print(f"[{formatted_time}] Heartbeat Received.")

            return

        elif message.payloadType == ProtoOAPayloadType.Value('PROTO_OA_SYMBOL_CHANGED_EVENT'):
            """
            SYMBOL CHANGED! Update SYMBOL JSON & Update YOUR g_position, g_subscribe,
            and the RunningPosition class!
            """
            print(f"Symbol change! Update!")
            running_position.g_command_queue.put("s")
            return

        elif message.payloadType == ProtoOAApplicationAuthRes().payloadType:
            print(f"API Application authorized")
            if CURRENT_CTIDTRADERACCOUNTID is not None:
                sendProtoOAAccountAuthReq()
            return

        elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
            protoOAAccountAuthRes = Protobuf.extract(message)
            print(f"Account {protoOAAccountAuthRes.ctidTraderAccountId} has been authorized")
            
            # Call symbol update command
            # Who knows during your offline, they updated the symbol IDs lol
            running_position.g_command_queue.put("s")

        elif message.payloadType == ProtoOASymbolsListRes().payloadType:
            global FIRST_TIME_BOOT_UP
            global UPDATING_SYMBOL

            res = Protobuf.extract(message)
            symbol_data = res.symbol
            filename = "symbolList_" + ACCOUNT_TYPE + ".txt"
            with open(filename, "w") as file:
                file.write(str(symbol_data))
            result, symbols_old_NAME_first_dict = utility.convert_txt_to_json(filename, ACCOUNT_TYPE)

            if result == utility.SymbolJsonUpdate.HAS_UPDATE:
                # You have a lot of shit to update..
                # tbh, i prefer you just clear everything and restart again LOL
                # First of all, stop the subscription, else it keeps accessing the dict
                if running_position.g_subscribe:
                    UPDATING_SYMBOL = True
                    print(f"Restarting everything!")
                    if running_position.g_positions:
                        for p in running_position.g_positions.values():
                            p.get('Object').alive = False
                        # Give script some time to process
                        time.sleep(2)
                    # Unsubscribe them all!
                    for s in running_position.g_subscribe.values():
                        running_position.g_command_queue.put(f"unsub {symbols_old_NAME_first_dict[s.get('symbol')]}")

            # Start monitoring running positions
            # This is so that, what if your scrip restarts, you need to check immediately
            # whether got running positions or not
            if FIRST_TIME_BOOT_UP:
                running_position.g_command_queue.put("m")
                FIRST_TIME_BOOT_UP = False

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

        elif message.payloadType == ProtoOAUnsubscribeDepthQuotesRes().payloadType or message.payloadType == ProtoOAPayloadType.Value('PROTO_OA_UNSUBSCRIBE_SPOTS_RES'):
            """
            Unsubscribe to symbols
            It will use count to tell me whether has all the symbols in
            g_subscribe has been unsubscribed, if it is, clear the dictionary
            dangerous to use
            ```
            len(g_subscribe) > 0:
                g_subscribe.pop()

            What if got race condition? Just be safe
            ```
            """
            global g_subscribe
            global g_subscribe_count
    
            res = Protobuf.extract(message)

            if UPDATING_SYMBOL:

                if g_subscribe_count == 0:
                    g_subscribe_count = len(running_position.g_subscribe)
                else:
                    g_subscribe_count -= 1

                if g_subscribe_count <= 0:
                    running_position.g_subscribe.clear()
                    UPDATING_SYMBOL = False
                    g_subscribe_count = 0
                    running_position.g_command_queue.put("m")

            payloadName = ProtoOAPayloadType.Name(message.payloadType)
            print(f"Unsubscribe symbol, Payload Name: {payloadName}")

        elif message.payloadType == ProtoOASpotEvent().payloadType:
            """
            Subscribe to symbols
            """
            global g_subscribe

            res = Protobuf.extract(message)
            # For now, let's try ignore getting real symbol name
            symbol = "demo"
            # symbol = utility.read_symbol_id(res.symbolId, ACCOUNT_TYPE)["symbolName"]

            # If data is 0, dont insert, later disrupt my script miscalculate or mistaken that can breakeven now
            if res.bid == 0 or res.ask == 0:
                return

            # If exists, just update the bid/ask price, else, write into dictionary
            with g_lock:
                if res.symbolId in running_position.g_subscribe:
                    running_position.g_subscribe[res.symbolId]["bid"] = int(res.bid)
                    running_position.g_subscribe[res.symbolId]["ask"] = int(res.ask)
                else:
                    running_position.g_subscribe[res.symbolId] = {"symbol": str(symbol), "bid": int(res.bid), "ask": int(res.ask), "NumOfUser": int(1)}

        # Get list of pending orders and running positions of account
        elif message.payloadType == ProtoOAReconcileRes().payloadType:
            global g_positions
            global g_subscribe
            res = Protobuf.extract(message)
            positionList = []
            if len(res.position) != 0:
                positionList = res.position
            else:
                # Ensure reset it back to None
                running_position.g_subscribe.clear()
                print("No running order")
                return

            for position in positionList:
                # Check if exists in list
                if position.positionId in running_position.g_positions:
                    continue
                if position.stopLoss == 0:
                    print(f"PositionId:{position.positionId}, stopLoss is 0. Abort.")
                    continue
                symbol = utility.read_symbol_id(position.tradeData.symbolId, ACCOUNT_TYPE)
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
                running_position.g_positions[position.positionId] = ({"Object": obj})
                # Check if SL trigger is opposite or not, if is not, set it to opposite
                if position.stopLossTriggerMethod != ProtoOAOrderTriggerMethod.Value('OPPOSITE'):
                    print(f"PositionId:{position.positionId} Symbol:{symbol} SL trigger is not OPPOSITE. Set to OPPOISTE now.")
                    running_position.g_command_queue.put(f"ap {position.positionId} {position.stopLoss} {position.takeProfit} OPPOSITE")

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

    def getCurrentAccount(clientMsgId = None):
        """
        """
        print(f"ctidTraderAccountId:{CURRENT_CTIDTRADERACCOUNTID}")

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
        Remove position from g_position since it hit SL or TP
        """
        global g_positions
        running_position.g_positions[positionId]["Object"].alive = False
        del running_position.g_positions[positionId]

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

    def sendAmendPosition(positionId, entryPrice, takeProfit, SLTriggerMethod = 'TRADE', clientMsgId=None):
        """
        Set BE
        And set trade side to default
        TODO: Set BE + few pips
        
        This also can be used to set SL trigger method to OPPOSITE, without setting BE
        That is, you set original stopLoss, takeProfit, but set SLTriggerMethod to "OPPOSITE"
        """
        request = ProtoOAAmendPositionSLTPReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.positionId = int(positionId)
        request.stopLoss = round(float(entryPrice), 2)
        request.takeProfit = round(float(takeProfit), 2)
        request.stopLossTriggerMethod = ProtoOAOrderTriggerMethod.Value(SLTriggerMethod)
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def printRunningList():
        """
        """
        print("\n")
        print("Subscription list now :")
        for s in running_position.g_subscribe.values():
            print(f"{s}")

    def printSubscriptionList():
        """
        """
        print("\n")
        print("Running list now :")
        for p in running_position.g_positions.keys():
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
        print("Note: Some command are not shown, those shall not be executed by you")
        print("help: showHelp,")
        print("set: setAccount, # Set global variable account ID")
        print("ver: sendProtoOAVersionReq, # Show version")
        print("auth: sendProtoOAGetAccountListByAccessTokenReq, # Authenticate all accounts")
        print("acc: getAllAccounts, # Get all account details")
        print("cur: getCurrentAccount, # Get current acc")
        print("renew: renewAccessToken, # Renew access & refresh token")
        print("hb: setHeartbeat, # Set print heartbeat true or false. Call it like this `hb 1`")
        print("qq: disconnect,")
        print("m: getRunningPositions, # m = monitor, to monitor your running position, and TPP if necessary")
        print("pp: printRunningList, # p = print running list")
        print("p: printSubscriptionList, # p = print subscription list")
        print("s: getSymbolList, # Update symbol files")
        print("r: refresh_RAM, # Refresh global variable with latest value")
        print("test: test,")

    def test(clientMsgId=None):
        pass
        # request = ProtoOAGetPositionUnrealizedPnLReq()
        # request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        # deferred = client.send(request, clientMsgId=clientMsgId)
        # deferred.addErrback(onError)

    commands = {
        "help": showHelp,
        "set": setAccount, # Set global variable account ID
        "ver": sendProtoOAVersionReq, # Show version
        "auth": sendProtoOAGetAccountListByAccessTokenReq, # Authenticate all accounts
        "acc": getAllAccounts, # Get all account details
        "cur": getCurrentAccount, # Get current acc
        "renew": renewAccessToken, # Renew access & refresh token
        "hb": setHeartbeat, # Set print heartbeat true or false. Call it like this `hb 1`
        "qq": disconnect,
        "sub": sendProtoOASubscribeSpotsReq, # subscribe to asset, call it like this `sub 41`
        "unsub": sendProtoOAUnsubscribeSpotsReq, # UNsubscribe to asset, call it like this `unsub 41`
        "tpp": sendCloseReq, # Take partial profit, call like this `tpp positionid volume` (In volume, check VOLUME_PER_PIP_SYMBOL in config.ini)
        "ap": sendAmendPosition, # Amend Running Position, call `ap positionId stopLoss takeProfit "TRADE"`
        "m": getRunningPositions, # m = monitor, to monitor your running position, and TPP if necessary
        "pp": printRunningList, # p = print running list
        "p": printSubscriptionList, # p = print subscription list
        "s": getSymbolList, # Update symbol files
        "r": refresh_RAM, # Refresh global variable with latest value
        "test": test,
    }

    def executeUserCommand():
        try:
            while True:
                print("\n=====================================\n")
                userInput = input("Command (ex help): ")
                running_position.g_command_queue.put(userInput)
        # !CTRL C!
        # To detech & handle CTRL C, but this will not work
        # Due to `reactor.run` is being treated as main thread
        except KeyboardInterrupt:
            print(f"CTRL C is pressed")
        # Detect CTRL D
        except EOFError:
            print(f"Terminate script forcefully.")
            os._exit(0)

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

    # Setting optional client callbacks
    client.setConnectedCallback(connected)
    client.setDisconnectedCallback(disconnected)
    client.setMessageReceivedCallback(onMessageReceived)

    # Starting the client service
    client.startService()

    # !CTRL C!
    # This will be treated as main thread
    # When you type CTRL C, this thread will capture it
    # TODO Find a way to handle CTRL C
    reactor.run()

