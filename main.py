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
    # ProtoHeartbeatEvent().payloadType,
    ProtoOAExecutionEvent().payloadType
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
        elif message.payloadType == ProtoHeartbeatEvent().payloadType:
            if g_heartbeat:
                # Get the current time in seconds since the epoch
                current_time = time.time()

                # Convert to a datetime object
                dt = datetime.fromtimestamp(current_time, g_mytimezone)

                # Format the time as "HHMM", GMT+8
                formatted_time = dt.strftime("%H%M")

                print(f"[{formatted_time}] Heartbeat Received.")


            getRunningPositions()


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
                symbol = utility.read_symbol_id(position.tradeData.symbolId, ACCOUNT_TYPE)["symbolName"]
                obj = running_position.RunningPosition(position.positionId, position.tradeData.symbolId, symbol, position.tradeData.volume, position.tradeData.tradeSide, position.price, position.stopLoss, position.takeProfit)
                obj.getBidAndAsk()
                thread = threading.Thread(target=obj.run)
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

    def monitorAndTPP(_ProtoOAPosition, clientMsgId=None):
        """
        Monitor running position and TPP if necessary
        """
        while True:
            if len(running_position.g_positions) == 0:
                pass


        _StopLossTakeProfit = -1
        if _ProtoOAPosition.relativeStopLoss != 0 or _ProtoOAPosition.relativeTakeProfit != 0:
            _StopLossTakeProfit = StopLossTakeProfit.RELATIVE.value
        elif _ProtoOAPosition.stopLoss != 0 or _ProtoOAPosition.takeProfit != 0:
            _StopLossTakeProfit = StopLossTakeProfit.ABSOLUTE.value
        else:
            print(f"Warning: Abnormal absolute & realtive TP SL detected. Skip")
            print(f"OrderId:{_ProtoOAPosition.orderId} Symbol:{symbol}")
            print(f"relativeStopLoss:{_ProtoOAPosition.relativeStopLoss}")
            print(f"relativeTakeProfit:{_ProtoOAPosition.relativeTakeProfit}")
            print(f"stopLoss:{_ProtoOAPosition.stopLoss}")
            print(f"takeProfit:{_ProtoOAPosition.takeProfit}")
            return

        _timezone = None
        if symbol == "DAXEUR":
            _timezone = pytz.timezone("Europe/Berlin")
        elif symbol == "NDXUSD" or symbol == "DJIUSD" or symbol =="XAUUSD":
            _timezone = pytz.timezone("America/New_York")

        is_dst = False
        if _timezone is not None:
            now = datetime.now(_timezone)
            # Check if DST is active
            is_dst = bool(now.dst())


        now = datetime.now(g_mytimezone)
        is_friday = now.weekday() == 4
        # Get today's midnight
        midnight = datetime.now(g_mytimezone).replace(hour=0, minute=0, second=0, microsecond=0)
        # Convert to Unix timestamp in millisecond
        unix_time = int(midnight.timestamp()) * 1000

        expiry = f""
        if is_dst:
            expiry = f"EXPIRY_DST_{symbol}"
        else:
            expiry = f"EXPIRY_STANDARD_{symbol}"

        if is_friday:
            expiry = expiry + "_FRIDAY"

        expiry_dt = unix_time + int(utility.gConfigData[expiry])
        dt = datetime.fromtimestamp(expiry_dt / 1000, tz=timezone.utc).astimezone(g_mytimezone)
        expiry_dt_str = dt.strftime("%d %b %Y %H%M")  # Format as "24 May 2025 2359"

        spread = f"SPREAD_{symbol}"
        price_per_pip = f"PRICE_PER_PIP_{symbol}"
        relative_per_pip = f"RELATIVE_PER_PIP_{symbol}"
        volume_per_lot = f"VOLUME_PER_LOT_{symbol}"


        is_limit_order = False
        if _ProtoOAPosition.orderType == ProtoOAOrderType.Value('LIMIT'):
            is_limit_order = True

        if not is_limit_order:
            print(f"Warning: OrderId:{_ProtoOAPosition.orderId} Symbol:{symbol} orderType is {ProtoOAOrderType.Name(_ProtoOAPosition.orderType)} order. I will skip this one")
            return
        if spread not in utility.gConfigData:
            print(f"Warning: Spread:{spread} is not defined for this Symbol:{symbol}. Skip.")
            return
        if price_per_pip not in utility.gConfigData:
            print(f"Warning: price_per_pip:{price_per_pip} is not defined for this Symbol:{symbol}. Skip.")
            return
        if relative_per_pip not in utility.gConfigData:
            print(f"Warning: relative_per_pip:{relative_per_pip} is not defined for this Symbol:{symbol}. Skip.")
            return
        if volume_per_lot not in utility.gConfigData:
            print(f"Warning: volume_per_lot:{volume_per_lot} is not defined for this Symbol:{symbol}. Skip.")
            return


        print(f"OrderId: {_ProtoOAPosition.orderId}")
        print(f"Symbol: {symbol}")
        print(f"tradeSide: {ProtoOATradeSide.Name(_ProtoOAPosition.tradeData.tradeSide)}")
        print(f"StopLossTakeProfit: {StopLossTakeProfit.getName(_StopLossTakeProfit)}")
        print(f"timezone: {_timezone}")
        print(f"is_dst: {'True' if is_dst else 'False'}")
        print(f"spread: {spread}:{round(float(utility.gConfigData[spread]),1)}")
        print(f"price_per_pip: {price_per_pip}:{round(float(utility.gConfigData[price_per_pip]),2)}")
        print(f"relative_per_pip: {relative_per_pip}:{int(utility.gConfigData[relative_per_pip])}")
        print(f"volume_per_lot: {volume_per_lot}:{int(utility.gConfigData[volume_per_lot])}")
        print(f"expiry: {expiry}, Until:{expiry_dt_str}")
        print(f"is_friday: {'True' if is_friday else 'False'}")


        request = ProtoOAAmendOrderReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.orderId = int(_ProtoOAPosition.orderId)
        # regarding _ProtoOAPosition.relativeStopLoss
        # It has if you NEVER place by entering price, but rather by dragging
        # It has value 0 if you entered using price,
        # And it will use _ProtoOAPosition.stopLoss, which is absolute stopLoss price
        # And if either one is 0, request will fail
        # Ok, sometimes it changed to use either & i dk how i triggered that
        # Best is, ur coding, should cover both
        request.limitPrice = round(float(_ProtoOAPosition.limitPrice) + (float(utility.gConfigData[spread]) * float(utility.gConfigData[price_per_pip]) * direction_bias_entry), 2)
        if _StopLossTakeProfit == StopLossTakeProfit.RELATIVE.value:
            request.relativeStopLoss   = int(_ProtoOAPosition.relativeStopLoss)   + int((int(utility.gConfigData[relative_per_pip]) * float(utility.gConfigData[spread]) * direction_bias_SL))
            request.relativeTakeProfit = int(_ProtoOAPosition.relativeTakeProfit) + int((int(utility.gConfigData[relative_per_pip]) * float(utility.gConfigData[spread]) * direction_bias_TP))
        else:
            request.stopLoss   = _ProtoOAPosition.stopLoss
            request.takeProfit = _ProtoOAPosition.takeProfit
        # request.volume = int(utility.gConfigData[volume_per_lot])
        # request.expirationTimestamp = expiry_dt
        # deferred = client.send(request, clientMsgId=clientMsgId)
        # deferred.addErrback(onError)

    def printRunningList():
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
        "m": getRunningPositions, # m = monitor, to monitor your running position, and TPP if necessary
        "p": printRunningList, # p = print running list
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

