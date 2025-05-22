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



# https://dev.to/jakewitcher/using-env-files-for-environment-variables-in-python-applications-55a1
# load_dotenv() will look for '.env' file
load_dotenv()
utility.read_config_file()


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
    ProtoOAExecutionEvent().payloadType
]
gAuthPrintOnly = False

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
            for acc in accounts:
                traderLogin = acc.traderLogin
                ctidTraderAccountId = acc.ctidTraderAccountId
                key = f"A_{traderLogin}"
                nickname = "None"
                if key in utility.gConfigData:
                    nickname = utility.gConfigData[key]
                print(f"Authenticating traderLogin:{traderLogin} ctidTraderAccountId:{ctidTraderAccountId} Nickname:{nickname}")
                if gAuthPrintOnly == False:
                    setAccount(ctidTraderAccountId)
            gAuthPrintOnly = False

        # Get list of pending orders and running positions of account
        elif message.payloadType == ProtoOAReconcileRes().payloadType:
            res = Protobuf.extract(message)

            # Just leave it here
            # For now, I only care pending orders
            # Those that entered position, please, you should know
            # it and you should set it immediately
            positionList = res.position

            if len(res.order) != 0:
                orderList = res.order
                for order in orderList:
                    symbol = utility.read_symbol_id(order.tradeData.symbolId, ACCOUNT_TYPE)["symbolName"]
                    amendOrder(order, symbol)
            else:
                print("No pending order")



        else:
            payloadName = ProtoOAPayloadType.Name(message.payloadType)
            print(f"Message received: payloadType = {message.payloadType} ({payloadName})")
            print("\n", Protobuf.extract(message))
            
    def onError(failure): # Call back for errors
        print("Message Error: ", failure)

    def showHelp():
        print("im too lazy to write, you should know better")

    def setAccount(ctidTraderAccountId):
        global CURRENT_CTIDTRADERACCOUNTID
        # if CURRENT_CTIDTRADERACCOUNTID is not None:
        #     sendProtoOAAccountLogoutReq()
        CURRENT_CTIDTRADERACCOUNTID = int(ctidTraderAccountId)
        # Dont authenticate, just set global variable
        # sendProtoOAAccountAuthReq()
        
        

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

    def sendProtoOAClosePositionReq(positionId, volume, clientMsgId = None):
        request = ProtoOAClosePositionReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.positionId = int(positionId)
        request.volume = int(volume) * 100
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def disconnect(clientMsgId=None): # Disconnect the client
        client._disconnected("User exited the connection")

    def getOrderList(clientMsgId=None):
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

    def subscribeToSymbolSpot(symbolId, clientMsgId=None):
        """
        Subscribe = Get
        Spot = the current price
        Output of this function

        ctidTraderAccountId: xxxxx
        symbolId: 41
        bid: 318645000
        ask: 318677000
        """
        request = ProtoOASubscribeSpotsReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.symbolId.append(int(symbolId))
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def amendOrder(_ProtoOAOrder, symbol, clientMsgId=None):
        """
        1. Check what is your order Type - ProtoOAOrderType
        if ProtoOAOrderType == LIMIT
            Then please assign request.limitPrice
        if ProtoOAOrderType == STOP
            Then please alert because I probably set wrong

        2. If you didnt set TP SL
        It will reset to no set

        3. expirationTimestamp
        Unix datetime in milisecond
        Example: 1747383452 (in second)
        Translate to
            16 May 2025 08:17:32 GMT+0
            16 May 2025 16:17:32 GMT+8

        Give this variable with value 1747383452000 (in milisecond)
        The expiry shown on cTrader is 16/05/2025 16:17:32

        I guess it will auto detect your cTrader timezone i dk
        Means, no need to convert anything

        Note:
        1. Check if expiry is set, if set, DONT SET AGAIN,
        cos it will DOUBLE set the spread!
        2. Maybe when u put order manually, you can put max lot size,
        then it confirm will fail execute,
        then write code only 5-10 mins before market open set the order,
        change lot size back to 0.01
        3. If Expiry for that symbol is not found in config.ini,
        print to alert me it will skip setting expiry, it will proceed
        to set the SL to opposite direction

        """
        print("\n")
        if _ProtoOAOrder.expirationTimestamp != 0:
            expiry_dt = _ProtoOAOrder.expirationTimestamp
            my_timezone = pytz.timezone("Asia/Singapore")
            dt = datetime.fromtimestamp(expiry_dt / 1000, tz=timezone.utc).astimezone(my_timezone)
            expiry_dt_str = dt.strftime("%d %b %Y %H%M")  # Format as "24 May 2025 2359"
            print(f"OrderId:{_ProtoOAOrder.orderId} Symbol:{symbol} has expiration date set. Expiration: {expiry_dt_str}. Skip.")
            return


        # For amending order with spread
        # for buy limit, add spread to higher price
        # for sell limit, minus spread to lower price
        direction_bias_entry = 0
        # Default, same got both BUY/SELL, dont change
        direction_bias_TP = -1
        direction_bias_SL = 1

        # Here's the rule
        # I only handle LIMIT orders
        # If buy limit, means con9lan7firm order limit price is below current market price
        # If sell limit, means con9lan7firm order limit price is above current market price
        if _ProtoOAOrder.tradeData.tradeSide == ProtoOATradeSide.Value('BUY'):
            direction_bias_entry = 1
        else:
            direction_bias_entry = -1

        _StopLossTakeProfit = -1
        if _ProtoOAOrder.relativeStopLoss != 0 or _ProtoOAOrder.relativeTakeProfit != 0:
            _StopLossTakeProfit = StopLossTakeProfit.RELATIVE.value
        elif _ProtoOAOrder.stopLoss != 0 or _ProtoOAOrder.takeProfit != 0:
            _StopLossTakeProfit = StopLossTakeProfit.ABSOLUTE.value
        else:
            print(f"Warning: Abnormal absolute & realtive TP SL detected. Skip")
            print(f"OrderId:{_ProtoOAOrder.orderId} Symbol:{symbol}")
            print(f"relativeStopLoss:{_ProtoOAOrder.relativeStopLoss}")
            print(f"relativeTakeProfit:{_ProtoOAOrder.relativeTakeProfit}")
            print(f"stopLoss:{_ProtoOAOrder.stopLoss}")
            print(f"takeProfit:{_ProtoOAOrder.takeProfit}")
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


        my_timezone = pytz.timezone("Asia/Singapore")
        now = datetime.now(my_timezone)
        is_friday = now.weekday() == 4
        # Get today's midnight
        midnight = datetime.now(my_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
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
        dt = datetime.fromtimestamp(expiry_dt / 1000, tz=timezone.utc).astimezone(my_timezone)
        expiry_dt_str = dt.strftime("%d %b %Y %H%M")  # Format as "24 May 2025 2359"

        spread = f"SPREAD_{symbol}"
        price_per_pip = f"PRICE_PER_PIP_{symbol}"
        relative_per_pip = f"RELATIVE_PER_PIP_{symbol}"
        volume_per_lot = f"VOLUME_PER_LOT_{symbol}"


        is_limit_order = False
        if _ProtoOAOrder.orderType == ProtoOAOrderType.Value('LIMIT'):
            is_limit_order = True

        if not is_limit_order:
            print(f"Warning: OrderId:{_ProtoOAOrder.orderId} Symbol:{symbol} orderType is {ProtoOAOrderType.Name(_ProtoOAOrder.orderType)} order. I will skip this one")
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


        print(f"OrderId: {_ProtoOAOrder.orderId}")
        print(f"Symbol: {symbol}")
        print(f"tradeSide: {ProtoOATradeSide.Name(_ProtoOAOrder.tradeData.tradeSide)}")
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
        request.orderId = int(_ProtoOAOrder.orderId)
        # regarding _ProtoOAOrder.relativeStopLoss
        # It has if you NEVER place by entering price, but rather by dragging
        # It has value 0 if you entered using price,
        # And it will use _ProtoOAOrder.stopLoss, which is absolute stopLoss price
        # And if either one is 0, request will fail
        # Ok, sometimes it changed to use either & i dk how i triggered that
        # Best is, ur coding, should cover both
        request.limitPrice = round(float(_ProtoOAOrder.limitPrice) + (float(utility.gConfigData[spread]) * float(utility.gConfigData[price_per_pip]) * direction_bias_entry), 2)
        if _StopLossTakeProfit == StopLossTakeProfit.RELATIVE.value:
            request.relativeStopLoss   = int(_ProtoOAOrder.relativeStopLoss)   + int((int(utility.gConfigData[relative_per_pip]) * float(utility.gConfigData[spread]) * direction_bias_SL))
            request.relativeTakeProfit = int(_ProtoOAOrder.relativeTakeProfit) + int((int(utility.gConfigData[relative_per_pip]) * float(utility.gConfigData[spread]) * direction_bias_TP))
        else:
            request.stopLoss   = _ProtoOAOrder.stopLoss
            request.takeProfit = _ProtoOAOrder.takeProfit
        request.volume = int(utility.gConfigData[volume_per_lot])
        request.expirationTimestamp = expiry_dt
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
        
    def test(orderId, clientMsgId=None):
        print("hello")

    commands = {
        "help": showHelp,
        "set": setAccount, # Set global variable account ID
        "ver": sendProtoOAVersionReq, # Show version
        "auth": sendProtoOAGetAccountListByAccessTokenReq, # Authenticate all accounts
        "acc": getAllAccounts, # Get all account details
        "ClosePosition": sendProtoOAClosePositionReq,
        "renew": renewAccessToken, # Renew access & refresh token
        "qq": disconnect,
        "a": getOrderList, # Amend orders
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