#!/usr/bin/env python

import os
from dotenv import load_dotenv

from ctrader_open_api import Client, Protobuf, TcpProtocol, Auth, EndPoints
from ctrader_open_api.endpoints import EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
from twisted.internet import reactor
from inputimeout import inputimeout, TimeoutOccurred
import webbrowser
import datetime
import calendar

# https://dev.to/jakewitcher/using-env-files-for-environment-variables-in-python-applications-55a1
# load_dotenv() will look for '.env' file
load_dotenv()

# From .env file, get the variable
APP_CLIENT_ID = os.getenv('APP_CLIENT_ID')
APP_CLIENT_SECRET = os.getenv('APP_CLIENT_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')

ACCOUNT_TYPE = "demo"

if __name__ == "__main__":
    currentAccountId = None
    hostType = ACCOUNT_TYPE
    hostType = hostType.lower()


    appClientId = APP_CLIENT_ID
    appClientSecret = APP_CLIENT_SECRET
    accessToken = ACCESS_TOKEN

    client = Client(EndPoints.PROTOBUF_LIVE_HOST if hostType.lower() == "live" else EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)

    def connected(client): # Callback for client connection
        print("\nConnected")
        request = ProtoOAApplicationAuthReq()
        request.clientId = appClientId
        request.clientSecret = appClientSecret
        deferred = client.send(request)
        deferred.addErrback(onError)

    def disconnected(client, reason): # Callback for client disconnection
        print("\nDisconnected: ", reason)

    def onMessageReceived(client, message): # Callback for receiving all messages
        if message.payloadType in [ProtoOASubscribeSpotsRes().payloadType, ProtoOAAccountLogoutRes().payloadType, ProtoHeartbeatEvent().payloadType]:
            return
        elif message.payloadType == ProtoOAApplicationAuthRes().payloadType:
            print("API Application authorized\n")
            print("Please use setAccount command to set the authorized account before sending any other command, try help for more detail\n")
            print("To get account IDs use ProtoOAGetAccountListByAccessTokenReq command")
            if currentAccountId is not None:
                sendProtoOAAccountAuthReq()
                return
        elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
            protoOAAccountAuthRes = Protobuf.extract(message)
            print(f"Account {protoOAAccountAuthRes.ctidTraderAccountId} has been authorized\n")
            print("This acccount will be used for all future requests\n")
            print("You can change the account by using setAccount command")
        else:
            print("Message received: \n", Protobuf.extract(message))
        reactor.callLater(3, callable=executeUserCommand)

    def onError(failure): # Call back for errors
        print("Message Error: ", failure)
        reactor.callLater(3, callable=executeUserCommand)

    def showHelp():
        print("Commands (Parameters with an * are required), ignore the description inside ()")
        print("setAccount(For all subsequent requests this account will be used) *accountId")
        print("ProtoOAVersionReq clientMsgId")
        print("ProtoOAGetAccountListByAccessTokenReq clientMsgId")
        print("ProtoOAAssetListReq clientMsgId")
        print("ProtoOAAssetClassListReq clientMsgId")
        print("ProtoOASymbolCategoryListReq clientMsgId")
        print("ProtoOASymbolsListReq includeArchivedSymbols(True/False) clientMsgId")
        print("ProtoOATraderReq clientMsgId")
        print("ProtoOASubscribeSpotsReq *symbolId *timeInSeconds(Unsubscribes after this time) subscribeToSpotTimestamp(True/False) clientMsgId")
        print("ProtoOAReconcileReq clientMsgId")
        print("ProtoOAGetTrendbarsReq *weeks *period *symbolId clientMsgId")
        print("ProtoOAGetTickDataReq *days *type *symbolId clientMsgId")
        print("NewMarketOrder *symbolId *tradeSide *volume clientMsgId")
        print("NewLimitOrder *symbolId *tradeSide *volume *price clientMsgId")
        print("NewStopOrder *symbolId *tradeSide *volume *price clientMsgId")
        print("ClosePosition *positionId *volume clientMsgId")
        print("CancelOrder *orderId clientMsgId")
        print("DealOffsetList *dealId clientMsgId")
        print("GetPositionUnrealizedPnL clientMsgId")
        print("OrderDetails clientMsgId")
        print("OrderListByPositionId *positionId fromTimestamp toTimestamp clientMsgId")

        reactor.callLater(3, callable=executeUserCommand)

    def setAccount(accountId):
        global currentAccountId
        if currentAccountId is not None:
            sendProtoOAAccountLogoutReq()
        currentAccountId = int(accountId)
        sendProtoOAAccountAuthReq()

    def sendProtoOAVersionReq(clientMsgId = None):
        request = ProtoOAVersionReq()
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAGetAccountListByAccessTokenReq(clientMsgId = None):
        request = ProtoOAGetAccountListByAccessTokenReq()
        request.accessToken = accessToken
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAAccountLogoutReq(clientMsgId = None):
        request = ProtoOAAccountLogoutReq()
        request.ctidTraderAccountId = currentAccountId
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAAccountAuthReq(clientMsgId = None):
        request = ProtoOAAccountAuthReq()
        request.ctidTraderAccountId = currentAccountId
        request.accessToken = accessToken
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAAssetListReq(clientMsgId = None):
        request = ProtoOAAssetListReq()
        request.ctidTraderAccountId = currentAccountId
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAAssetClassListReq(clientMsgId = None):
        request = ProtoOAAssetClassListReq()
        request.ctidTraderAccountId = currentAccountId
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOASymbolCategoryListReq(clientMsgId = None):
        request = ProtoOASymbolCategoryListReq()
        request.ctidTraderAccountId = currentAccountId
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOASymbolsListReq(includeArchivedSymbols = False, clientMsgId = None):
        request = ProtoOASymbolsListReq()
        request.ctidTraderAccountId = currentAccountId
        request.includeArchivedSymbols = includeArchivedSymbols if type(includeArchivedSymbols) is bool else bool(includeArchivedSymbols)
        deferred = client.send(request)
        deferred.addErrback(onError)

    def sendProtoOATraderReq(clientMsgId = None):
        request = ProtoOATraderReq()
        request.ctidTraderAccountId = currentAccountId
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAUnsubscribeSpotsReq(symbolId, clientMsgId = None):
        request = ProtoOAUnsubscribeSpotsReq()
        request.ctidTraderAccountId = currentAccountId
        request.symbolId.append(int(symbolId))
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOASubscribeSpotsReq(symbolId, timeInSeconds, subscribeToSpotTimestamp	= False, clientMsgId = None):
        request = ProtoOASubscribeSpotsReq()
        request.ctidTraderAccountId = currentAccountId
        request.symbolId.append(int(symbolId))
        request.subscribeToSpotTimestamp = subscribeToSpotTimestamp if type(subscribeToSpotTimestamp) is bool else bool(subscribeToSpotTimestamp)
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)
        reactor.callLater(int(timeInSeconds), sendProtoOAUnsubscribeSpotsReq, symbolId)

    def sendProtoOAReconcileReq(clientMsgId = None):
        request = ProtoOAReconcileReq()
        request.ctidTraderAccountId = currentAccountId
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAGetTrendbarsReq(weeks, period, symbolId, clientMsgId = None):
        request = ProtoOAGetTrendbarsReq()
        request.ctidTraderAccountId = currentAccountId
        request.period = ProtoOATrendbarPeriod.Value(period)
        request.fromTimestamp = int(calendar.timegm((datetime.datetime.utcnow() - datetime.timedelta(weeks=int(weeks))).utctimetuple())) * 1000
        request.toTimestamp = int(calendar.timegm(datetime.datetime.utcnow().utctimetuple())) * 1000
        request.symbolId = int(symbolId)
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAGetTickDataReq(days, quoteType, symbolId, clientMsgId = None):
        request = ProtoOAGetTickDataReq()
        request.ctidTraderAccountId = currentAccountId
        request.type = ProtoOAQuoteType.Value(quoteType.upper())
        request.fromTimestamp = int(calendar.timegm((datetime.datetime.utcnow() - datetime.timedelta(days=int(days))).utctimetuple())) * 1000
        request.toTimestamp = int(calendar.timegm(datetime.datetime.utcnow().utctimetuple())) * 1000
        request.symbolId = int(symbolId)
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOANewOrderReq(symbolId, orderType, tradeSide, volume, price = None, clientMsgId = None):
        request = ProtoOANewOrderReq()
        request.ctidTraderAccountId = currentAccountId
        request.symbolId = int(symbolId)
        request.orderType = ProtoOAOrderType.Value(orderType.upper())
        request.tradeSide = ProtoOATradeSide.Value(tradeSide.upper())
        request.volume = int(volume) * 100
        if request.orderType == ProtoOAOrderType.LIMIT:
            request.limitPrice = float(price)
        elif request.orderType == ProtoOAOrderType.STOP:
            request.stopPrice = float(price)
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendNewMarketOrder(symbolId, tradeSide, volume, clientMsgId = None):
        sendProtoOANewOrderReq(symbolId, "MARKET", tradeSide, volume, clientMsgId = clientMsgId)

    def sendNewLimitOrder(symbolId, tradeSide, volume, price, clientMsgId = None):
        sendProtoOANewOrderReq(symbolId, "LIMIT", tradeSide, volume, price, clientMsgId)

    def sendNewStopOrder(symbolId, tradeSide, volume, price, clientMsgId = None):
        sendProtoOANewOrderReq(symbolId, "STOP", tradeSide, volume, price, clientMsgId)

    def sendProtoOAClosePositionReq(positionId, volume, clientMsgId = None):
        request = ProtoOAClosePositionReq()
        request.ctidTraderAccountId = currentAccountId
        request.positionId = int(positionId)
        request.volume = int(volume) * 100
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOACancelOrderReq(orderId, clientMsgId = None):
        request = ProtoOACancelOrderReq()
        request.ctidTraderAccountId = currentAccountId
        request.orderId = int(orderId)
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOADealOffsetListReq(dealId, clientMsgId=None):
        request = ProtoOADealOffsetListReq()
        request.ctidTraderAccountId = currentAccountId
        request.dealId = int(dealId)
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAGetPositionUnrealizedPnLReq(clientMsgId=None):
        request = ProtoOAGetPositionUnrealizedPnLReq()
        request.ctidTraderAccountId = currentAccountId
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAOrderDetailsReq(orderId, clientMsgId=None):
        request = ProtoOAOrderDetailsReq()
        request.ctidTraderAccountId = currentAccountId
        request.orderId = int(orderId)
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAOrderListByPositionIdReq(positionId, fromTimestamp=None, toTimestamp=None, clientMsgId=None):
        request = ProtoOAOrderListByPositionIdReq()
        request.ctidTraderAccountId = currentAccountId
        request.positionId = int(positionId)
        deferred = client.send(request, fromTimestamp=fromTimestamp, toTimestamp=toTimestamp, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    commands = {
        "help": showHelp,
        "setAccount": setAccount,
        "ProtoOAVersionReq": sendProtoOAVersionReq,
        "ProtoOAGetAccountListByAccessTokenReq": sendProtoOAGetAccountListByAccessTokenReq,
        "ProtoOAAssetListReq": sendProtoOAAssetListReq,
        "ProtoOAAssetClassListReq": sendProtoOAAssetClassListReq,
        "ProtoOASymbolCategoryListReq": sendProtoOASymbolCategoryListReq,
        "ProtoOASymbolsListReq": sendProtoOASymbolsListReq,
        "ProtoOATraderReq": sendProtoOATraderReq,
        "ProtoOASubscribeSpotsReq": sendProtoOASubscribeSpotsReq,
        "ProtoOAReconcileReq": sendProtoOAReconcileReq,
        "ProtoOAGetTrendbarsReq": sendProtoOAGetTrendbarsReq,
        "ProtoOAGetTickDataReq": sendProtoOAGetTickDataReq,
        "NewMarketOrder": sendNewMarketOrder,
        "NewLimitOrder": sendNewLimitOrder,
        "NewStopOrder": sendNewStopOrder,
        "ClosePosition": sendProtoOAClosePositionReq,
        "CancelOrder": sendProtoOACancelOrderReq,
        "DealOffsetList": sendProtoOADealOffsetListReq,
        "GetPositionUnrealizedPnL": sendProtoOAGetPositionUnrealizedPnLReq,
        "OrderDetails": sendProtoOAOrderDetailsReq,
        "OrderListByPositionId": sendProtoOAOrderListByPositionIdReq,
    }

    def executeUserCommand():
        try:
            print("\n")
            userInput = inputimeout("Command (ex help): ", timeout=18)
        except TimeoutOccurred:
            print("Command Input Timeout")
            reactor.callLater(3, callable=executeUserCommand)
            return
        userInputSplit = userInput.split(" ")
        if not userInputSplit:
            print("Command split error: ", userInput)
            reactor.callLater(3, callable=executeUserCommand)
            return
        command = userInputSplit[0]
        try:
            parameters = [parameter if parameter[0] != "*" else parameter[1:] for parameter in userInputSplit[1:]]
        except:
            print("Invalid parameters: ", userInput)
            reactor.callLater(3, callable=executeUserCommand)
        if command in commands:
            commands[command](*parameters)
        else:
            print("Invalid Command: ", userInput)
            reactor.callLater(3, callable=executeUserCommand)

    # Setting optional client callbacks
    client.setConnectedCallback(connected)
    client.setDisconnectedCallback(disconnected)
    client.setMessageReceivedCallback(onMessageReceived)
    # Starting the client service
    client.startService()
    reactor.run()