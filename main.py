#!/usr/bin/env python

"""
How the system works
1. 4 threads
- MainThread
- Thread-6 (executeUserCommand)
- Thread-7 (processCommand)
- PoolThread-twisted.internet.reactor-0

MainThread will always be the one to handle message received.
Hence, message receiving is single-threaded.

When you type command that will send request, that request sending
will be handled by that thread, sending command has no authentication
issue, cos the sender only need to tell server which trader ID.
Server will be the one authenticate the request.
Hence, which thread sending request is not a problem.
"""

import os
from dotenv import load_dotenv
from ctrader_open_api import Client, Protobuf, TcpProtocol, Auth, EndPoints
from ctrader_open_api.endpoints import EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
from twisted.internet import reactor
import datetime
from datetime import datetime, time as time2
import pytz
import utility
import fileinput
import threading
import time
import running_position
from enum import Enum

load_dotenv()
utility.read_config_file() # Read config.ini

class StopLossTakeProfit(Enum):
    """
    # In an order, it has relative stop loss or absolute stop loss
    # You have to choose one side
    """
    RELATIVE = 1
    ABSOLUTE = 2

    @classmethod
    def getName(cls, value):
        for key in cls:
            if key.value == value:
                return key.name
        return None

FIRST_TIME_BOOT_UP = True       # To run my main command, to monitor my trade
UPDATING_SYMBOL = False         # To handle updating symbol ID if receives from server saying symbol IDs updated
g_subscribe_count = 0           # Use tgt with UPDATING_SYMBOL, to clear g_subscribe dictionary
SET_LOTSIZE = False             # For command that sets lotsize only
MARKET_CLOSE_SET_LIAO = False   # For detecting open/close market, set lotsize accordingly
MARKET_OPEN_SET_LIAO = False    # For detecting open/close market, set lotsize accordingly
CLOSE_ALL = False               # To close all running position once approaching market close on friday
ProtoOASymbolByIdRes_PRINT_ONLY = False # When you get symbol detail, but you want to print only
APP_CLIENT_ID       = os.getenv('APP_CLIENT_ID')
APP_CLIENT_SECRET   = os.getenv('APP_CLIENT_SECRET')
ACCESS_TOKEN        = os.getenv('ACCESS_TOKEN')
ACCOUNT_TYPE        = os.getenv('ACCOUNT_TYPE')
CURRENT_CTIDTRADERACCOUNTID = int(os.getenv('CURRENT_ACCOUNT_ID'))

utility.read_symbol_file(ACCOUNT_TYPE) # Read symbolList_demo/live.json

g_print_heartbeat = False # Enable print heartbeat message
g_mytimezone = pytz.timezone("Asia/Singapore")
g_pending = {} # List of pending orders

# This helps me keep track what is the last time_checks i checked
# If already done, then can skip the market open/close checking shit
g_time_checks_record = { "None" : -1 }

g_favourite_symbol = ["XAUUSD", "DAXEUR", "NDXUSD", "DJIUSD", "NIKJPY"]

# For command processing
# My rules, the list index contains the following
# [0] - function name
# [1] - parameters to pass to function
# [2] - The payload ENUM, this is for handling function that sends requests
# [3] - For debugging [2] purposes, this holds the comment to tell me the
# one whom trigger this task to keep waiting for server reply is triggered by whom
# to the server. If this is set, [0] shall be None
g_task_queue = []

# For those task that sends request to server
# The server returns data, this holds the data
g_data_dict = {}

# When you run `acc`, it will set this to TRUE
# Then it will set it back to false
# Purpose is to print your accounts only
# When you run `auth`, it wont modify this variable,
# leads to authenticating your acc
gAuthPrintOnly = False

# For user input handling
# If new print has printed onto console
# Then ask user to retype their shit
NEW_PRINT_HAS_HAPPENED = False

# For my conveniences of `set 1`, `set 2`, set accounts by just typing 1 num
g_auth_acc = []

# List of server message to ignore
gPayloadIgnoreList = [
    ProtoOASubscribeSpotsRes().payloadType,
    ProtoOAAccountLogoutRes().payloadType,
    # ProtoHeartbeatEvent().payloadType,
    # ProtoOAExecutionEvent().payloadType
]

hostType = ACCOUNT_TYPE
hostType = hostType.lower()
appClientId = APP_CLIENT_ID
appClientSecret = APP_CLIENT_SECRET

client = Client(EndPoints.PROTOBUF_LIVE_HOST if hostType.lower() == "live" else EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)

if __name__ == "__main__":

    def connected(client):
        """
        # Callback for client connection
        """
        global g_task_queue
        current_time = time.time()
        dt = datetime.fromtimestamp(current_time, g_mytimezone)
        formatted_time = dt.strftime("%H%M")
        print(f"\n[{formatted_time}] Connected. ACCOUNT_TYPE:{ACCOUNT_TYPE}")
        
        # Startup tasks! Yay!
        # Authenticate API
        g_task_queue.append([send_Authenticate_API, None, None, None])
        g_task_queue.append([None, None, ProtoOAApplicationAuthRes().payloadType, "Call by send_Authenticate_API"])
        
        if CURRENT_CTIDTRADERACCOUNTID is not None:
            # Authenticate account
            g_task_queue.append([send_Auth_Account, None, None, None])
            g_task_queue.append([None, None, ProtoOAAccountAuthRes().payloadType, "Call by send_Auth_Account"])
        
        # Check is there any symbol update
        g_task_queue.append([send_Get_Symbol_List, None, None, None])
        g_task_queue.append([None, None, ProtoOASymbolsListRes().payloadType, "Call by send_Get_Symbol_List"])
        g_task_queue.append([Update_Symbol_List_Json, None, None, None])
        
        # g_task_queue.append([Update_Symbol_List_Json, None, None, None])

    def disconnected(client, reason):
        """
        # Callback for client disconnection
        """
        current_time = time.time()
        dt = datetime.fromtimestamp(current_time, g_mytimezone)
        formatted_time = dt.strftime("%H%M")
        print(f"\n[{formatted_time}] Disconnected: {reason}")

    def onError(failure):
        """
        # Call back for errors
        """
        print("Message Error: ", failure)

    def onMessageReceived(client, message):
        """
        # Callback for receiving all messages
        """
        global UPDATING_SYMBOL
        global SET_LOTSIZE
        global MARKET_CLOSE_SET_LIAO
        global MARKET_OPEN_SET_LIAO
        global CLOSE_ALL
        global g_subscribe_count
        global FIRST_TIME_BOOT_UP
        global gAuthPrintOnly
        global g_positions
        global g_subscribe
        global g_pending
        global g_time_checks_record
        global gSymbolData
        global gSymbolDataSwap
        global ProtoOASymbolByIdRes_PRINT_ONLY
        global gConfigData
        global g_task_queue
        global NEW_PRINT_HAS_HAPPENED

        if message.payloadType in gPayloadIgnoreList:
            pass

        elif message.payloadType == ProtoOAExecutionEvent().payloadType:
            """
            To detect whether buy/sell limit is hit & entered trade
            And to detect whether the position is still running

            I will use this to help me
            1. Set SL trigger method to OPPOSITE if it is not OPPOSITE
            2. Close running position on Saturday morning
            """
            NEW_PRINT_HAS_HAPPENED = True
            res = Protobuf.extract(message)

            # print("\n==================================")
            # print(res)
            # print("==================================\n")
            # This is to tell me whether did my script pick up a running order
            # I encountered an issue where my ordder got hit during this script
            # disconnection, and once the script up, it didnt pick up the running
            # order
            print(f"ProtoOAExecutionEvent")

            # executionType = res.executionType
            positionStatus = res.position.positionStatus
            isServerEvent = res.isServerEvent

            if isServerEvent == True and positionStatus == ProtoOAPositionStatus.Value('POSITION_STATUS_OPEN'):
                """
                New position created & running
                Entered a trade

                Known Issue
                if you TPP, the leftover position is treated as opened a new position
                and getRunningPosition runs again
                And it is known that, the new position, will have the same position ID as previous
                """
                getRunningPositions()

        elif message.payloadType == ProtoHeartbeatEvent().payloadType:
            if g_print_heartbeat:
                NEW_PRINT_HAS_HAPPENED = True
                current_time = time.time()
                dt = datetime.fromtimestamp(current_time, g_mytimezone)
                formatted_time = dt.strftime("%H%M")
                print(f"[{formatted_time}] Heartbeat Received.")

            # 1. Modify Pending Order lotsizes according to time
            # 2. Close all running order according to time

            now = datetime.now(g_mytimezone)
            current_time = now.time()
            current_time_for_myself = time.time()
            dt = datetime.fromtimestamp(current_time_for_myself, g_mytimezone)
            formatted_time = dt.strftime("%H%M")
            current_weekday = now.strftime("%A")

            lotsize = 0

            # Market open, set lotsize to my lotsize
            if current_weekday == "Monday":
                time_checks = time2(8, 30)
                if current_time > time_checks:
                    if current_weekday not in g_time_checks_record:
                        NEW_PRINT_HAS_HAPPENED = True
                        print(f"Today is {current_weekday} {formatted_time}. Market opening. Set all pending order lotsize to {utility.gConfigData['LOTSIZE']}.")
                        lotsize = utility.gConfigData["LOTSIZE"]
                        g_time_checks_record = {current_weekday : lotsize}

            # Market closing, weekend, close all running positions too
            elif current_weekday == "Saturday":
                time_checks = time2(2, 0)
                if current_time > time_checks:
                    if current_weekday not in g_time_checks_record:
                        NEW_PRINT_HAS_HAPPENED = True
                        print(f"Today is {current_weekday} {formatted_time}. Market closing. Set all pending order lotsize to max. Also close all running order.")
                        lotsize = 100
                        g_time_checks_record = {current_weekday : lotsize}

            else:
                if current_weekday not in g_time_checks_record:
                    NEW_PRINT_HAS_HAPPENED = True
                    print(f"Today is {current_weekday} {formatted_time}.")
                    g_time_checks_record = {current_weekday : lotsize}

            # After a lot of checking above, here handles the aftermath
            if lotsize != 0:
                if len(g_pending) != 0:
                    NEW_PRINT_HAS_HAPPENED = True
                    MAX_MIN_LOT = "MIN_LOT_"
                    if lotsize == 100:
                        """
                        I scare for some symbol, lotsize 100 is not their maximum so.
                        I rather use MAX_LOT_SYMBOL
                        """
                        MAX_MIN_LOT = "MAX_LOT_"
                    for value in g_pending.values():
                        # Get MIN_LOT_XAUUSD from config.ini
                        volume_to_pip_converter = 0.01 / float(utility.gConfigData[f"{MAX_MIN_LOT}{value['symbol']}"])
                        # Same lotsize, no need adjust
                        if value["Object"].tradeData.volume * volume_to_pip_converter == lotsize:
                            continue
                        # Divide, eg: (x = x / 4) is same as (x /= 4)
                        # Gotta update this g_pending, else next time they detect still same volume & send command again
                        value["Object"].tradeData.volume = int(lotsize / volume_to_pip_converter)
                        amendOrder_setLotSize(value["Object"], value["symbol"], lotsize)

                # Close all running position
                # I want to run getRunningPositions after amendOrder_setLotSize
                # Because amendOrder will modify g_pending
                # getRunningPositions will clear & reinsert g_pending
                # If these 2 terbalik, then g_pending, which freshly refreshed, will get overwritten by amendOrder
                # Tho, I dk whether the message receive, will be in order or not.
                # Because u know, both getRunningPositions and amendOrder_setLotSize will send command to server
                if current_weekday == "Saturday":
                    NEW_PRINT_HAS_HAPPENED = True
                    CLOSE_ALL = True
                    getRunningPositions()

        elif message.payloadType == ProtoOAPayloadType.Value('PROTO_OA_SYMBOL_CHANGED_EVENT'):
            """
            SYMBOL CHANGED! Update SYMBOL JSON & Update YOUR g_position, g_subscribe,
            and the RunningPosition class!
            """
            NEW_PRINT_HAS_HAPPENED = True
            print(f"Symbol change! Update!")
            running_position.g_command_queue.put({"userInput":"us"})

        elif message.payloadType == ProtoOAApplicationAuthRes().payloadType:
            print(f"API Application authorized")

        elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
            protoOAAccountAuthRes = Protobuf.extract(message)
            # If no such environment, it will be "None"
            nickname = os.getenv(f'A_{protoOAAccountAuthRes.ctidTraderAccountId}')
            print(f"Account [{protoOAAccountAuthRes.ctidTraderAccountId}: {nickname}] has been authorized")

            # Call symbol update command
            # Who knows during your offline, they updated the symbol IDs lol
            # running_position.g_command_queue.put({"userInput":"us"})

        elif message.payloadType == ProtoOASymbolsListRes().payloadType:
            res = Protobuf.extract(message)
            g_data_dict[ProtoOASymbolsListRes().payloadType] = res

        elif message.payloadType == ProtoOASymbolByIdRes().payloadType:
            """
            Symbol entity details
            Mainly is to get the MinVolume and MaxVolume, for lotsize

            There is 1 problem with this
            If you have 4 symbols, you need to call it 4 times
            I believe it's not healthy for bootup script
            I think is better keep this as "manual trgger"

            Update: It will return a list
            res.symbol[0....].symbolId
            Hence, can just call it 1 time
            """
            res = Protobuf.extract(message)

            if ProtoOASymbolByIdRes_PRINT_ONLY:
                ProtoOASymbolByIdRes_PRINT_ONLY = False
                print(res)

            else:
                # Update the symbol to config.ini
                #!NOTE! Gonna make sure symbol ID gets updated
                symbolList = res.symbol
                for s in symbolList:
                    symbol = utility.gSymbolData[s.symbolId]
                    section = "SYMBOL_SECTION"
                    key_min = f"MIN_LOT_{symbol}"
                    key_max = f"MAX_LOT_{symbol}"
                    min_lot = s.minVolume
                    max_lot = s.maxVolume

                    utility.write_config_file(section, key_min, min_lot)
                    utility.write_config_file(section, key_max, max_lot)

                utility.gConfigData = None
                utility.read_config_file()

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
            res = Protobuf.extract(message)
            accounts = res.ctidTraderAccount
            g_auth_acc.clear()
            for index, acc in enumerate(accounts):
                traderLogin = acc.traderLogin
                ctidTraderAccountId = acc.ctidTraderAccountId
                nickname = os.getenv(f'A_{ctidTraderAccountId}')
                g_auth_acc.append({"no": index, "traderLogin": traderLogin, "ctidTraderAccountId": ctidTraderAccountId, "nickname": nickname})
                # print(f"Authenticating traderLogin:{traderLogin} ctidTraderAccountId:{ctidTraderAccountId} Nickname:{nickname}")
                if gAuthPrintOnly == False:
                    setAccount(index)
            print("\n")
            for acc in g_auth_acc:
                print(acc)
            gAuthPrintOnly = False

        elif message.payloadType == ProtoOAUnsubscribeSpotsRes().payloadType:
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
            res = Protobuf.extract(message)

            if UPDATING_SYMBOL:

                # Becuase i will call unsubscribe on each symbol
                # Gonna use count to help me keep tract how many unsubscribed
                if g_subscribe_count == 0:
                    g_subscribe_count = len(running_position.g_subscribe)
                else:
                    g_subscribe_count -= 1

                if g_subscribe_count <= 0:
                    running_position.g_subscribe.clear()
                    g_subscribe_count = 0

                    symbolIdList = []
                    for s in g_favourite_symbol:
                        symbolId = utility.gSymbolDataSwap[s]
                        symbolIdList.append(symbolId)

                    updateSymbolDetailList(symbolIdList)

                    # Update the gConfigData will be handled in the response of the function above

                    # Restart the monitoring again
                    running_position.g_command_queue.put("m")

            # Clean up the g_subscribe
            with running_position.g_lock:
                if len(running_position.g_positions) == 0:
                    running_position.g_subscribe.clear()
                else:
                    temp = running_position.g_subscribe.copy()
                    for t in temp:
                        for p in running_position.g_positions.values():
                            if t == p["Object"].symbolId:
                                continue
                            del running_position.g_subscribe[t]

            payloadName = ProtoOAPayloadType.Name(message.payloadType)
            print(f"Unsubscribe symbol.")

        elif message.payloadType == ProtoOASpotEvent().payloadType:
            """
            Subscribe to symbols
            """
            res = Protobuf.extract(message)
            # For now, let's try ignore getting real symbol name
            symbol = utility.gSymbolData[res.symbolId]

            # If data is 0, dont insert, later disrupt my script miscalculate or mistaken that can breakeven now
            if res.bid == 0 or res.ask == 0:
                pass
            else:
                # If exists, just update the bid/ask price, else, write into dictionary
                with running_position.g_lock:
                    if res.symbolId in running_position.g_subscribe:
                        running_position.g_subscribe[res.symbolId]["bid"] = int(res.bid)
                        running_position.g_subscribe[res.symbolId]["ask"] = int(res.ask)
                    else:
                        running_position.g_subscribe[res.symbolId] = {"symbol": str(symbol), "bid": int(res.bid), "ask": int(res.ask), "NumOfUser": int(0)}

        elif message.payloadType == ProtoOAReconcileRes().payloadType:
            """
            Get list of pending orders and running positions of account
            """
            res = Protobuf.extract(message)
            positionList = []
            pendingOrderList = []
            g_pending.clear()

            pendingOrderList = res.order
            print(f"Pending Order: {len(pendingOrderList)}")
            if len(pendingOrderList) != 0:
                for o in pendingOrderList:
                    symbol = utility.gSymbolData[o.tradeData.symbolId]
                    g_pending[o.orderId] = {"Object": o , "symbol": symbol}

            # This is for setting lotsize purposes
            if SET_LOTSIZE:
                # Reason i do `if != 0` rather than `if == 0 return`
                # Niama, so close, i forgot to put SET_LOTSIZE = False in the `if == 0 return`
                if len(pendingOrderList) != 0:
                    for order in pendingOrderList:
                        symbol = utility.gSymbolData[order.tradeData.symbolId]
                        amendOrder_setLotSize(order, symbol, g_lotsize)
                SET_LOTSIZE = False
                # TODO, remove return
                return

            if len(res.position) != 0:
                positionList = res.position
            else:
                # Ensure reset it back to None
                running_position.g_subscribe.clear()
                current_time = time.time()
                dt = datetime.fromtimestamp(current_time, g_mytimezone)
                formatted_time = dt.strftime("%H%M")
                print(f"[{formatted_time}] No running order")
                # TODO, remove return
                return

            if CLOSE_ALL:
                for position in positionList:
                    # Check if exists in list
                    if position.positionId in running_position.g_positions:
                        running_position.g_positions[position.positionId]["Object"].closeAll = True
                        continue
                    # What if there's 0.01 left running, right?
                    # !note! Just so you know, TPP will cause cTrader to create new ID for the running position
                    print(f"PositionId:{position.positionId} Symbol:{utility.gSymbolData[position.tradeData.symbolId]} Volume:{position.tradeData.volume} closing position.")
                    running_position.g_command_queue.put(f"tpp {position.positionId} {position.tradeData.volume}")
                CLOSE_ALL = False

            # This list is needed because
            # This snippet of code, is in onMessageReceived
            # Unless you done running this `elif` part of onMessageReceived
            # The command i called `sub symbolId` wont be running
            # I will use this list to help me not to double subscribe to a symbolid
            subscribed_list = []
            for position in positionList:

                # Check if exists in list
                if position.positionId in running_position.g_positions:
                    continue

                if position.stopLoss == 0:
                    print(f"PositionId:{position.positionId}, stopLoss is 0. Abort.")
                    continue

                symbol = utility.gSymbolData[position.tradeData.symbolId]
                # Safety check, make sure config has the asset
                # I will only check one for the sake of simplicity,
                # Please, be full of integrity, if you add one symbol config,
                # please remember to add all
                if f"SPREAD_{symbol}" not in utility.gConfigData:
                    print(f"Symbol {symbol} has incomplete config.ini. Abort.")
                    continue

                volume_to_pip_converter = 0.01 / float(utility.gConfigData[f"MIN_LOT_{symbol}"])
                lotsize = round(position.tradeData.volume * volume_to_pip_converter, 2)

                # You can't TPP with lotsize 0.01
                if lotsize == 0.01:
                    print(f"PositionId:{position.positionId} running, is 0.01 lot. Skip.")
                    continue

                current_time = time.time()
                dt = datetime.fromtimestamp(current_time, g_mytimezone)
                formatted_time = dt.strftime("%H%M")
                if position.positionId in running_position.g_positions:
                    print(f"PositionId:{position.positionId} Symbol:{symbol} Lotsize:{running_position.g_positions[position.positionId]['Object'].lotsize} already exist in g_position!")
                    continue
                else:
                    print(f"[{formatted_time}] PositionId:{position.positionId} Symbol:{symbol} Lotsize:{lotsize} Position created.")
                obj = running_position.RunningPosition(position.positionId, position.tradeData.symbolId, symbol, position.tradeData.volume, position.tradeData.tradeSide, position.price, position.stopLoss, position.takeProfit)
                running_position.g_positions[position.positionId] = ({"Object": obj})

                if position.tradeData.symbolId not in running_position.g_subscribe and position.tradeData.symbolId not in subscribed_list:
                    subscribed_list.append(position.tradeData.symbolId)
                    running_position.g_command_queue.put(f"sub {position.tradeData.symbolId}")
                thread = threading.Thread(target=obj.run, name = str(position.positionId))
                thread.start()

                # Check if SL trigger is opposite or not, if is not, set it to opposite
                if position.stopLossTriggerMethod != ProtoOAOrderTriggerMethod.Value('OPPOSITE'):
                    print(f"PositionId:{position.positionId} Symbol:{symbol} SL trigger is not OPPOSITE. Set to OPPOISTE now.")
                    # Note: After this command
                    # If you get description: "Protection can\'t be negative"
                    # Dont worry, this means you didnt set TP
                    # Usually this happens when I trying to test demo
                    running_position.g_command_queue.put(f"ap {position.positionId} {position.stopLoss} {position.takeProfit} OPPOSITE")
                else:
                    print(f"PositionId:{position.positionId} Symbol:{symbol} SL trigger is OPPOSITE.")

        else:
            payloadName = ProtoOAPayloadType.Name(message.payloadType)
            print(f"Message received: payloadType = {message.payloadType} ({payloadName})")
            print("\n", Protobuf.extract(message))
            NEW_PRINT_HAS_HAPPENED = True

        if len(g_task_queue) != 0:
            if g_task_queue[0][2] is not None:
                if g_task_queue[0][2] == message.payloadType:
                    g_task_queue[0][2] = None

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
        send_Auth_Account()

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

    def send_Auth_Account(clientMsgId = None):
        request = ProtoOAAccountAuthReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.accessToken = ACCESS_TOKEN
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

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
        request = ProtoOAUnsubscribeSpotsReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.symbolId.append(int(symbolId))
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def User_Disconnect(clientMsgId=None): # disconnect the client
        client.stopService()
        # After disconnect
        # Your main thread script still running.
        # Terminate your main thread script
        reactor.callLater(3, callable=terminate_script)

    def terminate_script():
        os._exit(0)

    def getRunningPositions(clientMsgId=None):
        """
        This is for pending orders
        """
        current_time = time.time()
        dt = datetime.fromtimestamp(current_time, g_mytimezone)
        formatted_time = dt.strftime("%H%M")
        print(f"[{formatted_time}] getRunningPositions")

        request = ProtoOAReconcileReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def stopRunningPosition(positionId, clientMsgId=None):
        """
        Remove position from g_position since it hit SL or TP
        What about g_subscribe?
        Once this object is set to False
        It will be handled in the object destroy() function
        """
        global g_positions

        current_time = time.time()
        dt = datetime.fromtimestamp(current_time, g_mytimezone)
        formatted_time = dt.strftime("%H%M")
        print(f"[{formatted_time}] stopRunningPosition")

        # This is for the case where 0.01lot runningposition gets closed.
        if positionId not in running_position.g_positions:
            print(f"PositionId:{positionId} not in g_positions. Skip.")
            return

        # Because i still encounter issue 0.01 lot gets closed by my script
        # And is without error saynig g_positions no such positionId key exsts
        if running_position.g_positions[positionId]["Object"].lotsize == 0.01:
            print(f"PositionId:{positionId}, Symbol:{running_position.g_positions[positionId]['Object'].symbol} lotsize 0.01 alive lol")
            print(f"For now, i wont do anything, you want restart script you restart. I will print list for you")
            running_position.g_command_queue.put("p")
            running_position.g_command_queue.put("pp")
            return

        running_position.g_positions[positionId]["Object"].alive = False
        # Dont delete here, let it be deleted in to obejct itself
        # del running_position.g_positions[positionId]

    def send_Get_Symbol_List(clientMsgId=None):
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
        global ProtoOASymbolByIdRes_PRINT_ONLY

        ProtoOASymbolByIdRes_PRINT_ONLY = True
        request = ProtoOASymbolByIdReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.symbolId.append(int(symbolId))
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def updateSymbolDetail(symbolId, clientMsgId=None):
        """
        Update symbol to config.ini
        """
        request = ProtoOASymbolByIdReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.symbolId.append(int(symbolId))
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def updateSymbolDetailList(symbolIdList, clientMsgId=None):
        """
        Update symbol to config.ini, but with list
        """
        request = ProtoOASymbolByIdReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        for id in symbolIdList:
            request.symbolId.append(int(id))
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def getSymbolIDs(favourite = True):
        """
        favourite = True
        Print my favourite symbols only
        Else, all
        """
        for id, symbol in utility.gSymbolData.items():
            if favourite:
                if symbol in g_favourite_symbol:
                    print(f"ID:{id}, Symbol:{symbol}")
            else:
                    print(f"ID:{id}, Symbol:{symbol}")


    def sendCloseReq(positionId, volume, clientMsgId=None):
        """
        Take partial profit
        If you put all volume, means close all position
        """
        request = ProtoOAClosePositionReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        request.positionId = int(positionId)
        request.volume = int(volume)
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def sendAmendRunningPosition(positionId, entryPrice, takeProfit, SLTriggerMethod = 'TRADE', clientMsgId=None):
        """
        Set BE
        And set trade side to default

        Regarding set BE + few pips
        I prefer you do it at the caller of this function
        BEcause this function has been called by multiple cases
        eg:
        1. Some call it to set OPPOSITE trigger method SL
        2. Some call it to set BE
        Hence, i prefer the caller, if you set BE, just pass in entry + few pips SL

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

    def amendOrder_setLotSize(_ProtoOAOrder, symbol, lotsize, clientMsgId=None):
        """
        !Note!
        Please take note whether it will change ur SL trigger method or not
        I have verified that, it wont.
        Example: My SL is triggered by opposite bid/ask price, i verfied that
        after running this function, the trigger method still same, that is
        opposite.
        """
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

        min_lot = f"MIN_LOT_{symbol}"

        is_limit_order = False
        if _ProtoOAOrder.orderType == ProtoOAOrderType.Value('LIMIT'):
            is_limit_order = True

        if not is_limit_order:
            print(f"Warning: OrderId:{_ProtoOAOrder.orderId} Symbol:{symbol} orderType is {ProtoOAOrderType.Name(_ProtoOAOrder.orderType)} order. I will skip this one")
            return
        if min_lot not in utility.gConfigData:
            print(f"Warning: min_lot:{min_lot} is not defined for this Symbol:{symbol}. Skip.")
            return

        print(f"OrderId: {_ProtoOAOrder.orderId}")
        print(f"Symbol: {symbol}")
        print(f"tradeSide: {ProtoOATradeSide.Name(_ProtoOAOrder.tradeData.tradeSide)}")
        print(f"StopLossTakeProfit: {StopLossTakeProfit.getName(_StopLossTakeProfit)}")
        print(f"MIN_LOT: {min_lot}:{int(utility.gConfigData[min_lot])}")
        print(f"Existing lotsize:{_ProtoOAOrder.tradeData.volume}")
        print(f"Passed in lotsize:{lotsize}")
        print(f"=============================\n")

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
        request.limitPrice = float(_ProtoOAOrder.limitPrice)
        if _StopLossTakeProfit == StopLossTakeProfit.RELATIVE.value:
            request.relativeStopLoss   = int(_ProtoOAOrder.relativeStopLoss)
            request.relativeTakeProfit = int(_ProtoOAOrder.relativeTakeProfit)
        else:
            request.stopLoss   = _ProtoOAOrder.stopLoss
            request.takeProfit = _ProtoOAOrder.takeProfit
        request.volume = int(int(utility.gConfigData[min_lot]) * 100 * lotsize)
        if _ProtoOAOrder.expirationTimestamp != 0:
            request.expirationTimestamp = _ProtoOAOrder.expirationTimestamp
        request.trailingStopLoss = _ProtoOAOrder.trailingStopLoss
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def send_Authenticate_API():
        request = ProtoOAApplicationAuthReq()
        request.clientId = appClientId
        request.clientSecret = appClientSecret
        deferred = client.send(request)
        deferred.addErrback(onError)

    def Update_Symbol_List_Json():
        symbol_data = g_data_dict[ProtoOASymbolsListRes().payloadType]
        del g_data_dict[ProtoOASymbolsListRes().payloadType]
        
        filename = "symbolList_" + ACCOUNT_TYPE + ".txt"
        with open(filename, "w") as file:
            file.write(str(symbol_data))
        result = utility.convert_txt_to_json(filename, ACCOUNT_TYPE)

        if result == utility.SymbolJsonUpdate.HAS_UPDATE:
            # You have a lot of shit to update..
            # tbh, i prefer you just clear everything and restart again LOL
            # First of all, stop the subscription, else it keeps accessing the dict
            if running_position.g_subscribe:
                UPDATING_SYMBOL = True
                print(f"Symbol has update. Restarting everything!")

                # Kill all running positions, they wont TPP or whatsoever, just get destroyed
                if running_position.g_positions:
                    for p in running_position.g_positions.values():
                        p.get('Object').alive = False
                    # Give script some time to process
                    time.sleep(2)

                # Unsubscribe them all!
                # Cleanup of g_subscribe will be handled in the unsubscribe function
                for s in running_position.g_subscribe.values():
                    running_position.g_command_queue.put(f"unsub {utility.gSymbolDataSwap[s.get('symbol')]}")

            # Update the global data that hold the symbol detail
            utility.gSymbolData = None
            utility.gSymbolDataSwap = None
            utility.read_symbol_file()

    def setLotSize(lotsize, clientMsgId=None):
        """
        This is for pending orders
        """
        global g_lotsize
        global SET_LOTSIZE
        SET_LOTSIZE = True

        g_lotsize = round(float(lotsize), 2)
        request = ProtoOAReconcileReq()
        request.ctidTraderAccountId = CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def printPendingList():
        """
        """
        print("\n")
        print("Pending list now :")
        for o in g_pending.values():
            volume_to_pip_converter = 0.01 / float(utility.gConfigData[f"MIN_LOT_{o['symbol']}"])
            print(f"OrderId:{o['Object'].orderId}, Symbol: {o['symbol']}, Lotsize: {o['Object'].tradeData.volume * volume_to_pip_converter}")

    def printRunningList():
        """
        """
        print("\n")
        print("Running list now :")
        for p in running_position.g_positions.values():
            print(f"PositionId:{p['Object'].positionId}, Symbol: {p['Object'].symbol}")

    def printSubscriptionList():
        """
        """
        print("\n")
        print("Subscription list now :")
        for s in running_position.g_subscribe.values():
            print(f"{s}")

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
        global g_print_heartbeat
        g_print_heartbeat = int(value)

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
        print("qq: User_Disconnect,")
        print("m: getRunningPositions, # m = monitor, to monitor your running position, and TPP if necessary")

        print("ppp: printPendingList,")
        print("pp: printRunningList,")
        print("p: printSubscriptionList,")

        print("gsl: getSymbolIDs, # gsl = get symbol list. List the symbol and their ID")
        print("gsd: getSymbolDetail, # gsd = get symbol detail, call `sd symbolId`")
        print("us: send_Get_Symbol_List, # us = update symbol list json file")
        print("usd: updateSymbolDetail, # usd = update symbol detail to config.ini, call `us symbolId`")

        print("lt: setLotSize, # lt = lot. Set lot size. Call like this `lt 100`, `lt 0.01`")
        print("r: refresh_RAM, # Refresh global variable with latest value")
        print("test: test,")

    def test(clientMsgId=None):
        pass
        # symbolIdList = []
        # for s in g_favourite_symbol:
        #     symbolId = utility.gSymbolDataSwap[s]
        #     symbolIdList.append(symbolId)

        # updateSymbolDetailList(symbolIdList)

    commands = {
        "help": showHelp,
        "set": setAccount, # Set global variable account ID
        "ver": sendProtoOAVersionReq, # Show version
        "auth": sendProtoOAGetAccountListByAccessTokenReq, # Authenticate all accounts
        "acc": getAllAccounts, # Get all account details
        "cur": getCurrentAccount, # Get current acc
        "renew": renewAccessToken, # Renew access & refresh token
        "hb": setHeartbeat, # Set print heartbeat true or false. Call it like this `hb 1`
        "qq": User_Disconnect,
        "sub": sendProtoOASubscribeSpotsReq, # subscribe to asset, call it like this `sub 41`
        "unsub": sendProtoOAUnsubscribeSpotsReq, # UNsubscribe to asset, call it like this `unsub 41`
        "tpp": sendCloseReq, # Take partial profit, call like this `tpp positionid volume` (In volume, check MIN_LOT_SYMBOL in config.ini)
        "ap": sendAmendRunningPosition, # Amend Running Position, call `ap positionId stopLoss takeProfit 'TRADE'`
        "m": getRunningPositions, # m = monitor, to monitor your running position, and TPP if necessary

        "ppp": printPendingList,
        "pp": printRunningList,
        "p": printSubscriptionList,

        "gsl": getSymbolIDs, # gsl = get symbol list. List the symbol and their ID
        "gsd": getSymbolDetail, # gsd = get symbol detail, call `sd symbolId`
        "us": send_Get_Symbol_List, # us = update symbol list json file
        "usd": updateSymbolDetail, # usd = update symbol detail to config.ini, call `us symbolId`
        "usdl": updateSymbolDetailList, # usdl = update symbol detail with list of symbolIds to config.ini, call `us [41, 42, 43...]`

        "ltoid": amendOrder_setLotSize, # ltid = lotsize with order ID, call `ltoid orderId lotsize`
        "lt": setLotSize, # lt = lot. Set lot size. Call like this `lt 100`, `lt 0.01`
        "r": refresh_RAM, # Refresh global variable with latest value
        "test": test,
    }

    def executeUserCommand():
        global g_task_queue
        global NEW_PRINT_HAS_HAPPENED

        # For starting, wait until connected,
        # The function handle connection will
        # add entries into this queue
        while len(g_task_queue) == 0:
            continue

        try:
            while True:
                while len(g_task_queue) == 0:
                    current_time = time.time()
                    dt = datetime.fromtimestamp(current_time, g_mytimezone)
                    formatted_time = dt.strftime("%H%M")
                    print("\n=====================================\n")
                    userInput = input(f"[{formatted_time}] Cmd (Rmb Termux eats 1 char): ")
                    print(f"Cmd typed: {userInput}")

                    # You have to find out which message receives will be receiving from
                    # server and does not require user to issue command
                    # eg: Heartbeat
                    if NEW_PRINT_HAS_HAPPENED:
                        print(f"A new print to console message has happened. Retype your command")
                        NEW_PRINT_HAS_HAPPENED = False
                        continue

                    userInputSplit = userInput.split(" ")
                    if not userInputSplit:
                        print("Command split error: ", userInput)
                        continue

                    command = userInputSplit[0]
                    parameters = None
                    try:
                        parameters = [parameter if parameter[0] != "*" else parameter[1:] for parameter in userInputSplit[1:]]
                    except:
                        print("Invalid parameters: ", userInput)
                        continue

                    if command not in commands:
                        print("Invalid Command: ", userInput)
                        continue

                    g_task_queue.append([commands[command], parameters, None, None])


        # !CTRL C!
        # To detech & handle CTRL C, but this will not work
        # Due to `reactor.run` is being treated as main thread
        except KeyboardInterrupt:
            print(f"CTRL C is pressed")
        # Detect CTRL D
        except EOFError:
            print(f"Disconnect & Terminate script")
            User_Disconnect()

    def processCommand():
        global g_task_queue
        
        while True:
            while len(g_task_queue) != 0:
                
                # Usually [2] is waiting for server to reply
                # Wait until server finish replying
                # There's a reason why i dont use current_task = g_task_queue[0]
                # and then check current_task instead
                # Because once received server reply, i will modify the g_task_queue
                # If i use current_task, forever stuck in loop
                if g_task_queue[0][2] is not None:
                    while g_task_queue[0][2] is not None:
                        continue
                    # One done replying, this task is done, next
                    g_task_queue.pop(0)
                    continue

                current_task = g_task_queue[0]
                
                # [0] is function, if is None, means the current task
                # is waiting for server to reply to me
                function = current_task[0]
                parameters = current_task[1]
                if parameters is None:
                    parameters = []
                    
                # Run the function
                function(*parameters)
                
                # After run the function only then you pop it
                # if not ah, the executeUserCommand thread will
                # prompt for user input before you finish executing
                # the previous command
                g_task_queue.pop(0)


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

