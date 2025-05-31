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

# When you run `acc`, it will set this to TRUE
# Then it will set it back to false
# Purpose is to print your accounts only
# When you run `auth`, it wont modify this variable,
# leads to authenticating your acc
gAuthPrintOnly = False

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

        # Let's try reconenct back
        # I dk whether is this a good way but lets try
        reactor.callLater(3, callable=User_Reconnect)

    def onMessageReceived(client, message): # Callback for receiving all messages
        # Initially i put at `if elif`
        # Just realized, it is within function
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

            # print("\n==================================")
            # print(res)
            # print("==================================\n")

            # executionType = res.executionType
            positionStatus = res.position.positionStatus
            isServerEvent = res.isServerEvent

            if isServerEvent == True and positionStatus == ProtoOAPositionStatus.Value('POSITION_STATUS_OPEN'):
                """
                New position created & running
                Entered a trade

                Known Issue
                if you TPP, the leftover position is treated as opened a new position
                and getRunningPOsition runs again
                And it is known that, the new position, will have the same position ID as previous
                """
                current_time = time.time()
                dt = datetime.fromtimestamp(current_time, g_mytimezone)
                # Format the time as "HHMM", GMT+8
                formatted_time = dt.strftime("%H%M")
                print(f"[{formatted_time}] getRunningPositions")
                getRunningPositions()

            elif isServerEvent == True and positionStatus == ProtoOAPositionStatus.Value('POSITION_STATUS_CLOSED'):
                """
                Position closed, either hit TP or SL

                Known issue
                once u TPP, everything goes well
                but if hit breakeven, it run stopRunningPosition again
                But ok, i believe after TPP, it is left 0.01 lot and it will skip


                I also notice, sometimes, i randomly get isServerEvent == True and positionStatus == ProtoOAPositionStatus.Value('POSITION_STATUS_CLOSED')
                For twice, i rmb is after TPP and set BE
                Then script somehow trigger stopRunningPosition
                And also, my running position is 0.01, but somehow, it able to close my position

                I also rmb when i put print(res)
                It output values when i did nothing, i was watching the screen, waiting it hit my limit order
                I couldn't understand what cause it to receive the message,
                Maybe i need stronger checking protection
                """
                current_time = time.time()
                dt = datetime.fromtimestamp(current_time, g_mytimezone)
                # Format the time as "HHMM", GMT+8
                formatted_time = dt.strftime("%H%M")
                print(f"[{formatted_time}] stopRunningPosition")
                # Because, if you have 0.01lot left running, once it closed,
                # will trigger this block also
                # My handling will be, check if g_position has the runningposition
                # if no, skip. I lazy to check res.position.tradeData.volume,
                # I scare the script is too overloaded, need reduce latency
                # If a problem can be solved in simple way, let it be that way
                stopRunningPosition(res.position.positionId)

                # Call again to make it run "No running order" & clears g_subscribe
                # In case g_subscribe is not cleared
                # Maybe no need first? I dk
                # getRunningPositions()
            return

        elif message.payloadType == ProtoHeartbeatEvent().payloadType:
            if g_print_heartbeat:
                current_time = time.time()
                dt = datetime.fromtimestamp(current_time, g_mytimezone)

                # Format the time as "HHMM", GMT+8
                formatted_time = dt.strftime("%H%M")

                print(f"[{formatted_time}] Heartbeat Received.")

            # Set limit order 100lot and 0.02lot in specified time
            # Everyday, 4am set to 100lot, 830am set 0.02lot
            # Friday special, 2am set to 100lot
            # If is saturday 830am, dont do anything, until monday 830am
            # Only modify pending order
            now = datetime.now(g_mytimezone)
            current_time = now.time()  # Get the current time as a time object
            current_time_for_myself = time.time()
            dt = datetime.fromtimestamp(current_time_for_myself, g_mytimezone)
            # Format the time as "HHMM", GMT+8
            formatted_time = dt.strftime("%H%M")
            current_weekday = now.strftime("%A")

            lotsize = 0

            # Define standard times
            # 4AM & 830AM
            # Close order at 4am (Set pending order lotsize 100)
            # Open order at 830am (Set pending order lotsize 0.02)
            time_checks = [time2(4, 0), time2(8, 30)]

            # If today is monday, only market open, no closing,
            # because before monday (which is sun) already closed
            # What i want to say is, saturday morning 2am already closed until monday 830am
            if current_weekday == "Monday":
                time_checks = [None, time2(8, 30)]
                if current_time > time_checks[1]:
                    if current_weekday not in g_time_checks_record:
                        print(f"Today is {current_weekday} {formatted_time}. Market opening.")
                        lotsize = 0.02
                        g_time_checks_record = {current_weekday : lotsize}

            # If today is saturday, only market close. See above & you know what 7 im saying
            elif current_weekday == "Saturday":
                time_checks = [time2(2, 0), None]
                if current_time > time_checks[0]:
                    if current_weekday not in g_time_checks_record:
                        print(f"Today is {current_weekday} {formatted_time}. Market closing. Also closing running positions.")
                        lotsize = 100
                        g_time_checks_record = {current_weekday : lotsize}

            # If today is sunday, None
            elif current_weekday == "Sunday":
                time_checks = []
                if current_weekday not in g_time_checks_record:
                    g_time_checks_record = {current_weekday : lotsize}

            # Normal day, my time_checks shall have close & open time
            else:
                # Matket closing
                if current_time > time_checks[0] and current_time < time_checks[1]:
                    if current_weekday not in g_time_checks_record or g_time_checks_record.get(current_weekday) != 100:
                        print(f"Today is {current_weekday} {formatted_time}. Market closing.")
                        lotsize = 100
                        g_time_checks_record = {current_weekday : lotsize}
                # Market opening
                elif current_time > time_checks[1]:
                    if current_weekday not in g_time_checks_record or g_time_checks_record.get(current_weekday) != 0.02:
                        print(f"Today is {current_weekday} {formatted_time}. Market opening.")
                        lotsize = 0.02
                        g_time_checks_record = {current_weekday : lotsize}
                else:
                    print(f"Unhandled time. current_weekday:{current_weekday}, formatted_time:{formatted_time}")
                    g_time_checks_record = {current_weekday : -1}


            # After a lot of checking above, here handles the aftermath
            if lotsize != 0:
                if len(g_pending) != 0:
                    for value in g_pending.values():
                        volume_to_pip_converter = 0.01 / float(utility.gConfigData[f"VOLUME_PER_LOT_{value['symbol']}"])
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
                    CLOSE_ALL = True
                    getRunningPositions()

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
                    print(f"Symbol has update. Restarting everything!")

                    # Kill all running positions, they wont TPP or whatsoever, just get destroyed
                    if running_position.g_positions:
                        for p in running_position.g_positions.values():
                            p.get('Object').alive = True
                        # Give script some time to process
                        time.sleep(2)

                    # Unsubscribe them all!
                    # Cleanup of g_subscribe will be handled in the unsubscribe function
                    for s in running_position.g_subscribe.values():
                        running_position.g_command_queue.put(f"unsub {symbols_old_NAME_first_dict[s.get('symbol')]}")

                utility.gSymbolData = None
                utility.read_symbol_file()

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
                    UPDATING_SYMBOL = False
                    g_subscribe_count = 0
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
                return

            # If exists, just update the bid/ask price, else, write into dictionary
            with running_position.g_lock:
                if res.symbolId in running_position.g_subscribe:
                    running_position.g_subscribe[res.symbolId]["bid"] = int(res.bid)
                    running_position.g_subscribe[res.symbolId]["ask"] = int(res.ask)
                else:
                    running_position.g_subscribe[res.symbolId] = {"symbol": str(symbol), "bid": int(res.bid), "ask": int(res.ask), "NumOfUser": int(0)}

        # Get list of pending orders and running positions of account
        elif message.payloadType == ProtoOAReconcileRes().payloadType:
            res = Protobuf.extract(message)
            positionList = []
            pendingOrderList = []
            g_pending.clear()

            pendingOrderList = res.order
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
                return

            if len(res.position) != 0:
                positionList = res.position
            else:
                # Ensure reset it back to None
                running_position.g_subscribe.clear()
                current_time = time.time()
                dt = datetime.fromtimestamp(current_time, g_mytimezone)
                # Format the time as "HHMM", GMT+8
                formatted_time = dt.strftime("%H%M")
                print(f"[{formatted_time}] No running order")
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

                volume_to_pip_converter = 0.01 / float(utility.gConfigData[f"VOLUME_PER_LOT_{symbol}"])
                lotsize = round(position.tradeData.volume * volume_to_pip_converter, 2)

                # You can't TPP with lotsize 0.01
                if lotsize == 0.01:
                    continue

                current_time = time.time()
                dt = datetime.fromtimestamp(current_time, g_mytimezone)
                # Format the time as "HHMM", GMT+8
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

    def User_Disconnect(clientMsgId=None): # disconnect the client
        client.stopService()
        # After disconnect
        # Your main thread script still running.
        # Terminate your main thread script
        reactor.callLater(3, callable=terminate_script)

    def User_Reconnect():
        client.startService()

    def terminate_script():
        os._exit(0)

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
        What about g_subscribe?
        Once this object is set to False
        It will be handled in the object destroy() function
        """
        global g_positions

        # This is for the case where 0.01lot runningposition gets closed.
        if positionId not in running_position.g_positions:
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
        request.symbolId.append(int(symbolId))
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

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

        volume_per_lot = f"VOLUME_PER_LOT_{symbol}"

        is_limit_order = False
        if _ProtoOAOrder.orderType == ProtoOAOrderType.Value('LIMIT'):
            is_limit_order = True

        if not is_limit_order:
            print(f"Warning: OrderId:{_ProtoOAOrder.orderId} Symbol:{symbol} orderType is {ProtoOAOrderType.Name(_ProtoOAOrder.orderType)} order. I will skip this one")
            return
        if volume_per_lot not in utility.gConfigData:
            print(f"Warning: volume_per_lot:{volume_per_lot} is not defined for this Symbol:{symbol}. Skip.")
            return

        print(f"OrderId: {_ProtoOAOrder.orderId}")
        print(f"Symbol: {symbol}")
        print(f"tradeSide: {ProtoOATradeSide.Name(_ProtoOAOrder.tradeData.tradeSide)}")
        print(f"StopLossTakeProfit: {StopLossTakeProfit.getName(_StopLossTakeProfit)}")
        print(f"volume_per_lot: {volume_per_lot}:{int(utility.gConfigData[volume_per_lot])}")
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
        request.volume = int(int(utility.gConfigData[volume_per_lot]) * 100 * lotsize)
        if _ProtoOAOrder.expirationTimestamp != 0:
            request.expirationTimestamp = _ProtoOAOrder.expirationTimestamp
        request.trailingStopLoss = _ProtoOAOrder.trailingStopLoss
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

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
            volume_to_pip_converter = 0.01 / float(utility.gConfigData[f"VOLUME_PER_LOT_{o['symbol']}"])
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
        print("m: getRunningPositions, # m = monitor, monitor ur trade. Your main command")
        print("ppp: printPendingList,")
        print("pp: printRunningList,")
        print("p: printSubscriptionList,")
        print("s: getSymbolList, # Update symbol files")
        print("sd: getSymbolDetail, # sd = symbol detail, call `sd symbolId`")
        print("lt: setLotSize, # lt = lot. Set lot size. Call like this `lt 100`, `lt 0.01`")
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

        # For now, disable this command, use CTRL D to disconnect
        # Because in disconnect message receive, i put reconnect
        # "qq": User_Disconnect,

        "sub": sendProtoOASubscribeSpotsReq, # subscribe to asset, call it like this `sub 41`
        "unsub": sendProtoOAUnsubscribeSpotsReq, # UNsubscribe to asset, call it like this `unsub 41`
        "tpp": sendCloseReq, # Take partial profit, call like this `tpp positionid volume` (In volume, check VOLUME_PER_PIP_SYMBOL in config.ini)
        "ap": sendAmendRunningPosition, # Amend Running Position, call `ap positionId stopLoss takeProfit "TRADE"`
        "m": getRunningPositions, # m = monitor, to monitor your running position, and TPP if necessary
        "ppp": printPendingList,
        "pp": printRunningList,
        "p": printSubscriptionList,
        "s": getSymbolList, # Update symbol files
        "sd": getSymbolDetail, # sd = symbol detail, call `sd symbolId`
        "ltoid": amendOrder_setLotSize, # ltid = lotsize with order ID, call `ltoid orderId lotsize`
        "lt": setLotSize, # lt = lot. Set lot size. Call like this `lt 100`, `lt 0.01`
        "r": refresh_RAM, # Refresh global variable with latest value
        "test": test,
    }

    def executeUserCommand():
        try:
            while True:
                current_time = time.time()
                dt = datetime.fromtimestamp(current_time, g_mytimezone)
                # Format the time as "HHMM", GMT+8
                formatted_time = dt.strftime("%H%M")
                print("\n=====================================\n")
                userInput = input(f"[{formatted_time}] Cmd (Rmb Termux eats 1 char): ")
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

