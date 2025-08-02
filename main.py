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
from dotenv import load_dotenv
load_dotenv()

import os
from ctrader_open_api import Client, Protobuf, TcpProtocol, Auth, EndPoints
from ctrader_open_api.endpoints import EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
from twisted.internet import reactor
import datetime
from datetime import datetime, time as time2, timedelta
import utility
import fileinput
import threading
import time
from globalpy import GlobalVar, SymbolJsonUpdate, StopLossTakeProfit
import math
import copy

utility.read_config_file() # Read config.ini
utility.read_symbol_file(GlobalVar.ACCOUNT_TYPE) # Read symbolList_demo/live.json
utility.populate_favourite_symbol()

# List of server message to ignore
gPayloadIgnoreList = [
    ProtoOASubscribeSpotsRes().payloadType,
    ProtoOAAccountLogoutRes().payloadType,
    # ProtoHeartbeatEvent().payloadType,
    # ProtoOAExecutionEvent().payloadType
]

client = Client(EndPoints.PROTOBUF_LIVE_HOST if GlobalVar.ACCOUNT_TYPE.lower() == "live" else EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)

if __name__ == "__main__":

    def connected(client):
        """
        Callback for client connection
        This is the 1st function run, after connection has been established
        """
        current_time = time.time()
        dt = datetime.fromtimestamp(current_time, GlobalVar.g_mytimezone)
        formatted_time = dt.strftime("%H%M")
        print(f"\n[{formatted_time}] Connected. ACCOUNT_TYPE:{GlobalVar.ACCOUNT_TYPE}")

        # Startup tasks! Yay!
        GlobalVar.g_task_queue.append([send_Authenticate_API, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOAApplicationAuthRes().payloadType, "Call by send_Authenticate_API"])

        if GlobalVar.CURRENT_CTIDTRADERACCOUNTID is not None:
            GlobalVar.g_task_queue.append([send_Auth_Account, None, None, None])
            GlobalVar.g_task_queue.append([None, None, ProtoOAAccountAuthRes().payloadType, "Call by send_Auth_Account"])

        handle_symbol_update()
        handle_record_order()
        GlobalVar.g_task_queue.append([check_token_expiry, None, None, None])
        GlobalVar.g_task_queue.append([set_START_USER_COMMAND_True, None, None, None])

    def disconnected(client, reason):
        """
        Callback for client disconnection
        """
        current_time = time.time()
        dt = datetime.fromtimestamp(current_time, GlobalVar.g_mytimezone)
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
        current_time = time.time()
        dt = datetime.fromtimestamp(current_time, GlobalVar.g_mytimezone)
        formatted_time = dt.strftime("%H%M")

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
            GlobalVar.NEW_PRINT_HAS_HAPPENED = True
            res = Protobuf.extract(message)
            GlobalVar.g_data_dict[ProtoOAExecutionEvent().payloadType] = res

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

                TODO: You gotta find a way to detect if user enter market position directly
                Need to handle the SL set to opposite
                """
                GlobalVar.g_task_queue.append([Set_RunningPosition_StopLoss_To_Opposite, None, None, None])

            handle_record_order()

        elif message.payloadType == ProtoHeartbeatEvent().payloadType:
            if GlobalVar.g_print_heartbeat:
                GlobalVar.NEW_PRINT_HAS_HAPPENED = True
                print(f"\n\n[{formatted_time}] Heartbeat Received.")
            handle_time_checks()

        elif message.payloadType == ProtoOAPayloadType.Value('PROTO_OA_SYMBOL_CHANGED_EVENT'):
            """
            SYMBOL CHANGED! Update SYMBOL JSON
            """
            GlobalVar.NEW_PRINT_HAS_HAPPENED = True
            print(f"\n\n[{formatted_time}] Server sent Symbol Update message!")
            handle_symbol_update()

        elif message.payloadType == ProtoOAApplicationAuthRes().payloadType:
            print(f"API Application authorized")

        elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
            protoOAAccountAuthRes = Protobuf.extract(message)
            # If no such environment, it will be "None"
            nickname = os.getenv(f'A_{protoOAAccountAuthRes.ctidTraderAccountId}')
            print(f"Account [{protoOAAccountAuthRes.ctidTraderAccountId}: {nickname}] has been authorized")

        elif message.payloadType == ProtoOASymbolsListRes().payloadType:
            res = Protobuf.extract(message)
            GlobalVar.g_data_dict[ProtoOASymbolsListRes().payloadType] = res

        elif message.payloadType == ProtoOASymbolByIdRes().payloadType:
            """
            Symbol entity details
            Mainly is to get the MinVolume and MaxVolume, for lotsize
            """
            res = Protobuf.extract(message)
            GlobalVar.g_data_dict[ProtoOASymbolByIdRes().payloadType] = res

        elif message.payloadType == ProtoOARefreshTokenRes().payloadType:
            res = Protobuf.extract(message)
            GlobalVar.g_data_dict[ProtoOARefreshTokenRes().payloadType] = res

        elif message.payloadType == ProtoOAGetAccountListByAccessTokenRes().payloadType:
            res = Protobuf.extract(message)
            GlobalVar.g_data_dict[ProtoOAGetAccountListByAccessTokenRes().payloadType] = res

        elif message.payloadType == ProtoOAReconcileRes().payloadType:
            """
            Get list of pending orders and running positions of account
            """
            res = Protobuf.extract(message)
            GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType] = res

        else:
            payloadName = ProtoOAPayloadType.Name(message.payloadType)
            print(f"\n\n[{formatted_time}] Message received: payloadType = {message.payloadType} ({payloadName})")
            print("\n", Protobuf.extract(message))
            GlobalVar.NEW_PRINT_HAS_HAPPENED = True

        if len(GlobalVar.g_task_queue) != 0:
            if GlobalVar.g_task_queue[0][2] is not None:
                if GlobalVar.g_task_queue[0][2] == message.payloadType:
                    GlobalVar.g_task_queue[0][2] = None

    def setAccount(index, clientMsgId = None):
        """
        index is GlobalVar.g_auth_acc index
        call `acc` and you know what 7 im saying
        """
        index = int(index)

        if len(GlobalVar.g_auth_acc) == 0:
            print("Call `acc` first, to get account list")
            return

        GlobalVar.CURRENT_CTIDTRADERACCOUNTID = GlobalVar.g_auth_acc[index]["ctidTraderAccountId"]
        GlobalVar.g_task_queue.append([send_Auth_Account, None, None, None])

    def sendProtoOAVersionReq(clientMsgId = None):
        request = ProtoOAVersionReq()
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def sendProtoOAGetAccountListByAccessTokenReq(clientMsgId = None):
        GlobalVar.g_task_queue.append([getAllAccounts, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOAGetAccountListByAccessTokenRes().payloadType, "Call by getAllAccounts"])
        GlobalVar.g_task_queue.append([authenticate_all_accounts, None, None, None])

    def getAllAccounts(clientMsgId = None):
        """
        The account it displays, depends on the permission you set here
        Click on `sandbox` and you know what 7 im talking ady
        https://openapi.ctrader.com/apps
        """
        request = ProtoOAGetAccountListByAccessTokenReq()
        request.accessToken = GlobalVar.ACCESS_TOKEN
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def handle_print_all_accounts(clientMsgId = None):
        GlobalVar.g_task_queue.append([getAllAccounts, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOAGetAccountListByAccessTokenRes().payloadType, "Call by getAllAccounts"])
        GlobalVar.g_task_queue.append([print_all_accoutns, None, None, None])

    def getCurrentAccount(clientMsgId = None):
        """
        """
        nickname = os.getenv(f'A_{GlobalVar.CURRENT_CTIDTRADERACCOUNTID}')
        print(f"ctidTraderAccountId:{GlobalVar.CURRENT_CTIDTRADERACCOUNTID} Nickname: {nickname}")

    def sendProtoOAAccountLogoutReq(clientMsgId = None):
        request = ProtoOAAccountLogoutReq()
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def send_Auth_Account(clientMsgId = None):
        request = ProtoOAAccountAuthReq()
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        request.accessToken = GlobalVar.ACCESS_TOKEN
        deferred = client.send(request, clientMsgId = clientMsgId)
        deferred.addErrback(onError)

    def User_Disconnect(clientMsgId = None): # disconnect the client
        client.stopService()
        # After disconnect
        # Your main thread script still running.
        # Terminate your main thread script
        reactor.callLater(3, callable=terminate_script)

    def terminate_script(clientMsgId = None):
        os._exit(0)

    def send_close_all_running_positions(RepeatedCompositeContainer_position, get_dict = False, clientMsgId = None):
        if get_dict:
            res = GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]
            RepeatedCompositeContainer_position = res.position

        # During debugging, i do `type(RepeatedCompositeContainer_position)`
        # And i saw class 'google,protobuf.pyext._message.RepeatedCompositeContainer'
        # Comment first, my PC pass this code, my phone says no module named google.protobuf.pyext._message
        # from google.protobuf.pyext._message import RepeatedCompositeContainer
        # if not isinstance(RepeatedCompositeContainer_position, RepeatedCompositeContainer):
        #     raise TypeError(f"Expected ProtoOAPosition, but got {type(RepeatedCompositeContainer_position).__name__}")

        for position in RepeatedCompositeContainer_position:
            positionId = position.positionId
            symbol_name = GlobalVar.g_Symbol_Data_ID_As_Key[position.tradeData.symbolId]
            volume = position.tradeData.volume

            print(f"PositionId:{position.positionId} Symbol:{symbol_name} Volume:{volume} closing position.")

            request = ProtoOAClosePositionReq()
            request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
            request.positionId = int(positionId)
            request.volume = int(volume)
            deferred = client.send(request, clientMsgId=clientMsgId)
            deferred.addErrback(onError)

    def handle_refresh_token(clientMsgId = None):
        res = GlobalVar.g_data_dict[ProtoOARefreshTokenRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOARefreshTokenRes().payloadType]

        today = datetime.now(GlobalVar.g_mytimezone)
        # Add 2,628,000 seconds, that's the token expiry period, saw from the website
        future = today + timedelta(seconds=2628000)
        # Format as DD-MMM-YY
        formatted_date = future.strftime('%d-%b-%y')

        updates = {"ACCESS_TOKEN":res.accessToken, "REFRESH_TOKEN":res.refreshToken, "LAST_UPDATED":formatted_date}
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

    def Set_RunningPosition_StopLoss_To_Opposite(clientMsgId = None):
        res = GlobalVar.g_data_dict[ProtoOAExecutionEvent().payloadType]
        del GlobalVar.g_data_dict[ProtoOAExecutionEvent().payloadType]

        current_time = time.time()
        dt = datetime.fromtimestamp(current_time, GlobalVar.g_mytimezone)
        formatted_time = dt.strftime("%H%M")
        print(f"[{formatted_time}] Set Stoploss to Opposite")

        symbol = GlobalVar.g_Symbol_Data_ID_As_Key[res.position.tradeData.symbolId]
        if res.position.stopLoss == 0:
            print(f"PositionId:{res.position.positionId} Symbol:{symbol} stopLoss is 0. Abort.")
            return

        # Check if SL trigger is opposite or not, if is not, set it to opposite
        if res.position.stopLossTriggerMethod != ProtoOAOrderTriggerMethod.Value('OPPOSITE'):
            print(f"PositionId:{res.position.positionId} Symbol:{symbol} SL trigger is not OPPOSITE. Set to OPPOISTE now.")
            # Note: After this command
            # If you get description: "Protection can\'t be negative"
            # Dont worry, this means you didnt set TP
            # Usually this happens when I trying to test demo

            param = [res.position.positionId, res.position.stopLoss, res.position.takeProfit, "OPPOSITE"]
            GlobalVar.g_task_queue.append([send_Set_StopLoss_To_Opposite, param, None, None])
        else:
            print(f"PositionId:{res.position.positionId} Symbol:{symbol} SL trigger is OPPOSITE.")

    def send_Get_List_Of_Running_And_Pending_Orders(clientMsgId = None):
        request = ProtoOAReconcileReq()
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def send_Get_Symbol_List(clientMsgId = None):
        request = ProtoOASymbolsListReq()
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def getSymbolDetail(symbolId, clientMsgId = None):
        """
        """
        symbolId = int(symbolId)
        param = [symbolId]
        GlobalVar.g_task_queue.append([send_Get_Symbol_Detail, param, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOASymbolByIdRes().payloadType, "Call by send_Get_Symbol_Detail"])
        GlobalVar.g_task_queue.append([print_symbol_detail, None, None, None])

    def print_symbol_detail(clientMsgId = None):
        res = GlobalVar.g_data_dict[ProtoOASymbolByIdRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOASymbolByIdRes().payloadType]
        print(res)

    def update_lotsize_for_pending_order(lotsize, delete_dict = True, clientMsgId = None):
        """
        """
        lotsize = round(float(lotsize), 2)
        res = GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]

        if delete_dict:
            del GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]

        orderList = res.order
        if len(orderList) == 0:
            print(f"No pending orders to update lotsize")
            return

        for order in orderList:
            # Get MIN_LOT_XAUUSD from config.ini
            symbol_name         = GlobalVar.g_Symbol_Data_ID_As_Key[order.tradeData.symbolId]
            MIN_LOT_VALUE       = int(GlobalVar.g_Config_Data[f"MIN_LOT_VOLUME_{symbol_name}"])
            MAX_LOT_VALUE       = int(GlobalVar.g_Config_Data[f"MAX_LOT_VOLUME_{symbol_name}"])
            volume_to_pip_converter = 0.01 / float(MIN_LOT_VALUE)
            lotsize_special     = "None"

            # Check whether is same lotsize or not
            # If lotsize is 100, just use the maximum volume from config.ini
            if lotsize == 100:
                if order.tradeData.volume == MAX_LOT_VALUE:
                    continue
                order.tradeData.volume = MAX_LOT_VALUE

            elif lotsize == -1:
                """
                Back to their respective lotsize
                Then, clear that record from the record.ini
                """
                proceed = True
                section = 'HEADER'
                lotsize_special = int(GlobalVar.g_Record_Data[section][order.orderId]) / MIN_LOT_VALUE / 100
                if order.tradeData.volume * volume_to_pip_converter == lotsize_special:
                    """
                    If existing pending order lotsize is already same as the one in record.ini,
                    then no need to update
                    """
                    proceed = False
                else:
                    order.tradeData.volume = int(GlobalVar.g_Record_Data[section][order.orderId])

                # Remove from the list, later i will call function to write this newly reduced list into record.ini
                GlobalVar.g_Record_Data.remove_option(section, order.orderId)

                if proceed == False:
                    continue

            else:
                if order.tradeData.volume * volume_to_pip_converter == lotsize:
                    continue
                # lotsize = 0.02, volume_to_pip_converter = 0.00001
                # Tried to use int(), but i get 1999 instead
                # Hence, use math.ceil()
                order.tradeData.volume = math.ceil(lotsize / volume_to_pip_converter)

            if lotsize == -1:
                print(f"Pending order {order.orderId}:{symbol_name} change lotsize to {lotsize_special} lot")
            else:
                print(f"Pending order {order.orderId}:{symbol_name} change lotsize to {lotsize} lot")
            param = [order]
            GlobalVar.g_task_queue.append([send_Amend_Pending_Order_Lotsize, param, None, None])

    def updateSymbolDetail(symbolIdList, clientMsgId = None):
        """
        Update symbol to config.ini
        """
        # Convert non-list input into a list
        if not isinstance(symbolIdList, list):
            symbolIdList = [symbolIdList]

        param = []
        param.append(symbolIdList)
        GlobalVar.g_task_queue.append([send_Get_Symbol_Detail, param, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOASymbolByIdRes().payloadType, None])
        GlobalVar.g_task_queue.append([Update_Symbol_Detail, None, None, None])

    def updateSymbolDetailAccordingToFavourite(clientMsgId = None):
        """
        Update symbol to config.ini
        But this time, it will auto,
        It will get list of symbols from g_favourite_symbol
        """
        the_list = []
        for symbolID in GlobalVar.g_favourite_symbol.values():
            the_list.append(symbolID)
        param = []
        param.append(the_list)
        GlobalVar.g_task_queue.append([send_Get_Symbol_Detail, param, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOASymbolByIdRes().payloadType, None])
        GlobalVar.g_task_queue.append([Update_Symbol_Detail, None, None, None])

    def getSymbolIDs(favourite = True, clientMsgId = None):
        """
        favourite = True
        Print my favourite symbols only
        Else, all
        """
        # I will call function like this `getSymbolIDs 0`
        # "0" will be passed as string into function
        # It will evaluate to False
        favourite = bool(int(favourite)) if favourite.isdigit() else False
        for id, symbol in GlobalVar.g_Symbol_Data_ID_As_Key.items():
            if favourite:
                if symbol in GlobalVar.g_favourite_symbol.keys():
                    print(f"ID:{id}, Symbol:{symbol}")
            else:
                    print(f"ID:{id}, Symbol:{symbol}")

    def send_Set_StopLoss_To_Opposite(positionId, entryPrice, takeProfit, SLTriggerMethod = 'TRADE', clientMsgId = None):
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
        positionId = int(positionId)
        entryPrice = round(float(entryPrice), 2)
        takeProfit = round(float(takeProfit), 2)
        SLTriggerMethod = str(SLTriggerMethod)

        request = ProtoOAAmendPositionSLTPReq()
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        request.positionId  = positionId
        request.stopLoss    = entryPrice
        request.takeProfit  = takeProfit
        request.stopLossTriggerMethod = ProtoOAOrderTriggerMethod.Value(SLTriggerMethod)
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def send_Amend_Pending_Order_Lotsize(orderEntity, clientMsgId = None):
        """
        !Note!
        Please take note whether it will change ur SL trigger method or not
        I have verified that, it wont.
        Example: My SL is triggered by opposite bid/ask price, i verfied that
        after running this function, the trigger method still same, that is
        opposite.
        """
        if not isinstance(orderEntity, ProtoOAOrder):
            raise TypeError(f"Expected ProtoOAOrder, but got {type(orderEntity).__name__}")

        symbol_name = GlobalVar.g_Symbol_Data_ID_As_Key[orderEntity.tradeData.symbolId]

        _StopLossTakeProfit = -1
        if orderEntity.relativeStopLoss != 0 or orderEntity.relativeTakeProfit != 0:
            _StopLossTakeProfit = StopLossTakeProfit.RELATIVE.value
        elif orderEntity.stopLoss != 0 or orderEntity.takeProfit != 0:
            _StopLossTakeProfit = StopLossTakeProfit.ABSOLUTE.value
        else:
            print(f"Warning: Abnormal absolute & realtive TP SL detected. Skip")
            print(f"OrderId:{orderEntity.orderId} Symbol:{symbol_name}")
            print(f"relativeStopLoss:{orderEntity.relativeStopLoss}")
            print(f"relativeTakeProfit:{orderEntity.relativeTakeProfit}")
            print(f"stopLoss:{orderEntity.stopLoss}")
            print(f"takeProfit:{orderEntity.takeProfit}")
            return

        request = ProtoOAAmendOrderReq()
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        request.orderId = int(orderEntity.orderId)
        # regarding orderEntity.relativeStopLoss
        # It has if you NEVER place by entering price, but rather by dragging
        # It has value 0 if you entered using price,
        # And it will use orderEntity.stopLoss, which is absolute stopLoss price
        # And if either one is 0, request will fail
        # Ok, sometimes it changed to use either & i dk how i triggered that
        # Best is, ur coding, should cover both
        request.limitPrice = float(orderEntity.limitPrice)
        if _StopLossTakeProfit == StopLossTakeProfit.RELATIVE.value:
            request.relativeStopLoss   = int(orderEntity.relativeStopLoss)
            request.relativeTakeProfit = int(orderEntity.relativeTakeProfit)
        else:
            request.stopLoss   = orderEntity.stopLoss
            request.takeProfit = orderEntity.takeProfit
        request.volume = int(orderEntity.tradeData.volume)
        if orderEntity.expirationTimestamp != 0:
            request.expirationTimestamp = orderEntity.expirationTimestamp
        request.trailingStopLoss = orderEntity.trailingStopLoss
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def send_Authenticate_API(clientMsgId = None):
        request = ProtoOAApplicationAuthReq()
        request.clientId = GlobalVar.APP_CLIENT_ID
        request.clientSecret = GlobalVar.APP_CLIENT_SECRET
        deferred = client.send(request)
        deferred.addErrback(onError)

    def handle_time_checks(clientMsgId = None):
        """
        1. Modify Pending Order lotsizes according to time
        2. Close all running order according to time
        """
        now = datetime.now(GlobalVar.g_mytimezone)
        current_time = now.time()
        current_time_for_myself = time.time()
        dt = datetime.fromtimestamp(current_time_for_myself, GlobalVar.g_mytimezone)
        formatted_time = dt.strftime("%H%M")
        current_weekday = now.strftime("%A")

        lotsize = 0

        weekday_list = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

        if current_weekday in weekday_list:
            time_checks_close = time2(20, 0)
            time_checks_open = time2(8, 30)

            # Market closing, weekend
            if current_weekday == "Friday" and current_time > time_checks_close:
                if current_weekday not in GlobalVar.g_time_checks_record:
                    GlobalVar.NEW_PRINT_HAS_HAPPENED = True
                    print(f"\n\nToday is {current_weekday} {formatted_time}. Market closing. Set all pending order lotsize to max.")
                    lotsize = 100
                    GlobalVar.g_time_checks_record = {current_weekday : lotsize}

            # Market open, set lotsize to my lotsize
            else:
                if current_time > time_checks_open:
                    if current_weekday not in GlobalVar.g_time_checks_record:
                        GlobalVar.NEW_PRINT_HAS_HAPPENED = True
                        print(f"\n\nToday is {current_weekday} {formatted_time}. Market opening. Set all pending order lotsize to their respective lotsize.")
                        lotsize = -1
                        GlobalVar.g_time_checks_record = {current_weekday : lotsize}

        else:
            if current_weekday not in GlobalVar.g_time_checks_record:
                GlobalVar.NEW_PRINT_HAS_HAPPENED = True
                print(f"\n\nToday is {current_weekday} {formatted_time}.")
                GlobalVar.g_time_checks_record = {current_weekday : lotsize}
                # Allow me to do this lazy way first.., is better to seperate this from this function
                check_token_expiry()

        # After a lot of checking above, here handles the aftermath
        if lotsize != 0:
            # Allow me to do this lazy way first.., is better to seperate this from this function
            check_token_expiry()

            GlobalVar.NEW_PRINT_HAS_HAPPENED = True
            param = [lotsize, False]
            GlobalVar.g_task_queue.append([send_Get_List_Of_Running_And_Pending_Orders, None, None, None])
            GlobalVar.g_task_queue.append([None, None, ProtoOAReconcileRes().payloadType, "Call by send_Get_List_Of_Running_And_Pending_Orders"])
            # I will delete the dict myself, manually, at the end of this function
            GlobalVar.g_task_queue.append([update_lotsize_for_pending_order, param, None, None])

            if lotsize == -1:
                """
                If lotsize -1, means update_lotsize_for_pending_order will restore all pending orders
                with their respective lotsize.
                If that happens, means it will remove updated records in GlobalVar.g_Record_Data
                If that happens, means need to update record.ini with latest data
                """
                GlobalVar.g_task_queue.append([utility.write_config_file, None, None, None])

            # Close all running position
            # Let's cancle for the moment, what if it's -P/L and you closed it, right?
            # Let's just set lotsize to 100 on friday earlier time
            # if current_weekday == "Saturday":
            #     param = [None, True]
            #     GlobalVar.g_task_queue.append([send_close_all_running_positions, param, None, None])

            # Manual delete
            param = [ProtoOAReconcileRes().payloadType]
            GlobalVar.g_task_queue.append([del_data_dict, param, None, None])

            # Manual delete (old approach)
            # !note! there is one danger with this approach
            # The above, you append your task
            # But rmb, until your this function exits,
            # the above tasks are not run at all yet
            # And here you deleted it before the above tasks can run
            # So rmb, after you code "g_task_queue.append()",
            # you should code append task after that!
            # No more data manipulation after that!
            # Any data manipulation shall be added into your task queue also!
            # del GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]

    def del_data_dict(payloadEnum):
        del GlobalVar.g_data_dict[payloadEnum]

    def Update_Symbol_List_Json(clientMsgId = None):
        symbol_data = GlobalVar.g_data_dict[ProtoOASymbolsListRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOASymbolsListRes().payloadType]

        filename = "symbolList_" + GlobalVar.ACCOUNT_TYPE + ".txt"
        with open(filename, "w") as file:
            file.write(str(symbol_data))
        result = utility.convert_txt_to_json(filename, GlobalVar.ACCOUNT_TYPE)

        if result == SymbolJsonUpdate.HAS_UPDATE:
            # Update the global data that hold the symbol detail
            GlobalVar.g_Symbol_Data_ID_As_Key = None
            GlobalVar.g_Symbol_Data_Name_As_Key = None
            utility.read_symbol_file(GlobalVar.ACCOUNT_TYPE)

    def print_all_accoutns(clientMsgId = None):
        res = GlobalVar.g_data_dict[ProtoOAGetAccountListByAccessTokenRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOAGetAccountListByAccessTokenRes().payloadType]

        accounts = res.ctidTraderAccount
        GlobalVar.g_auth_acc.clear()
        for index, acc in enumerate(accounts):
            traderLogin = acc.traderLogin
            ctidTraderAccountId = acc.ctidTraderAccountId
            nickname = os.getenv(f'A_{ctidTraderAccountId}')
            GlobalVar.g_auth_acc.append({"no": index, "traderLogin": traderLogin, "ctidTraderAccountId": ctidTraderAccountId, "nickname": nickname})
        print("\n")
        for acc in GlobalVar.g_auth_acc:
            print(acc)

    def authenticate_all_accounts(clientMsgId = None):
        res = GlobalVar.g_data_dict[ProtoOAGetAccountListByAccessTokenRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOAGetAccountListByAccessTokenRes().payloadType]

        accounts = res.ctidTraderAccount
        GlobalVar.g_auth_acc.clear()
        for index, acc in enumerate(accounts):
            traderLogin = acc.traderLogin
            ctidTraderAccountId = acc.ctidTraderAccountId
            nickname = os.getenv(f'A_{ctidTraderAccountId}')
            GlobalVar.g_auth_acc.append({"no": index, "traderLogin": traderLogin, "ctidTraderAccountId": ctidTraderAccountId, "nickname": nickname})
            setAccount(index)

    def send_Get_Symbol_Detail(symbolIdList, clientMsgId = None):
        # Convert non-list input into a list
        if not isinstance(symbolIdList, list):
            symbolIdList = [symbolIdList]

        request = ProtoOASymbolByIdReq()
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        for symbolId in symbolIdList:
            request.symbolId.append(int(symbolId))
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def Update_Symbol_Detail(clientMsgId = None):
        # Update the symbol to config.ini
        #!NOTE! Gonna make sure symbol ID gets updated
        res = GlobalVar.g_data_dict[ProtoOASymbolByIdRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOASymbolByIdRes().payloadType]
        symbolList = res.symbol
        for s in symbolList:
            symbol  = GlobalVar.g_Symbol_Data_ID_As_Key[s.symbolId]
            section = "SYMBOL_SECTION"
            key_min = f"MIN_LOT_VOLUME_{symbol}"
            key_max = f"MAX_LOT_VOLUME_{symbol}"
            value_min_lot = s.minVolume
            value_max_lot = s.maxVolume
            key_pip_position   = f"PIP_POSITION_{symbol}"
            value_pip_position = s.pipPosition

            utility.write_config_file(section, key_min, value_min_lot)
            utility.write_config_file(section, key_max, value_max_lot)
            utility.write_config_file(section, key_pip_position, value_pip_position)

        GlobalVar.g_Config_Data = None
        utility.read_config_file()

    def handle_symbol_update(clientMsgId = None):
        """
        Query the server, get symbol ID, check if got ID changes
        Queryt he server, get favourtie symbol details, check if got some changes
        """
        GlobalVar.g_task_queue.append([send_Get_Symbol_List, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOASymbolsListRes().payloadType, "Call by send_Get_Symbol_List"])
        GlobalVar.g_task_queue.append([Update_Symbol_List_Json, None, None, None])
        GlobalVar.g_task_queue.append([updateSymbolDetailAccordingToFavourite, None, None, None])

    def set_START_USER_COMMAND_True(clientMsgId = None):
        GlobalVar.START_USER_COMMAND = True

    def add_record_into_record_file():
        """
        Get list of pending orders, add those that are not 100 lotsize into record.ini file
        I will use copy dictionary method
        If got new lotsize update that is not 100lotsize, it will be updated into dictionary and then write into the file
        It will only skip adding into the dictionary if the pending order lotsize is 100 lotsize
        """
        res = GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]

        section = 'HEADER'
        temp = copy.deepcopy(GlobalVar.g_Record_Data)
        temp[section].clear()
        orderList = res.order
        if len(orderList) == 0:
            print(f"No new records to be saved into record.ini. No pending orders.")
            return

        for order in orderList:
            symbol_name = GlobalVar.g_Symbol_Data_ID_As_Key[order.tradeData.symbolId]
            # Dont record those that is max lotsize, it is my strategy that, market close, set maximum lotsize
            if order.tradeData.volume == int(GlobalVar.g_Config_Data[f"MAX_LOT_VOLUME_{symbol_name}"]):
                continue
            temp[section][str(order.orderId)] = str(order.tradeData.volume)

        # No records to be updated
        if not temp.items(section):
            print(f"No new records to be saved into record.ini. All left is 100 lotsize pending orders.")
            return

        with open(GlobalVar.RECORD_FILENAME, 'w') as configfile:
            temp.write(configfile)

        # Refresh the GlobalVar.g_Record_Data
        utility.read_record_file()

    def check_token_expiry(clientMsgId = None):
        # Target date as string
        target_str = os.getenv("LAST_UPDATED")

        # Convert to datetime object
        target_date = datetime.strptime(target_str, "%d-%b-%y").date()

        # Get today's date
        today = datetime.today().date()

        # Calculate the difference
        days_remaining = (target_date - today).days

        print(f"Token expiry days remaining: {days_remaining}")
        # Check if it's 5 days away
        if days_remaining <= 5:
            print(f"!!!!!!!!!!!!!!!!!!!")
            print(f"!!!!!!!!!!!!!!!!!!!")
            print(f"!!!!!!!!!!!!!!!!!!!")
            print(f"Please renew token!")
            print(f"!!!!!!!!!!!!!!!!!!!")
            print(f"!!!!!!!!!!!!!!!!!!!")
            print(f"!!!!!!!!!!!!!!!!!!!")

    def handle_record_order(clientMsgId = None):
        """
        Record all the pending order in your account
        into record.ini
        This is so that when market open, it will set
        back to lotsize according to what you set, instead of
        forcing all order same lotsize
        """
        GlobalVar.g_task_queue.append([utility.create_record_file, None, None, None])
        # You need to read record.ini file so that GlobalVar.g_Record_Data has `config` datatype
        GlobalVar.g_task_queue.append([utility.read_record_file, None, None, None])
        GlobalVar.g_task_queue.append([send_Get_List_Of_Running_And_Pending_Orders, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOAReconcileRes().payloadType, "Call by send_Get_List_Of_Running_And_Pending_Orders"])
        GlobalVar.g_task_queue.append([add_record_into_record_file, None, None, None])

    def setLotSize(lotsize, clientMsgId = None):
        """
        This is for pending orders
        """
        lotsize = round(float(lotsize), 2)
        param = [lotsize]
        GlobalVar.g_task_queue.append([send_Get_List_Of_Running_And_Pending_Orders, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOAReconcileRes().payloadType, "Call by send_Get_List_Of_Running_And_Pending_Orders"])
        GlobalVar.g_task_queue.append([update_lotsize_for_pending_order, param, None, None])

    def saveLotSize(clientMsgId = None):
        """
        Save lotsize & put them into record.ini
        Then set lotsize to 100 lot

        This is for so that you can run `load` to load back all respective lotsize
        """
        GlobalVar.g_task_queue.append([send_Get_List_Of_Running_And_Pending_Orders, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOAReconcileRes().payloadType, "Call by send_Get_List_Of_Running_And_Pending_Orders"])
        GlobalVar.g_task_queue.append([add_record_into_record_file, None, None, None])
        param = []
        param.append("100")
        GlobalVar.g_task_queue.append([setLotSize, param, None, None])

    def loadLotSize(clientMsgId = None):
        """
        Load lotisze from record.ini
        """
        GlobalVar.g_task_queue.append([utility.read_record_file, None, None, None])
        GlobalVar.g_task_queue.append([send_Get_List_Of_Running_And_Pending_Orders, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOAReconcileRes().payloadType, "Call by send_Get_List_Of_Running_And_Pending_Orders"])
        param = [-1]
        GlobalVar.g_task_queue.append([update_lotsize_for_pending_order, param, None, None])
        GlobalVar.g_task_queue.append([utility.write_config_file, None, None, None])

    def clear_record_file(clientMsgId = None):
        """
        Before you clear, you need to ensure current exsting pending orders
        doesnt have 100 lotsize, else, they cannot be restored to their respective
        lot size.
        """
        GlobalVar.g_task_queue.append([send_Get_List_Of_Running_And_Pending_Orders, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOAReconcileRes().payloadType, "Call by send_Get_List_Of_Running_And_Pending_Orders"])
        GlobalVar.g_task_queue.append([check_clear_record_file, None, None, None])

    def check_clear_record_file(clientMsgId = None):
        """
        Check if lotsize 100 pending order exists
        If exists, then cannot clear the record.ini file
        If clear, means history lost forever leh
        """
        res = GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]

        GlobalVar.CLEAR_RECORD_INI_FILE = True
        orderList = res.order
        if len(orderList) != 0:
            for order in orderList:
                symbol_name = GlobalVar.g_Symbol_Data_ID_As_Key[order.tradeData.symbolId]
                if order.tradeData.volume == int(GlobalVar.g_Config_Data[f"MAX_LOT_VOLUME_{symbol_name}"]):
                    GlobalVar.CLEAR_RECORD_INI_FILE = False
                    break

        if GlobalVar.CLEAR_RECORD_INI_FILE:
            utility.create_record_file(True) # Clear the record.ini
            utility.read_record_file() # Refresh GlobalVar.g_Record_Data data
            GlobalVar.CLEAR_RECORD_INI_FILE = False
            print(f"Record.ini cleared")
        else:
            print(f"Record.ini NOT cleared, pending order lotsize 100 exists!")

    def print_g_data_dict(clientMsgId = None):
        """
        """
        print("g_data_dict:")
        for key, value in GlobalVar.g_data_dict.items():
            print(f"{key}: {value}")

    def print_g_time_checks_record(clientMsgId = None):
        """
        """
        print("g_time_checks_record:")
        for key, value in GlobalVar.g_time_checks_record.items():
            print(f"{key}: {value}")

    def print_g_favourite_symbol(clientMsgId = None):
        """
        """
        print("g_favourite_symbol:")
        for key, value in GlobalVar.g_favourite_symbol.items():
            print(f"{key} : {value}")

    def print_g_record_data(clientMsgId = None):
        """
        """
        utility.read_record_file()
        print("g_Record_data:")
        for section in GlobalVar.g_Record_Data.sections():
            print(f"[{section}]")
            for key, value in GlobalVar.g_Record_Data[section].items():
                print(f"{key} = {value}")
            print() # Blank line between sections

    def refresh_RAM(clientMsgId = None):
        """
        Reload everything into the RAM,
        dont care got new update or not
        """
        utility.read_config_file(True)
        utility.read_symbol_file(GlobalVar.ACCOUNT_TYPE)
        load_dotenv(override=True)
        GlobalVar.ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')

    def handle_renew_access_token(clientMsgId = None):
        GlobalVar.g_task_queue.append([send_renew_access_token, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOARefreshTokenRes().payloadType, "Call by send_renew_access_token"])
        GlobalVar.g_task_queue.append([handle_refresh_token, None, None, None])

    def send_renew_access_token(clientMsgId = None):
        request = ProtoOARefreshTokenReq()
        request.refreshToken = os.getenv("REFRESH_TOKEN")
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def setHeartbeat(value, clientMsgId = None):
        value = int(value)
        GlobalVar.g_print_heartbeat = int(value)

    def showHelp(clientMsgId = None):
        print()
        print("Note: Some command are not shown, those shall not be executed by you")
        print("help: showHelp,")
        print("ver: sendProtoOAVersionReq, # get API version")
        print("hb: setHeartbeat, # Set print heartbeat true or false. Call it like this `hb 1`")
        print("")
        print("qq: User_Disconnect,")
        print("")
        print("acc: handle_print_all_accounts, # Get all account details & print")
        print("set: setAccount, # Authenticate an account, call `set index`")
        print("cur: getCurrentAccount, # Get current set acc")
        print("auth: sendProtoOAGetAccountListByAccessTokenReq, # Authenticate all accounts")
        print("renew: handle_renew_access_token, # Renew access & refresh token")
        print("")
        print("gsl: getSymbolIDs, # gsl = get symbol list. List the symbol and their ID, call `gsl 0`, `gsl 1`")
        print("gsd: getSymbolDetail, # gsd = get symbol detail, call `gsd symbolId`")
        print("us: handle_symbol_update, # us = update symbol list json file")
        print("usd: updateSymbolDetail, # usd = update symbol detail to config.ini, call `usd symbolId`")
        print("usdd: updateSymbolDetailAccordingToFavourite, # Same as above, just auto update with your favourite list. Just call `usdd`")
        print("")
        print("lt: setLotSize, # lt = lot. Set pending order lotsize. Call like this `lt 100`, `lt 0.01`")
        print("save: saveLotSize, # save lotsize")
        print("load: loadLotSize, # load saved lotsize")
        print("clearrecord: clear_record_file, # Clear record.ini file")
        print("")
        print("p: print_g_data_dict, # Print g_data_dict")
        print("pp: print_g_time_checks_record, # Print g_time_checks_record")
        print("ppp: print_g_record_data, # Print g_Record_Data")
        print("pppp: print_g_favourite_symbol, # Print g_favourite_symbol")
        print("r: refresh_RAM, # Refresh global variable with latest value")
        print("")
        print("test: test,")

    def test(clientMsgId = None):
        pass

    commands = {
        "help": showHelp,
        "ver": sendProtoOAVersionReq, # get API version
        "hb": setHeartbeat, # Set print heartbeat true or false. Call it like this `hb 1`

        "qq": User_Disconnect,

        "acc": handle_print_all_accounts, # Get all account details & print
        "set": setAccount, # Authenticate an account, call `set index`
        "cur": getCurrentAccount, # Get current set acc
        "auth": sendProtoOAGetAccountListByAccessTokenReq, # Authenticate all accounts
        "renew": handle_renew_access_token, # Renew access & refresh token

        "gsl": getSymbolIDs, # gsl = get symbol list. List the symbol and their ID, call `gsl 0`, `gsl 1`
        "gsd": getSymbolDetail, # gsd = get symbol detail, call `gsd symbolId`
        "us": handle_symbol_update, # us = update symbol list json file
        "usd": updateSymbolDetail, # usd = update symbol detail to config.ini, call `usd symbolId`
        "usdd": updateSymbolDetailAccordingToFavourite, # Same as above, just auto update with your favourite list. Just call `usdd`

        "lt": setLotSize, # lt = lot. Set pending order lotsize. Call like this `lt 100`, `lt 0.01`
        "save": saveLotSize, # save lotsize
        "load": loadLotSize, # load saved lotsize
        "clearrecord": clear_record_file, # Clear record.ini file

        "p": print_g_data_dict, # Print g_data_dict
        "pp": print_g_time_checks_record, # Print g_time_checks_record
        "ppp": print_g_record_data, # Print g_Record_Data
        "pppp": print_g_favourite_symbol, # Print g_favourite_symbol
        "r": refresh_RAM, # Refresh global variable with latest value

        "test": test,
    }

    def executeUserCommand():
        while GlobalVar.START_USER_COMMAND == False:
            continue

        try:
            while True:
                while len(GlobalVar.g_task_queue) == 0:
                    current_time = time.time()
                    dt = datetime.fromtimestamp(current_time, GlobalVar.g_mytimezone)
                    formatted_time = dt.strftime("%H%M")
                    print("\n=====================================\n")
                    userInput = input(f"[{formatted_time}] Cmd (Rmb Termux eats 1 char): ")
                    print(f"Cmd typed: {userInput}")

                    # You have to find out which message receives will be receiving from
                    # server and does not require user to issue command
                    # eg: Heartbeat
                    if GlobalVar.NEW_PRINT_HAS_HAPPENED:
                        print(f"\n\nA new print to console message has happened. Retype your command")
                        GlobalVar.NEW_PRINT_HAS_HAPPENED = False
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

                    GlobalVar.g_task_queue.append([commands[command], parameters, None, None])

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
        while True:
            while len(GlobalVar.g_task_queue) != 0:

                # Usually [2] is waiting for server to reply
                # Wait until server finish replying
                # There's a reason why i dont use current_task = GlobalVar.g_task_queue[0]
                # and then check current_task instead
                # Because once received server reply, i will modify the GlobalVar.g_task_queue
                # If i use current_task, forever stuck in loop
                if GlobalVar.g_task_queue[0][2] is not None:
                    while GlobalVar.g_task_queue[0][2] is not None:
                        continue
                    # One done replying, this task is done, next
                    GlobalVar.g_task_queue.pop(0)
                    continue

                current_task = GlobalVar.g_task_queue[0]

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
                GlobalVar.g_task_queue.pop(0)

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
