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
from datetime import datetime, time as time2
import utility
import fileinput
import threading
import time
import running_position
from enum import Enum
from globalpy import GlobalVar

utility.read_config_file() # Read config.ini
utility.read_symbol_file(GlobalVar.ACCOUNT_TYPE) # Read symbolList_demo/live.json

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
        handle_Authenticate_API()
        if GlobalVar.CURRENT_CTIDTRADERACCOUNTID is not None:
            handle_Authenticate_Account()
        handle_symbol_update()
        GlobalVar.START_USER_COMMAND = True
        
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

            print(f"[{formatted_time}] ProtoOAExecutionEvent")

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

        elif message.payloadType == ProtoHeartbeatEvent().payloadType:
            if GlobalVar.g_print_heartbeat:
                GlobalVar.NEW_PRINT_HAS_HAPPENED = True
                print(f"[{formatted_time}] Heartbeat Received.")
            handle_time_checks()

        elif message.payloadType == ProtoOAPayloadType.Value('PROTO_OA_SYMBOL_CHANGED_EVENT'):
            """
            SYMBOL CHANGED! Update SYMBOL JSON
            """
            GlobalVar.NEW_PRINT_HAS_HAPPENED = True
            print(f"[{formatted_time}] Symbol change! Update!")
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
            print(f"[{formatted_time}] Message received: payloadType = {message.payloadType} ({payloadName})")
            print("\n", Protobuf.extract(message))
            GlobalVar.NEW_PRINT_HAS_HAPPENED = True

        if len(GlobalVar.g_task_queue) != 0:
            if GlobalVar.g_task_queue[0][2] is not None:
                if GlobalVar.g_task_queue[0][2] == message.payloadType:
                    GlobalVar.g_task_queue[0][2] = None

    def setAccount(index):
        """
        index is GlobalVar.g_auth_acc index
        call `acc` and you know what 7 im saying
        """
        if len(GlobalVar.g_auth_acc) == 0:
            print("Call `acc` first, to get account list")
            return

        # if GlobalVar.CURRENT_CTIDTRADERACCOUNTID is not None:
        #     sendProtoOAAccountLogoutReq()
        GlobalVar.CURRENT_CTIDTRADERACCOUNTID = GlobalVar.g_auth_acc[int(index)]["ctidTraderAccountId"]
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

    def handle_print_all_accounts():
        GlobalVar.g_task_queue.append([getAllAccounts, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOAGetAccountListByAccessTokenRes().payloadType, "Call by getAllAccounts"])
        GlobalVar.g_task_queue.append([print_all_accoutns, None, None, None])

    def getCurrentAccount(clientMsgId = None):
        """
        """
        print(f"ctidTraderAccountId:{GlobalVar.CURRENT_CTIDTRADERACCOUNTID}")

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

    def User_Disconnect(clientMsgId=None): # disconnect the client
        client.stopService()
        # After disconnect
        # Your main thread script still running.
        # Terminate your main thread script
        reactor.callLater(3, callable=terminate_script)

    def terminate_script():
        os._exit(0)

    def send_close_all_running_positions(positionEntity, clientMsgId=None):
        positionId = positionEntity.positionId
        symbol_name = GlobalVar.g_Symbol_Data_ID_As_Key[positionEntity.tradeData.symbolId]
        volume = positionEntity.tradeData.volume
        
        print(f"PositionId:{position.positionId} Symbol:{symbol_name} Volume:{volume} closing position.")

        request = ProtoOAClosePositionReq()
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        request.positionId = int(positionId)
        request.volume = int(volume)
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def handle_close_all_running_positions():
        res = GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]
        positionList = res.position
        
        for position in positionList:
            param = [position]
            GlobalVar.g_task_queue.append([send_close_all_running_positions, param, None, None])

    def getRunningPositions(clientMsgId=None):
        """
        This is for pending orders
        """
        current_time = time.time()
        dt = datetime.fromtimestamp(current_time, GlobalVar.g_mytimezone)
        formatted_time = dt.strftime("%H%M")
        print(f"[{formatted_time}] getRunningPositions")

        request = ProtoOAReconcileReq()
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)
        
    def handle_refresh_token():
        res = GlobalVar.g_data_dict[ProtoOARefreshTokenRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOARefreshTokenRes().payloadType]
        
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

    def Set_RunningPosition_StopLoss_To_Opposite():
        res = GlobalVar.g_data_dict[ProtoOAExecutionEvent().payloadType]
        del GlobalVar.g_data_dict[ProtoOAExecutionEvent().payloadType]
        
        positionList = res.position

        for position in positionList:
            symbol = GlobalVar.g_Symbol_Data_ID_As_Key[position.tradeData.symbolId]
            if position.stopLoss == 0:
                print(f"PositionId:{position.positionId} Symbol:{symbol} stopLoss is 0. Abort.")
                continue

            # Check if SL trigger is opposite or not, if is not, set it to opposite
            if position.stopLossTriggerMethod != ProtoOAOrderTriggerMethod.Value('OPPOSITE'):
                print(f"PositionId:{position.positionId} Symbol:{symbol} SL trigger is not OPPOSITE. Set to OPPOISTE now.")
                # Note: After this command
                # If you get description: "Protection can\'t be negative"
                # Dont worry, this means you didnt set TP
                # Usually this happens when I trying to test demo

                param = [position.positionId, position.stopLoss, position.takeProfit, "OPPOSITE"]
                GlobalVar.g_task_queue.append([send_Set_StopLoss_To_Opposite, param, None, None])
            else:
                print(f"PositionId:{position.positionId} Symbol:{symbol} SL trigger is OPPOSITE.")

    def send_Get_List_Of_Running_And_Pending_Orders():
        request = ProtoOAReconcileReq()
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)


    def stopRunningPosition(positionId, clientMsgId=None):
        """
        Remove position from g_position since it hit SL or TP
        What about g_subscribe?
        Once this object is set to False
        It will be handled in the object destroy() function
        """
        current_time = time.time()
        dt = datetime.fromtimestamp(current_time, GlobalVar.g_mytimezone)
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

    def handle_Authenticate_Account():
        GlobalVar.g_task_queue.append([send_Auth_Account, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOAAccountAuthRes().payloadType, "Call by send_Auth_Account"])
        

    def handle_Authenticate_API():
        GlobalVar.g_task_queue.append([send_Authenticate_API, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOAApplicationAuthRes().payloadType, "Call by send_Authenticate_API"])
        

    def send_Get_Symbol_List(clientMsgId=None):
        request = ProtoOASymbolsListReq()
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def getSymbolDetail(symbolId, clientMsgId=None):
        """
        """
        param = [symbolId]
        GlobalVar.g_task_queue.append([send_Get_Symbol_Detail, param, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOASymbolByIdRes().payloadType, "Call by send_Get_Symbol_Detail"])
        GlobalVar.g_task_queue.append([print_symbol_detail, None, None, None])

    def print_symbol_detail():
        res = GlobalVar.g_data_dict[ProtoOASymbolByIdRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOASymbolByIdRes().payloadType]
        print(res)

    def update_lotsize_for_pending_order_no_delete_dict(lotsize):
        """
        """
        res = GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]
        # No delete for this function
        # del GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]
        
        orderList = res.order
        if len(orderList) == 0:
            print(f"No pending orders")
            return
        
        for order in orderList:
            # Get MIN_LOT_XAUUSD from config.ini
            symbol_name         = GlobalVar.g_Symbol_Data_ID_As_Key[order.tradeData.symbolId]
            MIN_LOT_VALUE       = int(GlobalVar.g_Config_Data[f"MIN_LOT_VOLUME_{symbol_name}"])
            MAX_LOT_VALUE       = int(GlobalVar.g_Config_Data[f"MAX_LOT_VOLUME_{symbol_name}"])
            volume_to_pip_converter = 0.01 / float(MIN_LOT_VALUE)
            
            # Check whether is same lotsize or not
            # If lotsize is 100, just use the maximum volume from config.ini
            if lotsize == 100:
                if order.tradeData.volume == MAX_LOT_VALUE:
                    continue
                order.tradeData.volume = MAX_LOT_VALUE
                
            else:
                if order.tradeData.volume * volume_to_pip_converter == lotsize:
                    continue
                order.tradeData.volume = int(lotsize / volume_to_pip_converter)
            
            print(f"Pending order change lotsize to {lotsize} lot")
            param = [order]
            GlobalVar.g_task_queue.append([send_Amend_Pending_Order_Lotsize, param, None, None])
            
    def update_lotsize_for_pending_order(lotsize):
        """
        """
        res = GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]
        
        orderList = res.order
        if len(orderList) == 0:
            print(f"No pending orders")
            return
        
        for order in orderList:
            # Get MIN_LOT_XAUUSD from config.ini
            symbol_name         = GlobalVar.g_Symbol_Data_ID_As_Key[order.tradeData.symbolId]
            MIN_LOT_VALUE       = int(GlobalVar.g_Config_Data[f"MIN_LOT_VOLUME_{symbol_name}"])
            MAX_LOT_VALUE       = int(GlobalVar.g_Config_Data[f"MAX_LOT_VOLUME_{symbol_name}"])
            volume_to_pip_converter = 0.01 / float(MIN_LOT_VALUE)
            
            # Check whether is same lotsize or not
            # If lotsize is 100, just use the maximum volume from config.ini
            if lotsize == 100:
                if order.tradeData.volume == MAX_LOT_VALUE:
                    continue
                order.tradeData.volume = MAX_LOT_VALUE
                
            else:
                if order.tradeData.volume * volume_to_pip_converter == lotsize:
                    continue
                order.tradeData.volume = int(lotsize / volume_to_pip_converter)
            
            print(f"Pending order change lotsize to {lotsize} lot")
            param = [order]
            GlobalVar.g_task_queue.append([send_Amend_Pending_Order_Lotsize, param, None, None])


    def updateSymbolDetail(symbolId, clientMsgId=None):
        """
        Update symbol to config.ini, but only accept single symbolID
        """
        param = [symbolId]
        GlobalVar.g_task_queue.append([send_Get_Symbol_Detail, param, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOASymbolByIdRes().payloadType, None])
        GlobalVar.g_task_queue.append([Update_Symbol_Detail, None, None, None])

    def getSymbolIDs(favourite = True):
        """
        favourite = True
        Print my favourite symbols only
        Else, all
        """
        for id, symbol in GlobalVar.g_Symbol_Data_ID_As_Key.items():
            if favourite:
                if symbol in GlobalVar.g_favourite_symbol:
                    print(f"ID:{id}, Symbol:{symbol}")
            else:
                    print(f"ID:{id}, Symbol:{symbol}")

    def send_Set_StopLoss_To_Opposite(positionId, entryPrice, takeProfit, SLTriggerMethod = 'TRADE', clientMsgId=None):
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
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        request.positionId = int(positionId)
        request.stopLoss = round(float(entryPrice), 2)
        request.takeProfit = round(float(takeProfit), 2)
        request.stopLossTriggerMethod = ProtoOAOrderTriggerMethod.Value(SLTriggerMethod)
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)
        
    def send_Amend_Pending_Order_Lotsize(orderEntity, clientMsgId=None):
        """
        !Note!
        Please take note whether it will change ur SL trigger method or not
        I have verified that, it wont.
        Example: My SL is triggered by opposite bid/ask price, i verfied that
        after running this function, the trigger method still same, that is
        opposite.
        """
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

    def send_Authenticate_API():
        request = ProtoOAApplicationAuthReq()
        request.clientId = GlobalVar.APP_CLIENT_ID
        request.clientSecret = GlobalVar.APP_CLIENT_SECRET
        deferred = client.send(request)
        deferred.addErrback(onError)

    def handle_time_checks():
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

        # Market open, set lotsize to my lotsize
        if current_weekday == "Monday":
            # 830am set lotsize back to normal
            time_checks = time2(8, 30)
            if current_time > time_checks:
                if current_weekday not in GlobalVar.g_time_checks_record:
                    GlobalVar.NEW_PRINT_HAS_HAPPENED = True
                    print(f"Today is {current_weekday} {formatted_time}. Market opening. Set all pending order lotsize to {GlobalVar.g_Config_Data['LOTSIZE']}.")
                    lotsize = GlobalVar.g_Config_Data["LOTSIZE"]
                    GlobalVar.g_time_checks_record = {current_weekday : lotsize}

        # Market closing, weekend, close all running positions too
        elif current_weekday == "Saturday":
            # 2am set lotsize to maximum lotsize & close all running position
            time_checks = time2(2, 0)
            if current_time > time_checks:
                if current_weekday not in GlobalVar.g_time_checks_record:
                    GlobalVar.NEW_PRINT_HAS_HAPPENED = True
                    print(f"Today is {current_weekday} {formatted_time}. Market closing. Set all pending order lotsize to max. Also close all running order.")
                    lotsize = 100
                    GlobalVar.g_time_checks_record = {current_weekday : lotsize}

        else:
            if current_weekday not in GlobalVar.g_time_checks_record:
                GlobalVar.NEW_PRINT_HAS_HAPPENED = True
                print(f"Today is {current_weekday} {formatted_time}.")
                GlobalVar.g_time_checks_record = {current_weekday : lotsize}

        # After a lot of checking above, here handles the aftermath
        if lotsize != 0:
            GlobalVar.NEW_PRINT_HAS_HAPPENED = True
            param = [lotsize]
            GlobalVar.g_task_queue.append([send_Get_List_Of_Running_And_Pending_Orders, None, None, None])
            GlobalVar.g_task_queue.append([None, None, ProtoOAReconcileRes().payloadType, "Call by send_Get_List_Of_Running_And_Pending_Orders"])
            # I will delete the dict myself, manually, at the end of this function
            GlobalVar.g_task_queue.append([update_lotsize_for_pending_order_no_delete_dict, param, None, None])

            # Close all running position
            if current_weekday == "Saturday":
                GlobalVar.g_task_queue.append([send_close_all_running_positions, None, None, None])

            del GlobalVar.g_data_dict[ProtoOAReconcileRes().payloadType]

    def Update_Symbol_List_Json():
        symbol_data = GlobalVar.g_data_dict[ProtoOASymbolsListRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOASymbolsListRes().payloadType]
        
        filename = "symbolList_" + GlobalVar.ACCOUNT_TYPE + ".txt"
        with open(filename, "w") as file:
            file.write(str(symbol_data))
        result = utility.convert_txt_to_json(filename, GlobalVar.ACCOUNT_TYPE)

        if result == utility.SymbolJsonUpdate.HAS_UPDATE:
            # Update the global data that hold the symbol detail
            GlobalVar.g_Symbol_Data_ID_As_Key = None
            GlobalVar.g_Symbol_Data_Name_As_Key = None
            utility.read_symbol_file(GlobalVar.ACCOUNT_TYPE)

    def print_all_accoutns():
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

    def authenticate_all_accounts():
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

    def send_Get_Symbol_Detail(symbolIdList, clientMsgId=None):
        request = ProtoOASymbolByIdReq()
        request.ctidTraderAccountId = GlobalVar.CURRENT_CTIDTRADERACCOUNTID
        for symbolId in symbolIdList:
            request.symbolId.append(int(symbolId))
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)
        
    def Update_Symbol_Detail():
        # Update the symbol to config.ini
        #!NOTE! Gonna make sure symbol ID gets updated
        res = GlobalVar.g_data_dict[ProtoOASymbolByIdRes().payloadType]
        del GlobalVar.g_data_dict[ProtoOASymbolByIdRes().payloadType]
        symbolList = res.symbol
        for s in symbolList:
            symbol = GlobalVar.g_Symbol_Data_ID_As_Key[s.symbolId]
            section = "SYMBOL_SECTION"
            key_min = f"MIN_LOT_VOLUME_{symbol}"
            key_max = f"MAX_LOT_VOLUME_{symbol}"
            min_lot = s.minVolume
            max_lot = s.maxVolume

            utility.write_config_file(section, key_min, min_lot)
            utility.write_config_file(section, key_max, max_lot)

        GlobalVar.g_Config_Data = None
        utility.read_config_file()

    def handle_symbol_update():
        GlobalVar.g_task_queue.append([send_Get_Symbol_List, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOASymbolsListRes().payloadType, "Call by send_Get_Symbol_List"])
        GlobalVar.g_task_queue.append([Update_Symbol_List_Json, None, None, None])
        symbolIdList = []
        for symbolName in GlobalVar.g_favourite_symbol:
            symbolIdList.append(GlobalVar.g_Symbol_Data_Name_As_Key[symbolName])
        # Because i use function(*parameter) approach
        # It will unpack the list
        param = []
        param.append(symbolIdList)
        GlobalVar.g_task_queue.append([send_Get_Symbol_Detail, param, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOASymbolByIdRes().payloadType, None])
        GlobalVar.g_task_queue.append([Update_Symbol_Detail, None, None, None])

    def setLotSize(lotsize, clientMsgId=None):
        """
        This is for pending orders
        """
        param = [lotsize]
        GlobalVar.g_task_queue.append([send_Get_List_Of_Running_And_Pending_Orders, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOAReconcileRes().payloadType, "Call by send_Get_List_Of_Running_And_Pending_Orders"])
        # I will delete the dict myself, manually, at the end of this function
        GlobalVar.g_task_queue.append([update_lotsize_for_pending_order, param, None, None])

    def printPendingList():
        """
        """
        print("\n")
        print("Pending list now :")
        for o in g_pending.values():
            volume_to_pip_converter = 0.01 / float(GlobalVar.g_Config_Data[f"MIN_LOT_VOLUME_{o['symbol']}"])
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
        Reload everything into the RAM, 
        dont care got new update or not
        """
        utility.read_config_file(True)
        utility.read_symbol_file(GlobalVar.ACCOUNT_TYPE)
        load_dotenv(override=True)
        GlobalVar.ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')

    def handle_renew_access_token():
        GlobalVar.g_task_queue.append([send_renew_access_token, None, None, None])
        GlobalVar.g_task_queue.append([None, None, ProtoOARefreshTokenRes().payloadType, "Call by send_renew_access_token"])
        GlobalVar.g_task_queue.append([handle_refresh_token, None, None, None])

    def send_renew_access_token(clientMsgId=None):
        request = ProtoOARefreshTokenReq()
        request.refreshToken = os.getenv("REFRESH_TOKEN")
        deferred = client.send(request, clientMsgId=clientMsgId)
        deferred.addErrback(onError)

    def setHeartbeat(value, clientMsgId=None):
        GlobalVar.g_print_heartbeat = int(value)

    def showHelp():
        print()
        print("Note: Some command are not shown, those shall not be executed by you")

    def test(clientMsgId=None):
        pass
        # symbolIdList = []
        # for s in GlobalVar.g_favourite_symbol:
        #     symbolId = GlobalVar.g_Symbol_Data_Name_As_Key[s]
        #     symbolIdList.append(symbolId)

        # updateSymbolDetailList(symbolIdList)

    commands = {
        "help": showHelp,
        "set": setAccount, # Set global variable account ID
        "ver": sendProtoOAVersionReq, # Show version
        "auth": sendProtoOAGetAccountListByAccessTokenReq, # Authenticate all accounts
        "acc": handle_print_all_accounts, # Get all account details
        "cur": getCurrentAccount, # Get current acc
        "renew": handle_renew_access_token, # Renew access & refresh token
        "hb": setHeartbeat, # Set print heartbeat true or false. Call it like this `hb 1`
        "qq": User_Disconnect,
        "ap": send_Set_StopLoss_To_Opposite, # Amend Running Position, call `ap positionId stopLoss takeProfit 'TRADE'`
        "gsl": getSymbolIDs, # gsl = get symbol list. List the symbol and their ID
        "gsd": getSymbolDetail, # gsd = get symbol detail, call `sd symbolId`
        "us": handle_symbol_update, # us = update symbol list json file
        "usd": updateSymbolDetail, # usd = update symbol detail to config.ini, call `us symbolId`
        "lt": setLotSize, # lt = lot. Set lot size. Call like this `lt 100`, `lt 0.01`
        "r": refresh_RAM, # Refresh global variable with latest value
        "test": test,

        "ppp": printPendingList,
        "pp": printRunningList,
        "p": printSubscriptionList,

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
                        print(f"A new print to console message has happened. Retype your command")
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

