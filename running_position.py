import utility
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATradeSide
import queue
import threading

# It needs to be defined here
# If i defined it in main.py,
# calling g_command_queue.put() has no effect i dk why
g_command_queue = queue.Queue()
# list of running positions
# The dict key is the Running PositionId, the data afterwards has RunningPosition object
g_positions = {}
# list of symbols bid/ask price
# The dict key is the symbolID, the data afterwards has Symbol name, bid, ask, NumOfUsers
g_subscribe = {}

# Lock for multithread
g_lock = threading.Lock()

class RunningPosition:
    def __init__(self, positionId, symbolId, symbol, volume, tradeSide, entryPrice, stopLoss, takeProfit):
        self.alive = True
        self.closeAll = False # Close all without TPP
        self.positionId = positionId
        self.symbolId = symbolId
        self.symbol = symbol
        self.volume = volume
        self.tradeSide = tradeSide
        self.entryPrice = entryPrice
        self.stopLoss = stopLoss
        self.takeProfit = takeProfit
        self.price_per_pip = float(utility.gConfigData[f"PRICE_PER_PIP_{symbol}"])
        self.relative_per_pip = int(utility.gConfigData[f"RELATIVE_PER_PIP_{symbol}"])
        self.stopLossPip = abs(round(((self.entryPrice - self.stopLoss) / self.price_per_pip), 2))

        # Buy, TP/SL at bid price
        # Sell, TP/SL at ask price
        self.tp_sl_bid_or_ask = "bid"if self.tradeSide == ProtoOATradeSide.Value('BUY') else "ask"
        self.sl_direction_bias = 1 if self.tradeSide == ProtoOATradeSide.Value('BUY') else -1


        # Entry price plus + 1 pip, for set BE use
        self.entryPricePlusPips = entryPrice + (round(float(utility.gConfigData[f"PRICE_PER_PIP_{symbol}"]), 2) * self.sl_direction_bias)



        # The number, that, use (volume * this converter) will get lotsize
        # eg: XAUUSD, 0.4lot = 4000 volume. (4000 * this converter = 0.4) lotisze
        # The formula to get this converter = 0.01 lotisze / VOLUME_PER_0.01_LOT
        # Do not put round(num, 2) here, the output may have many decimal
        self.volume_to_pip_converter = 0.01 / float(utility.gConfigData[f"VOLUME_PER_LOT_{symbol}"])
        self.lotsize = round(volume * self.volume_to_pip_converter, 2)

        # If you have 0.02 lots, your total Stop Loss is x2
        # Lotsize is calculated in 0.01, hence, make it 1
        # !Note! Some asset lowest lotsize is 0.1
        self.totalStopLossPip = self.stopLossPip * (self.lotsize * 100)




        ############# SET TPP HERE ##############
        # This is set TPP after it run your total stop loss pip
        # Eg: 0.02 lot, SL 25 pips. Means total 50pips
        # I will TPP once 0.01 run 50pips
        # This tells me, after running how many pips i can take partial profit
        # With condition that, i will take all partial profit until 0.01 left
        # self.tpp_pips = self.totalStopLossPip / ((self.lotsize - 0.01) * 100)

        # ==========================================================
        # This is set TPP after RRR 1
        # To protect my mentality
        self.tpp_pips = self.stopLossPip



        # Lotsize to take partial profit
        # Just take volume & minus 1 VOLUME_PER_LOT
        # When closing position, you have to use volume
        self.tpp_lotsize_in_volume = volume - int(utility.gConfigData[f"VOLUME_PER_LOT_{symbol}"])

    def run(self):
        while self.symbolId not in g_subscribe:
            continue
        g_subscribe[self.symbolId]["NumOfUser"] += int(1)

        while True:
            # No need check lotsize 0.01 here, alreayd done before creating this class object
            # If SL-ed, then it shall be removed from the list & stop running this shit
            if self.alive == False:
                print(f"PositionId:{self.positionId} Symbol:{self.symbol} hit SL or Symbol Update.")
                break
            if self.closeAll:
                print(f"PositionId:{self.positionId} Symbol:{self.symbol} Close ALL!")
                g_command_queue.put(f"tpp {self.positionId} {self.volume}")
                break
            if self.symbolId not in g_subscribe:
                continue
            runningPip = 0
            # The documentation mention bid & ask are specified in 1/100000 unit
            runningPip = round(((g_subscribe[self.symbolId][self.tp_sl_bid_or_ask]/100000) - self.entryPrice) / self.price_per_pip, 2) * self.sl_direction_bias




            ##### FOR TESTING USE #####
            # import time
            # if self.symbol == "DAXEUR":
            #     time.sleep(2)
            # else:
            #     time.sleep(10)
            # """
            # There is 1 problem with this
            # If g_subscribe for some reason is not cleared due to race condition
            # This will straight away TRUE & TPP immediately after you enter trade
            # & set BE, then because your price still at entry then set BE fail
            # TODO
            # 1. We need find a way to guarantee g_subscribe gets cleared
            # 2. Maybe a command to refresh g_subscribe?
            
            # !NOTE!
            # There's 2nd problem
            # What if you accidentally ran the script in 2 different sessions?
            # Eg: 1 in your PC, 1 in your phone
            # Hmmm
            # """
            # print(f"PositionId:{self.positionId} Symbol:{self.symbol} TPP")
            # # Set TPP
            # g_command_queue.put(f"tpp {self.positionId} {self.tpp_lotsize_in_volume}")
            # # Set BE, set stopLoss = entryPrice
            # g_command_queue.put(f"ap {self.positionId} {self.entryPricePlusPips} {self.takeProfit}")
            # break




            # Take partial profit & set BE & set SL trigger is default (trade)
            if runningPip >= self.tpp_pips:
                """
                There is 1 problem with this
                If g_subscribe for some reason is not cleared due to race condition
                This will straight away TRUE & TPP immediately after you enter trade
                & set BE, then because your price still at entry then set BE fail
                TODO
                1. We need find a way to guarantee g_subscribe gets cleared
                2. Maybe a command to refresh g_subscribe?
                
                !NOTE!
                There's 2nd problem
                What if you accidentally ran the script in 2 different sessions?
                Eg: 1 in your PC, 1 in your phone
                Hmmm
                """
                print(f"PositionId:{self.positionId} Symbol:{self.symbol} TPP")
                # Set TPP
                g_command_queue.put(f"tpp {self.positionId} {self.tpp_lotsize_in_volume}")
                # Set BE, set stopLoss = entryPrice
                g_command_queue.put(f"ap {self.positionId} {self.entryPricePlusPips} {self.takeProfit}")
                
                # Because i suspect the script still randomly gets to trigger to close
                # my leftover 0.01 running position without g_position key not found error
                # Hence, I decide to try to assign it's volume to 0.01 lot, see if this
                # helps me catch the bug
                self.lotsize = 0.01
                break




        self.destroy()

    def destroy(self):
        global g_subscribe

        # No need remove from list in here, it already handled by stopRunningPosition()
        with g_lock:
            # Remove it from g_position
            # There's a case where i hit entry & hit SL immediately
            # Then this i think is lai bu ji save into g_positions
            # Then dictionary complain key not found
            # Then script stopped here, no unsubscribe, no delete g_subscribe etc.
            # Ok i fixed the code by moving running_position.g_positions[position.positionId] = ({"Object": obj})
            # to right after this object class creation
            # I think for now, can safely comment out this code first
            # while self.positionId not in g_positions[self.positionId]:
            #     continue
            del g_positions[self.positionId]

            # Check & remove subscription if no more user left
            g_subscribe[self.symbolId]["NumOfUser"] -= int(1)
            if g_subscribe[self.symbolId]["NumOfUser"] == int(0):
                # Unsubscribe
                # The handling of deleting g_subscribe will be handled in unsubscribe response
                g_command_queue.put(f"unsub {self.symbolId}")

        print(f"{self.positionId}:{self.symbol} is being destroyed.")
        del self
