import utility
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATradeSide
import queue

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

        # The number, that, use (volume * this converter) will get lotsize
        # eg: XAUUSD, 0.4lot = 4000 volume. (4000 * this converter = 0.4) lotisze
        # The formula to get this converter = 0.01 lotisze / VOLUME_PER_0.01_LOT
        self.volume_to_pip_converter = 0.01 / float(utility.gConfigData[f"VOLUME_PER_LOT_{symbol}"])
        self.lotsize = round(volume * self.volume_to_pip_converter, 2)

        # If you have 0.02 lots, your total Stop Loss is x2
        # Lotsize is calculated in 0.01, hence, make it 1
        # !Note! Some asset lowest lotsize is 0.1
        self.totalStopLossPip = self.stopLossPip * (self.lotsize * 100)

        # This tells me, after running how many pips i can take partial profit
        # With condition that, i will take all partial profit until 0.01 left
        self.tpp_pips = self.totalStopLossPip / ((self.lotsize - 0.01) * 100)

        # Lotsize to take partial profit
        # Just take volume & minus 1 VOLUME_PER_LOT
        # When closing position, you have to use volume
        self.tpp_lotsize_in_volume = volume - int(utility.gConfigData[f"VOLUME_PER_LOT_{symbol}"])

    def getBidAndAsk(self):
        global g_command_queue
        # Check if exists, if not exists, subscribe, else, add 1 user
        if self.symbolId not in g_subscribe:
            # Trigger command `sub 41` to subscribe to asset
            g_command_queue.put(f"sub {self.symbolId}")
        else:
            g_subscribe[self.symbolId]["NumOfUser"] += int(1)


    def run(self):
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
                g_command_queue.put(f"ap {self.positionId} {self.entryPrice} {self.takeProfit}")
                break
        self.destroy()

    def destroy(self):
        global g_subscribe

        # No need remove from list in here, it already handled by stopRunningPosition()

        # Check & remove subscription if no more user left
        g_subscribe[self.symbolId]["NumOfUser"] -= int(1)
        if g_subscribe[self.symbolId]["NumOfUser"] == int(0):
            # You know, due to multithreading
            # After you set to None, it probably set to some value before
            # you unsubcribe completely
            # Im thinking we can just leave it have values no problem gua i guess
            del g_subscribe[self.symbolId]
            # This is unsubscribe
            g_command_queue.put(f"unsub {self.symbolId}")


        print(f"{self.positionId}:{self.symbol} is being destroyed.")
        del self
