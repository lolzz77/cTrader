from main import sendProtoOASubscribeSpotsReq, sendProtoOAUnsubscribeSpotsReq
from utility import gConfigData
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATradeSide
import queue

# It needs to be defined here
# If i defined it in main.py,
# calling g_command_queue.put() has no effect i dk why
g_command_queue = queue.Queue()
# list of running positions
# [{positionId, RunningPosition object}]
g_positions = []
# list of symbols bid/ask price
# Initiate to 500, i want such that, if XAUUSD symbolId is 41, then insert into 41, easier to search
g_subscribe = [{"symbolId":None, "symbol":None, "bid":None, "ask":None, "NumOfUser":None} for _ in range(500)]



class RunningPosition:
    def __init__(self, positionId, symbolId, symbol, volume, tradeSide, entryPrice, stopLoss, takeProfit):
        self.positionId = positionId
        self.symbolId = symbolId
        self.symbol = symbol
        self.volume = volume
        self.tradeSide = tradeSide
        self.entryPrice = entryPrice
        self.stopLoss = stopLoss
        self.takeProfit = takeProfit
        self.price_per_pip = float(gConfigData[f"PRICE_PER_PIP_{symbol}"])
        # If buy, TP/SL at bid price
        # If sell, TP/SL at ask price
        self.tp_sl_at_bid_or_ask = "bid" if tradeSide == ProtoOATradeSide.Value('BUY') else "ask"
        self.stopLossPip = (self.entryPrice - self.stopLoss) / self.price_per_pip

    def getBidAndAsk(self):
        global g_command_queue
        # Check if exists, if not exists, subscribe, else, add 1 user
        if g_subscribe[self.symbolId]["symbolId"] is None:
            # Trigger command `sub 41` to subscribe to asset
            g_command_queue.put(f"sub {self.symbolId}")
        else:
            g_subscribe[self.symbolId]["NumOfUser"] += 1


    def run(self):
        while True:
            if g_subscribe[self.symbolId]["symbolId"] is None:
                continue
            runningPip = g_subscribe[self.symbolId][self.tp_sl_at_bid_or_ask] - self.entryPrice
            print(runningPip)
        self.destroy()

    def destroy(self):
        global g_positions
        global g_subscribe
        # remove position from list
        for p in g_positions[:]:  # Iterate over a copy of the list to avoid modification issues
            if p["positionId"] == self.positionId:
                g_positions.remove(p)
                break
        # Check & remove subscription if no more user left
        g_subscribe[self.symbolId]["NumOfUser"] -= 1
        if g_subscribe[self.symbolId]["NumOfUser"] == 0:
            # You know, due to multithreading
            # After you set to None, it probably set to some value before 
            # you unsubcribe completely
            # Im thinking we can just leave it have values no problem gua i guess
            g_subscribe[res.symbolId]["symbolId"] = None
            g_subscribe[res.symbolId]["symbol"] = None
            g_subscribe[res.symbolId]["bid"] = None
            g_subscribe[res.symbolId]["ask"] = None
            g_subscribe[res.symbolId]["NumOfUser"] = None
            # This is unsubscribe
            sendProtoOAUnsubscribeSpotsReq(self.symbolId)

        print(f"{self.positionId}:{self.symbol} is being destroyed.")
        del self
