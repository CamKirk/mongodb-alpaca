from alpaca_trade_api import REST, TimeFrame
import sys
import pandas as pd
from datetime import timedelta, date
from pymongo import MongoClient

# Alpaca api setup
alpaca = REST()

# mongo setup
mongoclient = MongoClient()

db = mongoclient.traderbot

reports_coll = db.reports

# CLI read
ticker = sys.argv[1].upper()

# Data acq
ohlc = alpaca.get_bars(
    ticker,
    TimeFrame.Hour,
    start = date.today() - timedelta(days=22),
    end = date.today() - timedelta(days=1)
).df

# Calculations
ohlc["SMA20"] = ohlc.close.rolling(20).mean()
ohlc["SMA50"] = ohlc.close.rolling(50).mean()
ohlc["distance"] = ohlc.SMA20 - ohlc.SMA50
ohlc["direction"] = ohlc.distance.apply(lambda x: int(x > 0))
ohlc["signal"] = ohlc.direction.shift()

# Last row isolation
lastrow = ohlc.iloc[-1]

# encode direction
direction = "UP" if lastrow.direction > 0 else "DOWN"

# encode signal
signal = "HOLD"

if lastrow.signal > 0:
    signal="SELL"
elif lastrow.signal < 0: 
    signal="BUY"

# store row in mongo
reports_coll.insert_one(lastrow.to_dict())

# message prep
message = f"""
Report for {ticker}
The current recommendation is to {signal}
The current trend is {direction}
The SMA20 is: {lastrow.SMA20}
The SMA50 is: {lastrow.SMA50}
The SMA distance is: {lastrow.distance}
The most recent price is: {lastrow.close}
"""
print(message)

last_report = reports_coll.find_one()

print(last_report)

