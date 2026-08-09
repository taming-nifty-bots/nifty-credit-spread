import sys
import os
from tamingnifty import connect_definedge as edge
from tamingnifty import utils as util
from tamingnifty import ta
from datetime import datetime, timedelta
from dateutil import parser
import pandas as pd
pd.set_option('display.max_rows', None)
import time
import sys
from retry import retry
from slack_sdk import WebClient
from pymongo import MongoClient
from dotenv import (  # pip install python-dotenv
    find_dotenv,
    load_dotenv,
)

"""
slack_url = os.environ.get('slack_url')
slack_channel = os.environ.get('slack_channel')
CONNECTION_STRING = os.environ.get('CONNECTION_STRING')  #Mongo Connection
trade_end_time = parser.parse(str(os.environ.get('trade_end_time'))).time()
"""
dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)
slack_channel = "niftyweekly"
slack_client = WebClient(token=os.environ.get('slack_token'))
CONNECTION_STRING = os.environ.get('CONNECTION_STRING') #Mongo Connection
trade_end_time = parser.parse("15:28:00").time()
trade_start_time = parser.parse("09:16:00").time()

mongo_client = MongoClient(CONNECTION_STRING)
collection_name = "supertrend"

supertrend_collection = mongo_client['Bots'][collection_name]
instrument_name = ["NIFTY"]

# credit_spread.py reads `instrument_name + "_Renko"`. The signal must write the
# same document, otherwise the executor never sees these bricks.
def renko_doc_id(instrument):
    return instrument + "_Renko"


def get_supertrend_start_date(instrument):
    supertrend = supertrend_collection.find_one({"_id": renko_doc_id(instrument)})
    return supertrend["start_date"]


def get_high_low(instrument):
    supertrend = supertrend_collection.find_one({"_id": renko_doc_id(instrument)})
    return supertrend["initial_high"], supertrend["initial_low"], supertrend["initial_color"]

#@retry(tries=5, delay=5, backoff=2)
def main():
    print("Supertrend Started")
    util.notify(message="Nifty Supertrend bot has started!", slack_client=slack_client)
    # Track the time when the last notification was sent
    last_notification_time = datetime.now()
    while True:
        current_time = datetime.now().time()
        # Calculate elapsed time since the last notification
        notification_time = datetime.now()

        # Calculate elapsed time since the last notification
        elapsed_time = notification_time - last_notification_time
        print(f"elapsed time: {elapsed_time}")
        if elapsed_time >= timedelta(hours=1):
            util.notify(message=f"{instrument_name} Supertrend bot is Alive!", slack_client=slack_client)
            util.notify(message=f"current time from {instrument_name} Supertrend: {current_time}", slack_client=slack_client)
            # Update the last notification time
            last_notification_time = notification_time

        if current_time > trade_start_time:
            for instrument in instrument_name:

                exchange = "NSE"
                if instrument == "NIFTY" or instrument == "supertrend":
                    trading_symbol = "Nifty 50"
                elif instrument == "BANKNIFTY":
                    trading_symbol = "Nifty Bank"


                days_ago = get_supertrend_start_date(instrument)
                days_ago_datetime = days_ago

                # Add one day
                start = days_ago_datetime + timedelta(days=1)
                start = start.replace(hour=9, minute=15, second=0, microsecond=0)
                end = datetime.today()

                conn = edge.login_to_integrate()
                initial_high, initial_low, initial_color = get_high_low(instrument)

                df = ta.renko(conn = conn, exchange = 'NSE', trading_symbol = trading_symbol, start=start, end=datetime.today(), brick_size=.05, last_high=initial_high, last_low=initial_low, initial_color=initial_color, initial_datetime=days_ago)

                print("\n***** Fetched 0.05% Renko Data *****\n")
                print(df.iloc[-20:])

                # Previous 40 bricks only. The iloc stop is exclusive, so -1 (the
                # newest brick, the one that breaks out) is deliberately omitted -
                # including it would make the breakout brick its own extreme and the
                # condition would never fire cleanly in a trend.
                high40 = df.iloc[-41:-1]['high'].max()
                low40 = df.iloc[-41:-1]['low'].min()
                df = ta.rsi(df, period=40)
                print(f"40 brick High: {high40}, Low: {low40}, RSI: {df.iloc[-1]['rsi']}")

                doc_id = renko_doc_id(instrument)
                if supertrend_collection.count_documents({"_id": doc_id}) == 0:
                    st = {"_id": doc_id, "datetime": df.iloc[-1]['datetime'], "color": df.iloc[-1]['color'], "close": df.iloc[-1]['close'], "rsi": df.iloc[-1]['rsi'], "last40_high": high40, "last40_low": low40, "start_date": start, "chart": "renko"}
                    supertrend_collection.insert_one(st)
                else:
                    supertrend_collection.update_one({'_id': doc_id}, {'$set': {"datetime": df.iloc[-1]['datetime'],
                                "close": df.iloc[-1]['close'], "color": df.iloc[-1]['color'], "rsi": df.iloc[-1]['rsi'], "last40_high": high40, "last40_low": low40, "chart": "renko"}})
            
            print("repeating loop for Supertrend")
        if current_time > trade_end_time:
            time.sleep(200)
            # Reseed tomorrow's first brick from the SAME chart type used above.
            # This previously called ta.pnf, which wrote Point & Figure seeds that
            # were then fed into ta.renko the next morning.
            df = ta.renko(conn = conn, exchange = 'NSE', trading_symbol = trading_symbol, start=start, end=datetime.today(), brick_size=.05, last_high=initial_high, last_low=initial_low, initial_color=initial_color, initial_datetime=days_ago)
            print("\n***** Fetched 0.05% Renko Data (end of day reseed) *****\n")

            # df.iloc[0] is the carried-over seed brick and still carries the OLD
            # start date, so anchoring on it would freeze start_date permanently.
            if df['datetime'].iloc[0].date() > days_ago.date():
                first_day = df['datetime'].iloc[0].date()
            else:
                first_day = df['datetime'].iloc[1].date()

            # Filter the DataFrame to include only the entries from the first day
            df_first_day = df[df['datetime'].dt.date == first_day]
            supertrend_collection.update_one({'_id': renko_doc_id(instrument)}, {'$set': {"initial_color": df_first_day.iloc[-1]['color'], "initial_high": df_first_day.iloc[-1]['high'], "initial_low": df_first_day.iloc[-1]['low'], "start_date": df_first_day.iloc[0]['datetime']}})
            return
        
        time.sleep(5)

if __name__ == "__main__":
    main()
