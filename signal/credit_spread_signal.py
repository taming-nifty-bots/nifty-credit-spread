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

def get_supertrend_start_date(instrument):
    supertrend = supertrend_collection.find_one({"_id": instrument})
    return supertrend["start_date"]


def get_high_low(instrument):
    supertrend = supertrend_collection.find_one({"_id": instrument})
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

                df = ta.pnf(conn, exchange, trading_symbol, start, end, 'min', brick_size=.05, last_high=initial_high, last_low=initial_low, initial_color=initial_color)
                
                print("\n***** Fetched 1 min PnF Data *****\n")
                # st2 = ta.supertrend(df.copy(),10, 2)
                # print(st2.iloc[-20:])
                df = ta.supertrend(df, 10, 3.5)
                df = ta.xo_zone(df, 4)
                df = ta.rsi_avg(df, 14)
                print(df.iloc[-20:])
                # print(st2.iloc[-20:])

                if supertrend_collection.count_documents({"_id": instrument}) == 0:
                    st = {"_id": instrument, "datetime": df.iloc[-1]['datetime'], "value": df.iloc[-1]['ST'], "signal": df.iloc[-1]['signal'], "color":df.iloc[-1]['color'], "xo_zone":df.iloc[-1]['xo_zone'], "rsi": df.iloc[-1]['rsi'], "double_top_buy":bool(df.iloc[-1]['double_top_buy']), "double_bottom_sell":bool(df.iloc[-1]['double_bottom_sell']), "prev_signal": df.iloc[-2]['signal']}
                    supertrend_collection.insert_one(st)
                else:
                    supertrend_collection.update_one({'_id': instrument}, {'$set': {"datetime": df.iloc[-1]['datetime'],
                                "value": df.iloc[-1]['ST'], "close": df.iloc[-1]['close'], "signal": df.iloc[-1]['signal'], "color":df.iloc[-1]['color'], "xo_zone":df.iloc[-1]['xo_zone'], "rsi": df.iloc[-1]['rsi'], "double_top_buy":bool(df.iloc[-1]['double_top_buy']), "double_bottom_sell":bool(df.iloc[-1]['double_bottom_sell']), "prev_signal": df.iloc[-2]['signal']}})
            
            print("repeating loop for Supertrend")
        if current_time > trade_end_time:
            time.sleep(200)
            # Extract the date of the first entry
            df = ta.pnf(conn, exchange, trading_symbol, start, end, 'min', brick_size=.05, last_high=initial_high, last_low=initial_low, initial_color=initial_color)
            first_day = df['datetime'].iloc[0].date()

            # Filter the DataFrame to include only the entries from the first day
            df_first_day = df[df['datetime'].dt.date == first_day]            
            supertrend_collection.update_one({'_id': instrument}, {'$set': {"initial_color": df_first_day.iloc[-1]['color'], "initial_high": df_first_day.iloc[-1]['high'], "initial_low": df_first_day.iloc[-1]['low'], "start_date": df_first_day.iloc[0]['datetime']}})
            return
        
        time.sleep(5)

if __name__ == "__main__":
    main()
