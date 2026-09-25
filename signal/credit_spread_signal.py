import sys
import os
from tamingnifty import connect_dhan as edge
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


# Camarilla H4/L4 - computed here and stored on the signal doc. Gates nothing.
def camarilla_levels(conn, trading_symbol):
    """Never raises: the signal loop must keep running even if this fails."""
    try:
        df = edge.fetch_historical_data(conn, 'NSE', trading_symbol,
                                        datetime.now() - timedelta(days=20),
                                        datetime.now(), 'day')
        df['datetime'] = pd.to_datetime(df['datetime'])
        # Last session strictly before today: today's bar is still forming, and
        # this skips weekends/holidays automatically.
        prev = df[df['datetime'].dt.date < datetime.now().date()].iloc[-1]
        close = round(float(prev['close']), 2)
        rng = round(float(prev['high']) - float(prev['low']), 2)
        return {"cam_h4": round(close + 0.55 * rng, 2),
                "cam_l4": round(close - 0.55 * rng, 2)}
    except Exception as e:
        print(f"[camarilla] unavailable ({e}) - signal unaffected")
        return {}


# Sending the message must never be the thing that kills the bot. One network blip takes
# out both Dhan and Slack, and if notify() raises inside an error handler the exception
# escapes and the bot dies - which is the exact failure the handlers exist to prevent.
def safe_notify(message):
    try:
        util.notify(message=message, slack_client=slack_client, slack_channel=slack_channel)
    except Exception as e:
        print(f"Could not send the Slack message '{message}': {e}")


#@retry(tries=5, delay=5, backoff=2)
def main():
    print("Supertrend Started")
    safe_notify("Nifty Supertrend bot has started!")
    # Track the time when the last notification was sent
    last_notification_time = datetime.now()

    # Remember the last error we reported. This loop runs every 5 seconds, so an error
    # that keeps happening would post 12 Slack messages a minute and train us to ignore
    # the channel. We report a new error straight away, then repeat it at most once
    # every 15 minutes, and say so once when it clears.
    last_error = None
    last_error_time = None

    while True:
        current_time = datetime.now().time()

        # Everything below is wrapped because this bot previously had no exception
        # handling at all. Any error killed it, and the spread bot then carried on
        # trading against a NIFTY_Renko doc that had silently stopped updating - the
        # one failure in this system that looks completely healthy from the outside.
        try:
            # Calculate elapsed time since the last notification
            notification_time = datetime.now()

            # Calculate elapsed time since the last notification
            elapsed_time = notification_time - last_notification_time
            print(f"elapsed time: {elapsed_time}")
            if elapsed_time >= timedelta(hours=1):
                safe_notify(f"{instrument_name} Supertrend bot is Alive!")
                safe_notify(f"current time from {instrument_name} Supertrend: {current_time}")
                # Update the last notification time
                last_notification_time = notification_time

            # Log in here, outside the trading window check, so the token is minted when
            # the bot starts rather than at 9:16 when the spread bot is also starting.
            # Dhan only allows one token every 2 minutes and the two bots are separate
            # processes, so whoever asks second gets refused. After the first call this
            # line is free - login_to_dhan() just returns the token cached in os.environ.
            # Assigning conn here also means it is always defined for the end of day
            # reseed below, which used to raise NameError if the bot started after 15:28.
            conn = edge.login_to_dhan()

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

                    # Camarilla H4/L4: written separately from the signal fields above
                    # so that a failure here can never disturb them.
                    cam = camarilla_levels(conn, trading_symbol)
                    if cam:
                        print(f"Camarilla H4: {cam['cam_h4']}, L4: {cam['cam_l4']}")
                        supertrend_collection.update_one({'_id': doc_id}, {'$set': cam})

                print("repeating loop for Supertrend")

            # Got all the way through without raising. If we were failing before, say
            # so - otherwise the channel shows an error and never tells you it stopped.
            if last_error is not None:
                safe_notify("Supertrend bot recovered, bricks are updating again.")
                last_error = None
                last_error_time = None

        except Exception as e:
            error_text = str(e)
            print(f"Exception occurred: {error_text}")

            if last_error is None:
                should_notify = True
            elif error_text != last_error:
                should_notify = True
            elif datetime.now() - last_error_time >= timedelta(minutes=15):
                should_notify = True
            else:
                should_notify = False

            if should_notify:
                safe_notify(f"Supertrend bot exception: {error_text}")
                last_error = error_text
                last_error_time = datetime.now()

        if current_time > trade_end_time:
            # The reseed needs its own handler. It uses trading_symbol, start,
            # initial_high, initial_low, initial_color, days_ago and instrument, all of
            # which are only assigned inside the trading window loop above - so if that
            # loop failed every time today, these names do not exist and this block
            # raises NameError. Either way the bot must exit cleanly and say what
            # happened, because a failed reseed is invisible until tomorrow morning.
            try:
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
                safe_notify("Closing Bell, Supertrend end of day reseed is done.")
            except Exception as e:
                # Loud on purpose. Without a reseed the doc keeps today's old seed, so
                # tomorrow's chart rebuilds from a stale start_date - it will still run,
                # it will just be wrong, and nothing else will ever tell you.
                safe_notify(f"END OF DAY RESEED FAILED: {str(e)} - NIFTY_Renko still holds today's old seed. Check it before 9:15 tomorrow.")

            return
        
        # The Renko bricks are built from 1 minute candles, so polling faster than the
        # data can change just rewrites an identical document. This loop is stateless -
        # it recomputes the whole series from start_date every pass - so a longer sleep
        # costs latency only, it can never skip a brick.
        time.sleep(20)

if __name__ == "__main__":
    main()
