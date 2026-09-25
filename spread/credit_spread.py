from pymongo import MongoClient
import os
import sys
from tamingnifty import connect_dhan as edge
from tamingnifty import utils as util
import time
from retry import retry
import datetime
from datetime import timedelta
from dateutil import parser
import pandas as pd
from slack_sdk import WebClient
pd.set_option('display.max_rows', None)
from dotenv import (  # pip install python-dotenv
    find_dotenv,
    load_dotenv,
)

"""
slack_url = os.environ.get('slack_url')
slack_channel = os.environ.get('slack_channel')
CONNECTION_STRING = os.environ.get('CONNECTION_STRING')  #Mongo Connection
user_name = os.environ.get('user_name')
quantity = os.environ.get('quantity')
trade_start_time = parser.parse("9:29:00").time()
trade_end_time = parser.parse(str(os.environ.get('trade_end_time'))).time()
slack_client = WebClient(token=os.environ.get('slack_client'))
"""
dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)

slack_channel = "niftyweekly"
CONNECTION_STRING = os.environ.get('CONNECTION_STRING') 
user_name = os.environ.get('user_name')
trade_start_time = parser.parse("9:16:05").time()
trade_end_time = parser.parse("15:28:00").time()
slack_client = WebClient(token=os.environ.get('slack_token'))
quantity = os.environ.get('quantity')
instrument_name = os.environ.get('instrument_name')
lot_size = 65

# Set live_trading=true in the .env only when you actually want real money orders
# going to Dhan. Anything else - including the variable being missing entirely -
# means orders are simulated, which is what forward testing runs on.
live_trading = os.environ.get('live_trading', 'false').lower() == 'true'
print(f"live_trading = {live_trading}")


mongo_client = MongoClient(CONNECTION_STRING)

strategies_collection_name = instrument_name.lower() + "_weekly" + "_" + user_name
orders_collection_name = "orders_" + instrument_name.lower() +"_weekly" + "_" + user_name
mongo_doc_id = instrument_name + "_Renko"
# trades collection
strategies = mongo_client['Bots'][strategies_collection_name]
orders = mongo_client['Bots'][orders_collection_name]  # orders collection
supertrend_collection = mongo_client['Bots']["supertrend"]


def get_instrument_close():
    supertrend = supertrend_collection.find_one({"_id": mongo_doc_id})
    print(f"{instrument_name} Close: {supertrend['close']}")
    return supertrend['close']

def get_high40():
    #
    supertrend = supertrend_collection.find_one({"_id": mongo_doc_id})
    print(f"{instrument_name} High of last 40 Bricks: {supertrend['last40_high']}")
    return supertrend['last40_high']

def get_low40():
    #
    supertrend = supertrend_collection.find_one({"_id": mongo_doc_id})
    print(f"{instrument_name} Low of last 40 bricks: {supertrend['last40_low']}")
    return supertrend['last40_low']

@retry(tries=5, delay=5, backoff=2)
def get_close_time():
    #
    supertrend = supertrend_collection.find_one({"_id": mongo_doc_id})
    print(f"{instrument_name} Last Brick Close time: {supertrend['datetime']}")
    return supertrend['datetime']


# Camarilla H4/L4 - read from the signal doc and recorded on every entry. Gates nothing.
def get_camarilla_context():
    """Never raises: the orders are already filled by the time this runs."""
    try:
        supertrend = supertrend_collection.find_one({"_id": mongo_doc_id})
        return {k: supertrend[k] for k in ('cam_h4', 'cam_l4') if k in supertrend}
    except Exception as e:
        print(f"[camarilla] unavailable ({e}) - trade unaffected")
        return {}


@retry(tries=5, delay=5, backoff=2)
def get_last_exit_time():
    #
    supertrend = supertrend_collection.find_one({"_id": mongo_doc_id})
    print(f"{instrument_name} Last Brick Close time: {supertrend['lastexittime']}")
    return supertrend['lastexittime']

@retry(tries=5, delay=5, backoff=2)
def update_last_exit_time():
    #
    supertrend_collection.update_one({"_id": mongo_doc_id}, {"$set": {"lastexittime": get_close_time()}})
    return

def check_quantity(qty):
    """
    Refuse anything above the exchange freeze limit.

    This runs before the FIRST leg is sent, not after. A spread where one leg is
    accepted and the other is rejected for being too large would leave a naked short
    option running, which is the worst thing this bot could possibly do.
    """
    if int(qty) > edge.NIFTY_FREEZE_QTY:
        message = (f"Quantity {qty} is above the exchange freeze limit of "
                   f"{edge.NIFTY_FREEZE_QTY}. No order was placed.")
        util.notify(message, slack_client=slack_client)
        raise Exception(message)


def simulated_order(conn, symbol, security_id, qty, transaction_type):
    """
    Build an order dict that looks exactly like a real Dhan order, but without
    sending anything to the broker. The fill price is the close of the most recent
    1 minute candle, which is what the Definedge version did as well.

    This is what runs during forward testing, i.e. whenever live_trading is false.
    """
    start = datetime.datetime.now() - timedelta(days=7)
    price = edge.get_option_price(conn, security_id, start, datetime.datetime.today(), 'min')
    return {
        "orderId": "SIMULATED",
        "orderStatus": "TRADED",
        "transactionType": transaction_type,
        "exchangeSegment": "NSE_FNO",
        "productType": "MARGIN",
        "orderType": "MARKET",
        "validity": "DAY",
        "tradingSymbol": symbol,
        "securityId": str(security_id),
        "quantity": int(qty),
        "filledQty": int(qty),
        "averageTradedPrice": price,
        "createTime": datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
        "simulated": True,
    }


def submit_order(symbol, security_id, qty, transaction_type):
    """
    Place one leg of a spread and return the final Dhan order dict.

    The keys here are Dhan's own, not Definedge's: the status field is orderStatus
    and a successful fill is "TRADED" (Definedge called it "COMPLETE"), and the fill
    price is averageTradedPrice.

    The caller MUST check orderStatus == "TRADED" before reading averageTradedPrice.
    A rejected order has no fill price.
    """
    check_quantity(qty)
    conn = edge.login_to_dhan()

    if live_trading == True:
        response = edge.place_order(conn, security_id, transaction_type, int(qty))
        print(f"Order accepted by Dhan: {response}")
        # Dhan only tells us the order was accepted. Poll until it is actually
        # filled (or rejected) so we know the real traded price.
        order = edge.wait_for_fill(conn, response['orderId'])
    else:
        order = simulated_order(conn, symbol, security_id, qty, transaction_type)

    print(f"Order placed: {order}")
    util.notify(f"Order placed: {order}", slack_client=slack_client)
    orders.insert_one(order)
    return order


# @retry(tries=5, delay=5, backoff=2)
def place_buy_order(symbol, security_id, qty):
    return submit_order(symbol, security_id, qty, "BUY")


# @retry(tries=5, delay=5, backoff=2)
def place_sell_order(symbol, security_id, qty):
    return submit_order(symbol, security_id, qty, "SELL")


# @retry(tries=5, delay=5, backoff=2)
def get_st_strike():
    return util.round_to_nearest(x=get_instrument_close(), base=100)



# Option contract lookup now lives in the library, as edge.get_index_option_symbol().
# It reads Dhan's scrip master CSV and returns the security id as well as the symbol,
# because Dhan orders are placed against a numeric security id, not a symbol string.
# The old load_csv_from_zip() / get_option_symbol() pair that read Definedge's
# allmaster.zip has been removed.


# @retry(tries=5, delay=5, backoff=2)
def create_bear_call_spread():
    option_type = "CE"
    atm = get_st_strike()
    instrument_close = get_instrument_close()
    sell_strike = atm + 100
    buy_strike = atm + 400
    util.notify(f"ST Strike: {atm}, SELL Strike: {sell_strike}, BUY Strike: {buy_strike}, Instrument Close: {instrument_close}",slack_client=slack_client)
    sell_strike_symbol, sell_security_id, expiry, contract_lot_size = edge.get_index_option_symbol(sell_strike, option_type, instrument_name)
    buy_strike_symbol, buy_security_id, expiry, contract_lot_size = edge.get_index_option_symbol(buy_strike, option_type, instrument_name)
    print(f"Expiry: {expiry}")

    # Buy the far hedge FIRST. If only one leg of the two ever goes through, we want
    # it to be the one that limits the loss, not the one that creates it.
    buy_order = place_buy_order(buy_strike_symbol, buy_security_id, quantity)
    if buy_order['orderStatus'] != "TRADED":
        # Nothing is on the book, so it is safe to skip this entry and let the main
        # loop try again on the next pass.
        util.notify(f"Hedge leg not filled, no spread created: {buy_order}",slack_client=slack_client)
        raise Exception("Buy leg not filled - " + str(buy_order))

    sell_order = place_sell_order(sell_strike_symbol, sell_security_id, quantity)
    if sell_order['orderStatus'] != "TRADED":
        # The hedge IS filled but the short is not, so we are sitting on a long
        # option that nothing is watching, and no Mongo document was written. Stop
        # the bot: if we carried on looping it would place a second spread on top.
        util.notify(f"SHORT LEG FAILED after the hedge filled. Long {buy_strike_symbol} is OPEN. MANUAL ACTION REQUIRED. Bot is stopping.",slack_client=slack_client)
        sys.exit(1)

    short_option_cost = sell_order['averageTradedPrice']
    long_option_cost = buy_order['averageTradedPrice']
    util.notify("created bear call spread!",slack_client=slack_client)
    record_details_in_mongo(sell_strike_symbol, sell_security_id, buy_strike_symbol, buy_security_id, "Bearish", instrument_close, expiry, short_option_cost, long_option_cost)



# @retry(tries=5, delay=5, backoff=2)
def record_details_in_mongo(sell_strike_symbol, sell_security_id, buy_strike_symbol, buy_security_id, trend, instrument_close, expiry, short_option_cost, long_option_cost):
    conn = edge.login_to_dhan()
    vix = edge.fetch_ltp(conn, 'NSE', 'India VIX')
    strategy = {
    'instrument_name': instrument_name,
    'India Vix': vix,
    'quantity': int(quantity),
    'lot_size': lot_size,
    'short_exit_price': 0,
    'long_exit_price': 0,
    'strategy_state': 'active',
    'entry_date': str(datetime.datetime.now().date()),
    'exit_date': '',
    'trend' : trend,
    'short_option_symbol' : sell_strike_symbol,
    # The security ids are what the exit orders and the running PnL are placed
    # against. Without them stored here we would have to search the scrip master
    # again on every exit, and Dhan has no way to trade a plain symbol string.
    'short_option_security_id' : sell_security_id,
    'long_option_symbol' : buy_strike_symbol,
    'long_option_security_id' : buy_security_id,
    'short_option_cost' : short_option_cost,
    'long_option_cost' : long_option_cost,
    'total_credit_received' : round((short_option_cost - long_option_cost) * int(quantity),2),
    # 'stop_loss' : round((short_option_cost - long_option_cost) * int(quantity) * -0.5,2),
    # 'trailing_stop_loss' : round((short_option_cost - long_option_cost) * int(quantity) * -0.5,2),
    'entry_time' : datetime.datetime.now().strftime('%H:%M'),
    'exit_time' : '',
    'instrument_close' : round(instrument_close,2),
    'expiry' : str(expiry),
    'running_pnl' : 0,
    'pnl': '',
    'max_pnl_reached': 0,
    'min_pnl_reached': 0
    }
    # Observational fields only - appended after the dict is built so they can
    # never interfere with any value above.
    strategy.update(get_camarilla_context())
    strategies.insert_one(strategy)



# @retry(tries=5, delay=5, backoff=2)
def create_bull_put_spread():
    option_type = "PE"
    atm = get_st_strike()
    instrument_close = get_instrument_close()
    sell_strike = atm - 100
    buy_strike = atm - 400
    util.notify(f"ATM Strike: {atm}, SELL Strike: {sell_strike}, BUY Strike: {buy_strike}, Instrument Close: {instrument_close}",slack_client=slack_client)
    sell_strike_symbol, sell_security_id, expiry, contract_lot_size = edge.get_index_option_symbol(sell_strike, option_type, instrument_name)
    buy_strike_symbol, buy_security_id, expiry, contract_lot_size = edge.get_index_option_symbol(buy_strike, option_type, instrument_name)
    print(f"Expiry: {expiry}")

    # Buy the far hedge FIRST. If only one leg of the two ever goes through, we want
    # it to be the one that limits the loss, not the one that creates it.
    buy_order = place_buy_order(buy_strike_symbol, buy_security_id, quantity)
    if buy_order['orderStatus'] != "TRADED":
        # Nothing is on the book, so it is safe to skip this entry and let the main
        # loop try again on the next pass.
        util.notify(f"Hedge leg not filled, no spread created: {buy_order}",slack_client=slack_client)
        raise Exception("Buy leg not filled - " + str(buy_order))

    sell_order = place_sell_order(sell_strike_symbol, sell_security_id, quantity)
    if sell_order['orderStatus'] != "TRADED":
        # The hedge IS filled but the short is not, so we are sitting on a long
        # option that nothing is watching, and no Mongo document was written. Stop
        # the bot: if we carried on looping it would place a second spread on top.
        util.notify(f"SHORT LEG FAILED after the hedge filled. Long {buy_strike_symbol} is OPEN. MANUAL ACTION REQUIRED. Bot is stopping.",slack_client=slack_client)
        sys.exit(1)

    short_option_cost = sell_order['averageTradedPrice']
    long_option_cost = buy_order['averageTradedPrice']
    util.notify("created bull put spread!",slack_client=slack_client)
    record_details_in_mongo(sell_strike_symbol, sell_security_id, buy_strike_symbol, buy_security_id, "Bullish", instrument_close, expiry, short_option_cost, long_option_cost)

def calculate_pnl(quantity, long_entry, long_exit, short_entry, short_exit):
    pnl = float(quantity) * ((float(short_entry) - float(short_exit)) + (float(long_exit) - float(long_entry)))
    return round(pnl, 2)

# @retry(tries=5, delay=5, backoff=2)
def close_active_positions():
    print(f"Closing active positions {instrument_name}")
    util.notify(f"Closing active positions {instrument_name}",slack_client=slack_client)
    active_strategies = strategies.find({'strategy_state': 'active'})
    for strategy in active_strategies:
        # Buy back the SHORT leg first. That is the leg carrying the open risk, so
        # if only one of the two goes through it needs to be this one.
        buy_order = place_buy_order(strategy['short_option_symbol'], strategy['short_option_security_id'], strategy['quantity'])
        if buy_order['orderStatus'] != "TRADED":
            util.notify(f"Could not buy back the short leg {strategy['short_option_symbol']}. The spread is STILL OPEN. MANUAL ACTION REQUIRED. Bot is stopping.",slack_client=slack_client)
            sys.exit(1)
        util.notify("Short option leg closed",slack_client=slack_client)

        sell_order = place_sell_order(strategy['long_option_symbol'], strategy['long_option_security_id'], strategy['quantity'])
        if sell_order['orderStatus'] != "TRADED":
            # Retrying this on the next loop would buy back the short leg a second
            # time and leave us naked long, so stop instead.
            util.notify(f"Short leg is closed but the long hedge {strategy['long_option_symbol']} is STILL OPEN. MANUAL ACTION REQUIRED. Bot is stopping.",slack_client=slack_client)
            sys.exit(1)
        util.notify("Long option leg closed",slack_client=slack_client)

        update_last_exit_time()
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'strategy_state': 'closed'}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_date': str(datetime.datetime.now().date())}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_time': datetime.datetime.now().strftime('%H:%M')}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'short_exit_price': buy_order['averageTradedPrice']}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'long_exit_price': sell_order['averageTradedPrice']}})
        pnl = calculate_pnl(strategy['quantity'], strategy['long_option_cost'], sell_order['averageTradedPrice'], strategy['short_option_cost'],buy_order['averageTradedPrice'])
        util.notify(f"Realized Gains: {round(pnl, 2)}",slack_client=slack_client)
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'pnl': pnl}})
    return

# @retry(tries=5, delay=5, backoff=2)
def get_pnl(strategy, start=None):
    conn = edge.login_to_dhan()
    if start is None:
        days_ago = datetime.datetime.now() - timedelta(days=7)
        start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    short_option_cost = edge.get_option_price(conn, strategy['short_option_security_id'], start, datetime.datetime.today(), 'min')
    long_option_cost = edge.get_option_price(conn, strategy['long_option_security_id'], start, datetime.datetime.today(), 'min')
    current_pnl = calculate_pnl(strategy['quantity'], strategy['long_option_cost'], long_option_cost, strategy['short_option_cost'], short_option_cost)
    strategies.update_one({'_id': strategy['_id']}, {'$set': {'running_pnl': current_pnl}})
    return current_pnl



# @retry(tries=5, delay=5, backoff=2)
def main():
    util.notify(f"{instrument_name} Positional bot kicked off",slack_client=slack_client)
    print(f"{instrument_name} Positional bot kicked off")
    days_ago = datetime.datetime.now() - timedelta(days=7)
    start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    
    # Track the time when the last notification was sent
    last_notification_time = datetime.datetime.now()
    while True:
        try:
            current_time = datetime.datetime.now().time()
            notification_time = datetime.datetime.now()

            # Calculate elapsed time since the last notification
            elapsed_time = notification_time - last_notification_time
            print(f"elapsed time: {elapsed_time}")
            if elapsed_time >= timedelta(hours=1):
                util.notify(message=f"{instrument_name} Weekly Credit Spread bot is Alive!", slack_client=slack_client)
                util.notify(message=f"current time from {instrument_name} Credit Spread: {current_time}", slack_client=slack_client)
                # Update the last notification time
                last_notification_time = notification_time
                
            print(f"current time: {current_time}")

            # Log in here, outside the trading window check, so the token is minted
            # when the bot starts rather than at the moment it wants to place an order.
            # Dhan only allows one token every 2 minutes and the signal bot is a
            # separate process asking for its own, so whoever asks second is refused.
            # On 2026-09-25 the first login of the day happened at 2:44pm inside
            # submit_order, collided with the signal bot, and the bull put spread was
            # never created. After the first call this line is free - login_to_dhan()
            # just returns the token already cached in os.environ.
            edge.login_to_dhan()

            if current_time > trade_start_time:
                print("Trading Window is active.")
                if strategies.count_documents({'strategy_state': 'active'}) > 0:
                    active_strategies = strategies.find(
                        {'strategy_state': 'active'})
                    for strategy in active_strategies:
                        pnl = get_pnl(strategy, start)
                        if strategy['max_pnl_reached'] < pnl:
                            strategies.update_one({'_id': strategy['_id']}, {'$set': {'max_pnl_reached': pnl}})
                            #strategies.update_one({'_id': strategy['_id']}, {'$set': {'trailing_stop_loss': strategy['stop_loss'] + pnl}})

                        if strategy['min_pnl_reached'] > pnl:
                            strategies.update_one({'_id': strategy['_id']}, {'$set': {'min_pnl_reached': pnl}})
                        
                        # if pnl <= strategy['trailing_stop_loss']:
                        #     util.notify(f"SL HIT! Current PnL: {pnl}",slack_client=slack_client, slack_channel=slack_channel)
                        #     close_active_positions()
                        #     time.sleep(60)
                        #     break

                        # There is deliberately no profit target here. A 70% target was
                        # tried and backtested over Jul-Sep 2026: it booked well on its own
                        # exits, but it kept ending trades while the Donchian still pointed
                        # the same way, so the bot re-entered the same trend at a worse
                        # price. Donchian exits went from -4,680 across 13 trades to -30,930
                        # across 14, and the run-to-expiry winners that pay for the losers
                        # (+5,140 average) mostly disappeared. Net cost over the window was
                        # 2,561 on one lot. The trade is held to a flip or to expiry.

                        if (strategy['trend'] == 'Bullish' and get_instrument_close() < get_low40()) or (strategy['trend'] == 'Bearish' and get_instrument_close() > get_high40()):
                            util.notify(f"Donchian Trend Changed",slack_client=slack_client)
                            close_active_positions()
                            time.sleep(60)
                            break

                        print(str(datetime.datetime.now().date()))
                        if current_time > datetime.time(hour=11, minute=45) and strategy['expiry'] == str(datetime.datetime.now().date()):
                            util.notify("Closing positions on Expiry",slack_client=slack_client)
                            close_active_positions()
                            break
                else:
                    if get_instrument_close() > get_high40() and get_close_time() > get_last_exit_time():
                        create_bull_put_spread()
                    elif get_instrument_close() < get_low40() and get_close_time() > get_last_exit_time():
                        create_bear_call_spread()
                    else:
                        print("waiting for a breakout to create new positions!")
        except Exception as e:
            util.notify(f"Exception occurred: {str(e)}", slack_client=slack_client, slack_channel=slack_channel)
        
        if current_time > trade_end_time:
            util.notify("Closing Bell, Bot will exit now",slack_client=slack_client)
            return   
        time.sleep(10)
if __name__ == "__main__":
    main()
