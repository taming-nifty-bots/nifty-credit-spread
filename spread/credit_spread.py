from pymongo import MongoClient
import os
import sys
from tamingnifty import connect_definedge as edge
from tamingnifty import utils as util
import requests
import time
import zipfile
from retry import retry
import io
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


mongo_client = MongoClient(CONNECTION_STRING)

strategies_collection_name = instrument_name.lower() + "_weekly" + "_" + user_name
orders_collection_name = "orders_" + instrument_name.lower() +"_weekly" + "_" + user_name

# trades collection
strategies = mongo_client['Bots'][strategies_collection_name]
orders = mongo_client['Bots'][orders_collection_name]  # orders collection


def get_supertrend_direction():
    supertrend_collection = mongo_client['Bots']["supertrend"]
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} Trend: {supertrend['signal']}")
    return supertrend["signal"]

def get_prev_supertrend_direction():
    supertrend_collection = mongo_client['Bots']["supertrend"]
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} Trend: {supertrend['prev_signal']}")
    return supertrend["prev_signal"]

def get_coloumn_color():
    supertrend_collection = mongo_client['Bots']["supertrend"]
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} Coloumn Color: {supertrend['color']}")
    return supertrend["color"]



def get_xo_zone():
    supertrend_collection = mongo_client['Bots']["supertrend"]
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} xo_zone: {supertrend['xo_zone']}")
    return supertrend["xo_zone"]


# @retry(tries=5, delay=5, backoff=2)
def get_supertrend_value():
    supertrend_collection = mongo_client['Bots']["supertrend"]
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"Super Trend Value: {supertrend['value']}")
    return supertrend["value"]


# @retry(tries=5, delay=5, backoff=2)
def get_instrument_close():
    supertrend_collection = mongo_client['Bots']["supertrend"]
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} Close: {supertrend['close']}")
    return supertrend['close']


# @retry(tries=5, delay=5, backoff=2)
def get_DTB():
    supertrend_collection = mongo_client['Bots']["supertrend"]
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} DTB: {supertrend['double_top_buy']}")
    return supertrend['double_top_buy']

def get_DBS():
    supertrend_collection = mongo_client['Bots']["supertrend"]
    supertrend = supertrend_collection.find_one({"_id": instrument_name})
    print(f"{instrument_name} DBS: {supertrend['double_bottom_sell']}")
    return supertrend['double_bottom_sell']

# @retry(tries=5, delay=5, backoff=2)
def place_buy_order(symbol, qty):
    # conn = edge.login_to_integrate(True)
    # io = edge.IntegrateOrders(conn)
    # order = io.place_order(
    #     exchange=conn.EXCHANGE_TYPE_NFO,
    #     order_type=conn.ORDER_TYPE_BUY,
    #     price=0,
    #     price_type=conn.PRICE_TYPE_MARKET,
    #     product_type=conn.PRODUCT_TYPE_NORMAL,
    #     quantity=qty,
    #     tradingsymbol=symbol,
    # )

    # order_id = order['order_id']
    # order = get_order_by_order_id(conn, order_id)
    # print(f"Order Status: {order['order_status']}")
    # if order['order_status'] != "COMPLETE":
    #     time.sleep(2)
    #     order = get_order_by_order_id(conn, order_id)
    #     print(f"Order Status after retry: {order['order_status']}")
    # if order['order_status'] != "COMPLETE":
    #     util.notify(f"Order Message: {order['message']}",slack_client=slack_client)
    #     util.notify(f"Order Failed: {order}",slack_client=slack_client)
    #     orders.insert_one(order)
    #     raise Exception("Error in placing order - " +
    #                 str(order['message']))
    
    ############################Temp Code for testing purpose only########################
    ######################################################################################
    order = {
        "order_id": "25052900010716",
        "last_fill_qty": qty,
        "tradingsymbol": symbol,
        "token": "40470",
        "quantity": qty,
        "price_type": "MARKET",
        "product_type": "NORMAL",
        "order_entry_time": datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
        "order_status": "COMPLETE",
        "order_type": "BUY",
        "exchange_orderid": "1100000127824637",
        "message": " ",
        "pending_qty": "0",
        "price": "0.00",
        "exchange_time": "29-05-2025 13:08:22",
        "average_traded_price": edge.get_option_price('NFO', symbol, (datetime.datetime.now() - timedelta(days=7)), datetime.datetime.today(), 'min'),
        "exchange": "NFO",
        "filled_qty": qty,
        "disclosed_quantity": "0",
        "validity": "DAY",
        "ordersource": "TRTP"
    }
    ############################Temp Code for testing purpose ENDS HERE########################
    ######################################################################################
    print(f"Order placed: {order}")
    util.notify(f"Order placed: {order}",slack_client=slack_client)
    orders.insert_one(order)
    return order


# @retry(tries=5, delay=5, backoff=2)
def place_sell_order(symbol, qty):
    # conn = edge.login_to_integrate(True)
    # io = edge.IntegrateOrders(conn)
    # order = io.place_order(
    #     exchange=conn.EXCHANGE_TYPE_NFO,
    #     order_type=conn.ORDER_TYPE_SELL,
    #     price=0,
    #     price_type=conn.PRICE_TYPE_MARKET,
    #     product_type=conn.PRODUCT_TYPE_NORMAL,
    #     quantity=qty,
    #     tradingsymbol=symbol,
    # )
    # order_id = order['order_id']
    # order = get_order_by_order_id(conn, order_id)
    # print(f"Order Status: {order['order_status']}")
    # if order['order_status'] != "COMPLETE":
    #     time.sleep(2)
    #     order = get_order_by_order_id(conn, order_id)
    #     print(f"Order Status after retry: {order['order_status']}")
    # if order['order_status'] != "COMPLETE":
    #     util.notify(f"Order Message: {order['message']}",slack_client=slack_client)
    #     util.notify(f"Order Failed: {order}",slack_client=slack_client)
    #     orders.insert_one(order)
    #     raise Exception("Error in placing order - " +
    #                 str(order['message']))
    ############################Temp Code for testing purpose only########################
    ######################################################################################
    order = {
        "order_id": "25052900010716",
        "last_fill_qty": qty,
        "tradingsymbol": symbol,
        "token": "40470",
        "quantity": qty,
        "price_type": "MARKET",
        "product_type": "NORMAL",
        "order_entry_time": datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
        "order_status": "COMPLETE",
        "order_type": "SELL",
        "exchange_orderid": "1100000127824637",
        "message": " ",
        "pending_qty": "0",
        "price": "0.00",
        "exchange_time": "29-05-2025 13:08:22",
        "average_traded_price": edge.get_option_price('NFO', symbol, (datetime.datetime.now() - timedelta(days=7)), datetime.datetime.today(), 'min'),
        "exchange": "NFO",
        "filled_qty": qty,
        "disclosed_quantity": "0",
        "validity": "DAY",
        "ordersource": "TRTP"
    }
    ############################Temp Code for testing purpose ENDS HERE########################
    ######################################################################################
    util.notify(f"Order placed: {order}",slack_client=slack_client)
    orders.insert_one(order)
    return order


# @retry(tries=5, delay=5, backoff=2)
def get_order_by_order_id(conn: edge.ConnectToIntegrate, order_id):
    io = edge.IntegrateOrders(conn)
    print(f"Getting order by order ID: {order_id}")
    order = io.order(order_id)
    print(order)
    return order


# @retry(tries=5, delay=5, backoff=2)
def get_st_strike():
    return util.round_to_nearest(x=get_supertrend_value(), base=100)



# @retry(tries=5, delay=5, backoff=2)
def load_csv_from_zip(url='https://app.definedgesecurities.com/public/allmaster.zip'):
    column_names = ['SEGMENT', 'TOKEN', 'SYMBOL', 'TRADINGSYM', 'INSTRUMENT TYPE', 'EXPIRY', 'TICKSIZE', 'LOTSIZE', 'OPTIONTYPE', 'STRIKE', 'PRICEPREC', 'MULTIPLIER', 'ISIN', 'PRICEMULT', 'UnKnown']
    # Send a GET request to download the zip file
    response = requests.get(url)
    response.raise_for_status()  # This will raise an exception for HTTP errors
    # Open the zip file from the bytes-like object
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        # Extract the name of the first CSV file in the zip archive
        csv_name = thezip.namelist()[0]
        # Extract and read the CSV file into a pandas DataFrame
        with thezip.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file, header=None, names=column_names, low_memory=False, on_bad_lines='skip')
    df = df[(df['SEGMENT'] == 'NFO') & (df['INSTRUMENT TYPE'] == 'OPTIDX')]
    df = df[(df['SYMBOL'].str.startswith(instrument_name))]
    df = df[df['SYMBOL'] == instrument_name]
    df['EXPIRY'] = df['EXPIRY'].astype(str).apply(lambda x: x.zfill(8))
    df['EXPIRY'] = pd.to_datetime(df['EXPIRY'], format='%d%m%Y', errors='coerce')
    df = df.sort_values(by='EXPIRY', ascending=True)
    # Return the loaded DataFrame
    return df



# @retry(tries=5, delay=5, backoff=2)
def get_option_symbol(strike=19950, option_type = "PE" ):
    df = load_csv_from_zip()
    df = df[df['TRADINGSYM'].str.contains(str(strike))]
    df = df[df['OPTIONTYPE'].str.match(option_type)]
    # Get the current date
    current_date = datetime.datetime.now()
    # Calculate the start and end dates of the current week
    df= df[df['EXPIRY'] > (current_date + timedelta(days=6))]
    df = df.head(1)
    print("Getting options Symbol...")
    print(f"Symbol: {df['TRADINGSYM'].values[0]} , Expiry: {df['EXPIRY'].values[0]}")
    return df['TRADINGSYM'].values[0], df['EXPIRY'].values[0]




# @retry(tries=5, delay=5, backoff=2)
def create_bull_put_spread():
    option_type = "PE"
    atm = get_st_strike()
    instrument_close = get_instrument_close()
    sell_strike = atm + util.round_to_nearest(x=0.0075 * atm, base=100)
    if instrument_name == "NIFTY":
        buy_strike = sell_strike - 400
    elif instrument_name == "BANKNIFTY":
        buy_strike = sell_strike - 500
    util.notify(f"ST Strike: {atm}, SELL Strike: {sell_strike}, BUY Strike: {buy_strike}, Instrument Close: {instrument_close}",slack_client=slack_client)
    sell_strike_symbol, expiry = get_option_symbol(sell_strike, option_type)
    buy_strike_symbol, expiry = get_option_symbol(buy_strike, option_type)
    print(expiry)
    expiry = str(expiry)
    expiry = parser.parse(expiry).date()
    print(expiry)
    buy_order = place_buy_order(buy_strike_symbol, quantity)
    if buy_order['order_status'] == "COMPLETE":
         sell_order = place_sell_order(sell_strike_symbol, quantity)
    short_option_cost = sell_order['average_traded_price']
    long_option_cost = buy_order['average_traded_price']
    util.notify("created bull put spread!",slack_client=slack_client)
    record_details_in_mongo(sell_strike_symbol, buy_strike_symbol, "Bullish", instrument_close, expiry, short_option_cost, long_option_cost)



# @retry(tries=5, delay=5, backoff=2)
def record_details_in_mongo(sell_strike_symbol, buy_strike_symbol, trend, instrument_close, expiry, short_option_cost, long_option_cost):
    conn = edge.login_to_integrate()
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
    'long_option_symbol' : buy_strike_symbol,
    'short_option_cost' : short_option_cost,
    'long_option_cost' : long_option_cost,
    'total_credit_received' : round((short_option_cost - long_option_cost) * int(quantity),2),
    'stop_loss' : round((short_option_cost - long_option_cost) * int(quantity) * -0.5,2),
    'trailing_stop_loss' : round((short_option_cost - long_option_cost) * int(quantity) * -0.5,2),
    'entry_time' : datetime.datetime.now().strftime('%H:%M'),
    'exit_time' : '',
    'instrument_close' : round(instrument_close,2),
    'expiry' : str(expiry),
    'running_pnl' : 0,
    'pnl': '',
    'max_pnl_reached': 0,
    'min_pnl_reached': 0
    }
    strategies.insert_one(strategy)



# @retry(tries=5, delay=5, backoff=2)
def create_bear_call_spread():
    option_type = "CE"
    atm = get_st_strike()
    instrument_close = get_instrument_close()
    sell_strike = atm - util.round_to_nearest(x=0.0075 * atm, base=100)
    if instrument_name == "NIFTY":
        buy_strike = sell_strike + 400
    elif instrument_name == "BANKNIFTY":
        buy_strike = sell_strike + 500
    util.notify(f"ST Strike: {atm}, SELL Strike: {sell_strike}, BUY Strike: {buy_strike}, Instrument Close: {instrument_close}",slack_client=slack_client)
    sell_strike_symbol, expiry = get_option_symbol(sell_strike, option_type)
    buy_strike_symbol, expiry = get_option_symbol(buy_strike, option_type)
    print(expiry)
    expiry = str(expiry)
    expiry = parser.parse(expiry).date()
    print(expiry)
    buy_order = place_buy_order(buy_strike_symbol, quantity)
    if buy_order['order_status'] == "COMPLETE":
        sell_order = place_sell_order(sell_strike_symbol, quantity)
    short_option_cost = sell_order['average_traded_price']
    long_option_cost = buy_order['average_traded_price']
    util.notify("created bear call spread!",slack_client=slack_client)
    record_details_in_mongo(sell_strike_symbol, buy_strike_symbol, "Bearish", instrument_close, expiry, short_option_cost, long_option_cost)

def calculate_pnl(quantity, long_entry, long_exit, short_entry, short_exit):
    pnl = float(quantity) * ((float(short_entry) - float(short_exit)) + (float(long_exit) - float(long_entry)))
    return round(pnl, 2)

# @retry(tries=5, delay=5, backoff=2)
def close_active_positions():
    print(f"Closing active positions {instrument_name}")
    util.notify(f"Closing active positions {instrument_name}",slack_client=slack_client)
    active_strategies = strategies.find({'strategy_state': 'active'})
    for strategy in active_strategies:
        buy_order = place_buy_order(strategy['short_option_symbol'], strategy['quantity'])
        util.notify("Short option leg closed",slack_client=slack_client)
        if buy_order['order_status'] == "COMPLETE":
            sell_order = place_sell_order(strategy['long_option_symbol'], strategy['quantity'])
            util.notify("Long option leg closed",slack_client=slack_client)
            strategies.update_one({'_id': strategy['_id']}, {'$set': {'strategy_state': 'closed'}})
            strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_date': str(datetime.datetime.now().date())}})
            strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_time': datetime.datetime.now().strftime('%H:%M')}})
            strategies.update_one({'_id': strategy['_id']}, {'$set': {'short_exit_price': buy_order['average_traded_price']}})
            strategies.update_one({'_id': strategy['_id']}, {'$set': {'long_exit_price': sell_order['average_traded_price']}})
            pnl = calculate_pnl(strategy['quantity'], strategy['long_option_cost'], sell_order['average_traded_price'], strategy['short_option_cost'],buy_order['average_traded_price'])
            util.notify(f"Realized Gains: {round(pnl, 2)}",slack_client=slack_client)
            strategies.update_one({'_id': strategy['_id']}, {'$set': {'pnl': pnl}})
    return

# @retry(tries=5, delay=5, backoff=2)
def get_pnl(strategy, start=None):
    if start is None:
        days_ago = datetime.datetime.now() - timedelta(days=7)
        start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    short_option_cost = edge.get_option_price('NFO', strategy['short_option_symbol'], start, datetime.datetime.today(), 'min')
    long_option_cost = edge.get_option_price('NFO', strategy['long_option_symbol'], start, datetime.datetime.today(), 'min')
    current_pnl = calculate_pnl(strategy['quantity'], strategy['long_option_cost'], long_option_cost, strategy['short_option_cost'], short_option_cost)
    strategies.update_one({'_id': strategy['_id']}, {'$set': {'running_pnl': current_pnl}})
    return current_pnl



# @retry(tries=5, delay=5, backoff=2)
def main():
    util.notify(f"{instrument_name} Positional bot kicked off",slack_client=slack_client)
    print(f"{instrument_name} Positional bot kicked off")
    util.notify(f"Supertrend Direction: {get_supertrend_direction()}", slack_client=slack_client)
    util.notify(f"Supertrend Value: {get_supertrend_value()}", slack_client=slack_client)
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
                util.notify(message=f"{instrument_name} Weekly option Selling bot is Alive!", slack_client=slack_client)
                util.notify(message=f"current time from {instrument_name}WeeklyOptionSelling: {current_time}", slack_client=slack_client)
                # Update the last notification time
                last_notification_time = notification_time
                
            print(f"current time: {current_time}")
            if current_time > trade_start_time:
                print("Trading Window is active.")
                if strategies.count_documents({'strategy_state': 'active'}) > 0:
                    active_strategies = strategies.find(
                        {'strategy_state': 'active'})
                    for strategy in active_strategies:
                        pnl = get_pnl(strategy, start)
                        if strategy['max_pnl_reached'] < pnl:
                            strategies.update_one({'_id': strategy['_id']}, {'$set': {'max_pnl_reached': pnl}})
                            strategies.update_one({'_id': strategy['_id']}, {'$set': {'trailing_stop_loss': strategy['stop_loss'] + pnl}})

                        if strategy['min_pnl_reached'] > pnl:
                            strategies.update_one({'_id': strategy['_id']}, {'$set': {'min_pnl_reached': pnl}})
                        
                        if pnl <= strategy['trailing_stop_loss']:
                            util.notify(f"SL HIT! Current PnL: {pnl}",slack_client=slack_client, slack_channel=slack_channel)
                            close_active_positions()
                            time.sleep(60)
                            break

                        if strategy['trend'] != get_supertrend_direction():
                            util.notify(f"Supertrend Direction Changed to {get_supertrend_direction()}",slack_client=slack_client)
                            close_active_positions()
                            time.sleep(60)
                            break

                        if  pnl > 0.9 * strategy['total_credit_received']:
                            util.notify("90% premium decayed! Closing positions",slack_client=slack_client)
                            close_active_positions()
                            time.sleep(60)
                            break

                        print(str(datetime.datetime.now().date()))
                        if current_time > datetime.time(hour=11, minute=45) and strategy['expiry'] == str(datetime.datetime.now().date()):
                            util.notify("Rolling over positions to next expiry",slack_client=slack_client)
                            close_active_positions()
                            break
                else:
                    if get_supertrend_direction() == 'Bullish' and get_prev_supertrend_direction() == 'Bullish' and get_coloumn_color() == "green" and get_DTB() == True and get_instrument_close() < (get_supertrend_value() + (.0125 * get_supertrend_value())):
                        create_bull_put_spread()
                    elif get_supertrend_direction() == 'Bearish' and get_prev_supertrend_direction() == 'Bearish' and get_coloumn_color() == "red" and get_DBS() == True and get_instrument_close() > (get_supertrend_value() - (.0125 * get_supertrend_value())):
                        create_bear_call_spread()
                    else:
                        print("waiting for a Pullback to create new positions!")
        except Exception as e:
            util.notify(f"Exception occurred: {str(e)}", slack_client=slack_client, slack_channel=slack_channel)
        
        if current_time > trade_end_time:
            util.notify("Closing Bell, Bot will exit now",slack_client=slack_client)
            return   
        time.sleep(10)
if __name__ == "__main__":
    main()
