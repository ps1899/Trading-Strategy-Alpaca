import logging
from time import *
import alpaca_trade_api as tradeapi
import pandas as pd

# init
logging.basicConfig(
	filename='errlog.log',
	level=logging.WARNING,
	format='%(asctime)s:%(levelname)s:%(message)s',
)

# importing all the necessary Alpaca API tools
api_key = 'PKOUI8V8*****WEKKYDT'
api_secret = 'j8afLqWGhjReFTT9*****OecZTGc5SBT9TaU3we9'
base_url = 'https://paper-api.alpaca.markets'
data_url = 'wss://data.alpaca.markets'

# instantiate REST API
api = tradeapi.REST(api_key, api_secret, base_url, api_version = 'v2')

# init WebSocket
conn = tradeapi.stream2.StreamConn(
	api_key,
	api_secret,
	base_url=base_url,
	data_url=data_url,
	data_stream='alpacadatav1',
)

# to let us know how many seconds are left until the market closes
def time_to_market_close():
    clock = api.get_clock()
    return (clock.next_close - clock.timestamp).total_seconds()

# this puts the script to sleep until market reopens again
def wait_for_market_open():
	clock = api.get_clock()
	if not clock.is_open:
		time_to_open = (clock.next_open - clock.timestamp).total_seconds()
		sleep(round(time_to_open))

# setting the trade parametes, setting high as all time high of last 10 rows. Similarly, setting low as all time low of last 10 rows. 
def set_trade_params(df):
    return {
		'high': df.high.tail(10).max(),
		'low': df.low.tail(10).min(),
		'trade_taken': False,
	}

# start sending the order by checking whether market is open or not
def send_order(direction, bar):
    if time_to_market_close() > 120:
        print(f'sent {direction} trade')
        range_size = (trade_params['high'] - trade_params['low'])
        
        if(direction == 'buy'):
            sl = bar.high - range_size
            tp = bar.high + range_size
        elif(direction == 'sell'):
            sl = bar.low + range_size
            tp = bar.low - range_size
            
            api.submit_order(
            symbol = 'AAPL',
            qty = 100,
            side = direction,
            type = 'market',
            time_in_force = 'day',
            order_class = 'bracket',
            stop_loss = dict(stop_price = str(sl)),
            take_profit = dict(limit_price = str(tp)),
        )
        return True
    else:
        wait_for_market_open()
        return False

# websocket callback fxn1
@conn.on(r'^AM.AAPL$')
async def on_minute_bars(conn, channel, bar):
	if isinstance(candlesticks.df, pd.DataFrame):
		ts = pd.to_datetime(bar.timestamp, unit='ms')
		candlesticks.df.loc[ts] = [bar.open, bar.high, bar.low, bar.close, bar.volume]

	if not trade_params['trade_taken']:
		if bar.high > trade_params['high']:
			trade_params['trade_taken'] = send_order('buy', bar)

		elif bar.low < trade_params['low']:
			trade_params['trade_taken'] = send_order('sell', bar)

	if time_to_market_close() > 120:
		wait_for_market_open()

# websocket callback fxn2
@conn.on(r'^trade_updates$')
async def on_trade_updates(conn, channel, trade):
	if trade.order['order_type'] != 'market' and trade.order['filled_qty'] == '100':
		# trade closed - look for new trade
		trade_params = set_trade_params(candlesticks.df.AAPL)

# Main driver caller
candlesticks = api.get_barset('AAPL', 'minute', limit=10)
trade_params = set_trade_params(candlesticks.df.AAPL)
conn.run(['AM.AAPL', 'trade_updates'])
