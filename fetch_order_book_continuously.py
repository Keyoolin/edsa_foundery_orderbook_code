# -*- coding: utf-8 -*-

import asyncio
import os
import sys
import pandas as pd
import numpy
from datetime import datetime

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

import ccxt.async_support as ccxt  # noqa: E402


async def main(exchange, symbol):
    
    try:
        while True:
            print('--------------------------------------------------------------')
            print(exchange.iso8601(exchange.milliseconds()), 'fetching', symbol, 'ticker from', exchange.name)
            # this can be any call really
            ticker = await exchange.fetch_order_book(symbol, limit = 200)
            print(exchange.iso8601(exchange.milliseconds()), 'fetched', symbol, 'ticker from', exchange.name)

            orderbook = pd.DataFrame(data=ticker)
            orderbook['timestamp']= orderbook['timestamp'].fillna(exchange.iso8601(exchange.milliseconds()))
            orderbook['bids_price']=orderbook.bids.apply( lambda x :x[0])
            orderbook['bids_volume']=orderbook.bids.apply(lambda x :x[1])
            orderbook['asks_price']=orderbook.asks.apply(lambda x :x[0])
            orderbook['asks_volume']=orderbook.asks.apply(lambda x :x[1])
            orderbook.drop(['bids','asks','datetime', 'nonce'],axis=1,inplace=True)
            orderbook.to_csv('order_book-{}.csv'.format((datetime.now()).strftime('%Y %m %d %H %M %S')),index=False)
            
            print(exchange.iso8601(exchange.milliseconds()), 'stored', symbol, 'ticker from', exchange.name)
            
    except KeyboardInterrupt:
        exchange.close()
        print('interrupted!')
        
        
# you can set enableRateLimit = True to enable the built-in rate limiter
# this way you request rate will never hit the limit of an exchange
# the library will throttle your requests to avoid that

exchange = ccxt.bitmex({
    'enableRateLimit': True,  # this option enables the built-in rate limiter
})

try:
    asyncio.get_event_loop().run_until_complete(main(exchange, 'BTC/USD'))
    
except KeyboardInterrupt:
        exchange.close()
        print('interrupted!')

