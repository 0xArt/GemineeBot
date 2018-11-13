import time
import json
import hmac
import base64
import hashlib
import requests
import linecache


class CryptoCompare(object):

    def __init__(self):
        '''Initialize the class.'''

        self.base_url = 'https://min-api.cryptocompare.com'

    def minuteHist(self, exchange, pair, base, interval, limit):
        """
        Send a trade history request, return the response.

        Arguements:
        symbol -- currency symbol (default 'btcusd')
        limit_trades -- maximum number of trades to return (default 50)
        timestamp -- only return trades after this unix timestamp (default 0)
        """

        url = self.base_url + '/data/histominute'
        params = {
            'fsym': pair,
            'tsym': base,
            'limit': limit,
            'aggregate': interval,
            'e': exchange
        }
        return requests.get(url, params)

    def hourHist(self, exchange, pair, base, interval, limit):
        """
        Send a trade history request, return the response.

        Arguements:
        symbol -- currency symbol (default 'btcusd')
        limit_trades -- maximum number of trades to return (default 50)
        timestamp -- only return trades after this unix timestamp (default 0)
        """

        url = self.base_url + '/data/histohour'
        params = {
            'fsym': pair,
            'tsym': base,
            'limit': limit,
            'aggregate': interval,
            'e': exchange
        }
        return requests.get(url, params)

    def book(self, symbol, limit=20):
        url = self.base_url + '/api/v1/depth'
        params = {
            'symbol': symbol,
            'limit': limit
        }
        return requests.get(url, params)


class Gemini(object):

    """
    A class to make requests to the Gemini API.

    Make public or authenticated requests according to the API documentation:
    https://docs.gemini.com/
    """

    def __init__(self):
        """
        Initialize the class.
        
        Arguments:
        api_key -- your Gemini API key
        secret_key -- your Gemini API secret key for signatures
        live -- use the live API? otherwise, use the sandbox (default False)
        """
        live = str(linecache.getline('settings.cfg', 11)).rstrip("\n\r")
        if (live == 'true'):
            self.api_key = linecache.getline('settings.cfg', 3).rstrip("\n\r")
            self.secret_key = linecache.getline('settings.cfg', 5).rstrip("\n\r")
            self.base_url = 'https://api.gemini.com'
        else:
            self.api_key = linecache.getline('settings.cfg', 7).rstrip("\n\r")
            self.secret_key = linecache.getline('settings.cfg', 9).rstrip("\n\r")
            self.base_url = 'https://api.sandbox.gemini.com'


    # public requests
    def symbols(self):
        """Send a request for all trading symbols, return the response."""
        url = self.base_url + '/v1/symbols'

        return requests.get(url)

    def book(self, symbol, limit_bids=0, limit_asks=0):
        """
        Send a request to get the public order book, return the response.

        Arguments:
        symbol -- currency symbol (default 'btcusd')
        limit_bids -- limit the number of bids returned (default 0)
        limit_asks -- limit the number of asks returned (default 0)
        """
        url = 'https://api.gemini.com/v1/book/ethbtc'
        params = {
            'limit_bids': limit_bids,
            'limit_asks': limit_asks
        }

        return requests.get(url, params)

    def trades(self, symbol='btceth', since=0, limit_trades=50,
               include_breaks=0):
        """
        Send a request to get all public trades, return the response.

        Arguments:
        symbol -- currency symbol (default 'btcusd')
        since -- only return trades after this unix timestamp (default 0)
        limit_trades -- maximum number of trades to return (default 50).
        include_breaks -- whether to display broken trades (default False)
        """
        url = self.base_url + '/v1/trades/' + symbol
        params = {
            'since': since,
            'limit_trades': limit_trades,
            'include_breaks': include_breaks
        }

        return requests.get(url, params)

    # authenticated requests
    def newOrder(self, amount, price, side, client_order_id=None,
                  symbol='btcusd', type='exchange limit'):
        """
        Send a request to place an order, return the response.

        Arguments:
        amount -- quoted decimal amount of BTC to purchase
        price -- quoted decimal amount of USD to spend per BTC
        side -- 'buy' or 'sell'
        client_order_id -- an optional client-specified order id (default None)
        symbol -- currency symbol (default 'btcusd')
        type -- the order type (default 'exchange limit')
        """
        request = '/v1/order/new'
        url = self.base_url + request
        params = {
            'request': request,
            'nonce': self.getNonce(),
            'symbol': symbol,
            'amount': amount,
            'price': price,
            'side': side,
            'type': type
        }

        if client_order_id is not None:
            params['client_order_id'] = client_order_id
        print(params)
        print(self.prepare(params))
        return requests.post(url, headers=self.prepare(params))

    def cancelOrder(self, order_id):
        """
        Send a request to cancel an order, return the response.

        Arguments:
        order_id - the order id to cancel
        """
        request = '/v1/order/cancel'
        url = self.base_url + request
        params = {
            'request': request,
            'nonce': self.getNonce(),
            'order_id': order_id
        }

        return requests.post(url, headers=self.prepare(params))

    def cancelSession(self):
        """Send a request to cancel all session orders, return the response."""
        request = '/v1/order/cancel/session'
        url = self.base_url + request
        params = {
            'request': request,
            'nonce': self.getNonce()
        }

        return requests.post(url, headers=self.prepare(params))

    def cancel_all(self):
        """Send a request to cancel all orders, return the response."""
        request = '/v1/order/cancel/all'
        url = self.base_url + request
        params = {
            'request': request,
            'nonce': self.getNonce()
        }

        return requests.post(url, headers=self.prepare(params))

    def orderStatus(self, order_id):
        """
        Send a request to get an order status, return the response.

        Arguments:
        order_id -- the order id to get information on
        """
        request = '/v1/order/status'
        url = self.base_url + request
        params = {
            'request': request,
            'nonce': self.getNonce(),
            'order_id': order_id
        }

        return requests.post(url, headers=self.prepare(params))

    def activeOrders(self):
        """Send a request to get active orders, return the response."""
        request = '/v1/orders'
        url = self.base_url + request
        params = {
            'request': request,
            'nonce': self.getNonce()
        }
        print(params)
        print(self.prepare(params))
        return requests.post(url, headers=self.prepare(params))

    def pastTrades(self, symbol='ethusd', limit_trades=50, timestamp=0):
        """
        Send a trade history request, return the response.

        Arguements:
        symbol -- currency symbol (default 'btcusd')
        limit_trades -- maximum number of trades to return (default 50)
        timestamp -- only return trades after this unix timestamp (default 0)
        """
        request = '/v1/mytrades'
        url = self.base_url + request
        params = {
            'request': request,
            'nonce': self.getNonce(),
            'symbol': symbol,
            'limit_trades': limit_trades,
            'timestamp': timestamp
        }

        return requests.post(url, headers=self.prepare(params))

    def ticker(self, symbol):
        """
        Send a trade history request, return the response.

        Arguements:
        symbol -- currency symbol (default 'btcusd')
        limit_trades -- maximum number of trades to return (default 50)
        timestamp -- only return trades after this unix timestamp (default 0)
        """
        url = "https://api.gemini.com/v1/pubticker/" + symbol
        return requests.get(url)

    def pastOrders(self):
        """
        Send a trade history request, return the response.

        Arguements:
        symbol -- currency symbol (default 'btcusd')
        limit_trades -- maximum number of trades to return (default 50)
        timestamp -- only return trades after this unix timestamp (default 0)
        """
        # date = datetime.date(2015,1,1).strftime("%s")
        url = "https://api.gemini.com/v1/trades/ethbtc?limit_trades=500"
        return requests.get(url)

    def dailyOrders(self, timestamp):
        """
        Send a trade history request, return the response.

        Arguements:
        symbol -- currency symbol (default 'btcusd')
        limit_trades -- maximum number of trades to return (default 50)
        timestamp -- only return trades after this unix timestamp (default 0)
        """
        # date = datetime.date(2015,1,1).strftime("%s")
        url = "https://min-api.cryptocompare.com/data/pricehistorical?fsym=ETH&tsyms=BTC&ts=" + \
            timestamp + "&markets=gemini"
        return requests.get(url)

    def balances(self):
        """Send an account balance request, return the response."""
        request = '/v1/balances'
        url = self.base_url + request
        params = {
            'request': request,
            'nonce': self.getNonce()
        }

        return requests.post(url, headers=self.prepare(params))

    def heartbeat(self):
        """Send a heartbeat message, return the response."""
        request = '/v1/heartbeat'
        url = self.base_url + request
        params = {
            'request': request,
            'nonce': self.getNonce()
        }

        return requests.post(url, headers=self.prepare(params))

    def getNonce(self):
        """Return the current millisecond timestamp as the nonce."""
        return int(round(time.time() * 1000))

    def prepare(self, params):
        """
        Prepare, return the required HTTP headers.

        Base 64 encode the parameters, sign it with the secret key,
        create the HTTP headers, return the whole payload.

        Arguments:
        params -- a dictionary of parameters
        """
        jsonparams = json.dumps(params)
        payload = base64.b64encode(jsonparams)
        signature = hmac.new(self.secret_key, payload,
                             hashlib.sha384).hexdigest()

        return {'X-GEMINI-APIKEY': self.api_key,
                'X-GEMINI-PAYLOAD': payload,
                'X-GEMINI-SIGNATURE': signature}




