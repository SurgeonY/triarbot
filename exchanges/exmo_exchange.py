#!/usr/bin/env python
"""
triarbot: Simple triangular arbitrage bot
Python 3+
(C) 2018 SurgeonY

Donate ETH: 0xFA745708C435300058278631429cA910AE175d52
Donate BTC: 16KqCc4zxEWf7CaerWNZdGYwyuU33qDzCv
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict
import logging
from typing import NamedTuple
from exchange_apis.exmo_api import ExmoError


class Ticker(NamedTuple):
    high: Decimal
    low: Decimal
    avg: Decimal
    vol: Decimal
    vol_curr: Decimal

    last_trade: Decimal
    buy_price: Decimal
    sell_price: Decimal
    updated: datetime

    def __str__(self):
        return "High: " + str(self.high) + \
               ", low: " + str(self.low) + \
               ", avg: " + str(self.avg) + \
               ", vol: " + str(self.vol) + \
               ", vol_curr: " + str(self.vol_curr) + \
               ", last_trade: " + str(self.last_trade) + \
               ", buy_price: " + str(self.buy_price) + \
               ", sell_price: " + str(self.sell_price) + \
               ", updated: " + str(self.updated)


# TODO consider CCXT lib, extract exchange interface and make pluggable exchanges
class ExmoExchange:

    def __init__(self, exmo_api, logger, ):
        self.exmo_api = exmo_api
        self.logger = logger.getChild(__name__) if logger else logging.getLogger(__name__)
        self.pair_settings = {}
        self.fees = {'maker': Decimal('0.002'), 'taker': Decimal('0.002')}  # plain fee 0.02% per deal

    def place_limit_buy(self, pair: str, quantity: Decimal, price: Decimal) -> int:
        return self.place_order(pair, quantity, price, 'buy')

    def place_limit_sell(self, pair: str, quantity: Decimal, price: Decimal) -> int:
        return self.place_order(pair, quantity, price, 'sell')

    def place_market_buy(self, pair: str, quantity: Decimal) -> int:
        return self.place_order(pair, quantity, Decimal('0'), 'market_buy')

    def place_market_sell(self, pair: str, quantity: Decimal) -> int:
        return self.place_order(pair, quantity, Decimal('0'), 'market_sell')

    def place_market_buy_total(self, pair: str, amount: Decimal) -> int:
        return self.place_order(pair, amount, Decimal('0'), 'market_buy_total')

    def place_market_sell_total(self, pair: str, amount: Decimal) -> int:
        return self.place_order(pair, amount, Decimal('0'), 'market_sell_total')

    def place_order(self, pair: str, quantity: Decimal, price: Decimal, ord_type: str) -> int:
        """
        :param pair: - currency pair
        :param quantity: - quantity for the order
        :param price: - price for the order
        :param ord_type: - type of order, can have the following values:
                buy - buy order
                sell - sell order
                market_buy - market buy-order
                market_sell - market sell-order
                market_buy_total - market buy-order for a certain amount
                market_sell_total - market sell-order for a certain amount
        :return:
                { "result": true,
                  "error": "",
                  "order_id": 123456
                }
                Fields description:
                result - 'true' in case of successful creation and 'false' in case of an error
                error - contains the text of the error
                order_id - order identifier
        """

        params = {
            "pair": pair,
            'quantity': str(quantity),
            'price': str(price),
            'type': ord_type}

        self.logger.info('Placing order: %s', params)
        try:
            result = self.exmo_api.api_query("order_create", params)

            # result['error'] is handled in exmo_api, will get an exception with error msg
            order_id = int(result['order_id'])
            self.logger.info('Order placed: %s:%s', str(order_id), params)
            return order_id
        except Exception as e:
            self.logger.error(e.args)
            raise ExmoError("Error placing order: ", e.args)

    def cancel_order(self, order_id):
        self.logger.debug("Canceling order: %s", order_id)

        params = {"order_id": order_id}
        response = self.exmo_api.api_query("order_cancel", params)

        if not response['result']:
            msg = "Error canceling order " + order_id + ", msg: " + response['error']
            self.logger.error(msg)
            raise ExmoError(msg, order_id)

    def get_order_trades(self, order_id):
        """
        Getting the history of deals with the order
        :param order_id: - order identifier
        :return: {
                  "type": "buy",            // type – type of order
                  "in_currency": "BTC",     // in_currency – incoming currency
                  "in_amount": "1",         // in_amount - amount of incoming currency
                  "out_currency": "USD",    // out_currency - outcoming currency
                  "out_amount": "100",      // out_amount - amount of outcoming currency
                  "trades": [               // trades - deals array where the values mean the following:
                    {
                      "trade_id": 3,        // trade_id - deal identifier
                      "date": 1435488248,   // date - date of the deal
                      "type": "buy",        // type - type of the deal
                      "pair": "BTC_USD",    // pair - currency pair
                      "order_id": 12345,    // order_id - order identifier
                      "quantity": 1,        // quantity - currency quantity
                      "price": 100,         // price - deal price
                      "amount": 100         // amount - sum of the deal
                    }
                  ]
                }
        """
        self.logger.debug("Getting trades for order: %s", order_id)

        params = {"order_id": order_id}
        response = self.exmo_api.api_query("order_trades", params)
        return response

    def get_user_trades(self, pair: str, offset: int = 0, limit: int = 100):
        """
        :param pair: one or various currency pairs separated by commas (example: BTC_USD,BTC_EUR)
        :param offset: last deal offset (default: 0)
        :param limit: the number of returned deals (default: 100, мmaximum: 10 000)
        :return:
        """
        self.logger.debug("Getting trades for the user")

        params = {"pair": pair, "offset": offset, "limit": limit}
        response = self.exmo_api.api_query("user_trades", params)
        return response

    def get_user_open_orders(self):
        """
        Getting the list of user's active orders
        :return:
                {
                  "BTC_USD": [
                    {
                      "order_id": "14",
                      "created": "1435517311",
                      "type": "buy",
                      "pair": "BTC_USD",
                      "price": "100",
                      "quantity": "1",
                      "amount": "100"
                    }
                  ]
                }
        """
        self.logger.debug("Getting user open orders")

        response = self.exmo_api.api_query("user_open_orders")
        return response

    def get_user_cancelled_orders(self, offset: int = 0, limit: int = 100):
        params = {"offset": offset, "limit": limit}
        self.logger.debug("Getting user canceled orders: %s", params)

        response = self.exmo_api.api_query("user_cancelled_orders", params)
        return response

    def get_ticker(self, pair=None) -> Dict[str, Ticker]:
        """
        Returns ticker for specific currency pair or all tradable.
        :param pair:
        by default if none specified returns all tradable pairs, otherwise returns particular ticker info,
        should be specified in exmo format like 'BTC_USD'
        :return:
        dict or array of dictionaries with Ticker fields:
            high - maximum deal price within the last 24 hours
            low - minimum deal price within the last 24 hours
            avg - average deal price within the last 24 hours
            vol - the volume of deals within the last 24 hours
            vol_curr - the total value of all deals within the last 24 hours

            last_trade - last deal price
            buy_price - current maximum buy price
            sell_price - current minimum sell price
            updated - date and time of data update
        """

        try:
            response = self.exmo_api.api_query('ticker')

            if pair and response[pair]:
                _t = response[pair]
                ticker = Ticker(
                    Decimal(_t["high"]),
                    Decimal(_t["low"]),
                    Decimal(_t["avg"]),
                    Decimal(_t["vol"]),
                    Decimal(_t["vol_curr"]),

                    Decimal(_t["last_trade"]),
                    Decimal(_t["buy_price"]),
                    Decimal(_t["sell_price"]),
                    from_timestamp(_t["updated"])
                )

                self.logger.debug("Ticker received - %s",  ticker)
                return ticker
            else:
                for pair in response:
                    _t = response[pair]
                    ticker = Ticker(
                        Decimal(_t["high"]),
                        Decimal(_t["low"]),
                        Decimal(_t["avg"]),
                        Decimal(_t["vol"]),
                        Decimal(_t["vol_curr"]),

                        Decimal(_t["last_trade"]),
                        Decimal(_t["buy_price"]),
                        Decimal(_t["sell_price"]),
                        from_timestamp(_t["updated"])
                    )
                    response[pair] = ticker

                return response

        except BaseException as e:
            self.logger.error("Error getting ticker: %s", e, exc_info=1)
            raise e

    def get_order_book(self, pair: str, limit: int = 100) -> Dict:
        """
        The book of current open orders on the currency pair
        :param pair: one or various currency pairs separated by commas (example: BTC_USD,BTC_EUR)
        :param limit: the number of displayed positions (default: 100, max: 1000)
        :return:
                {
                  "BTC_USD": {
                    "ask_quantity": "3",
                    "ask_amount": "500",
                    "ask_top": "100",
                    "bid_quantity": "1",
                    "bid_amount": "99",
                    "bid_top": "99",
                    "ask": [[100,1,100],[200,2,400]],
                    "bid": [[99,1,99]]
                    ]
                  }
                }
                Fields description:
                ask_quantity - the sum of all quantity values in sell orders
                ask_amount - the sum of all total sum values in sell orders
                ask_top - minimum sell price
                bid_quantity - the sum of all quantity values in buy orders
                bid_amount - the sum of all total sum values in buy orders
                bid_top - maximum buy price
                bid - the list of buy orders where every field is: price, quantity and amount
                ask - the list of sell orders where every field is: price, quantity and amount
        """

        params = {"pair": pair, "limit": limit}
        self.logger.debug("Getting order book for pair: %s", pair)

        response = self.exmo_api.api_query("order_book", params)
        return response

    def get_markets(self):
        """
        :return: list of currency pairs available for trading on Exmo
        """
        if not self.pair_settings:    # lazy loading markets
            self.pair_settings = self._get_pair_settings()

        return self.pair_settings.keys()

    def get_fees(self):
        return self.fees

    def get_currencies(self):
        """
        Currencies list
        :return:
                ["USD", "EUR", "RUB", "BTC", "DOGE", "LTC"]
        """
        response = self.exmo_api.api_query('currency')
        self.logger.debug('Currencies received: %s', str(response))
        return response

    def _get_pair_settings(self):
        """
        :return:
            Dict with settings by pair:
            {
              "BTC_USD": {
                "min_quantity": "0.001",
                "max_quantity": "100",
                "min_price": "1",
                "max_price": "10000",
                "max_amount": "30000",
                "min_amount": "1"
              }
            }
            Fields description:
            min_quantity - minimum quantity for the order
            max_quantity - maximum quantity for the order
            min_price - minimum price for the order
            max_price - maximum price for the order
            min_amount - minimum total sum for the order
            max_amount - maximum total sum for the order
        """
        response = self.exmo_api.api_query('pair_settings')
        self.logger.debug('Pair settings received %s', str(response))
        return response


def from_timestamp(s):
    """
    Util method to convert from unix time to normal datetime representation
    :param s: int or str with unix time
    :return: datetime
    """
    return datetime.fromtimestamp(int(s), timezone.utc)
