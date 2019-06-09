#!/usr/bin/env python
"""
triarbot: Simple triangular arbitrage bot
Python 3+
(C) 2018 SurgeonY, Planet Earth

Donate ETH: 0xFA745708C435300058278631429cA910AE175d52
Donate BTC: 16KqCc4zxEWf7CaerWNZdGYwyuU33qDzCv
"""

import logging
from typing import Dict, Callable, Tuple, List
from decimal import Decimal
import abc
from exchanges.exmo_exchange import Ticker
from triarbstrat.tri_arb_trader import PairAndRate, OrderSide, OrderType


class TriangularArbitrageIndicator:
    """
    Tracks price changes and looks for arbitrage opportunities between the currency pairs starting from the specified
    Quote Currency. E.g. triangular arbitrage expected loop would be BTC/USD->ETH/BTC->ETH/USD.
    Indicator outputs potential profit gain in base currency rate (first pair).
    Once opportunity appears it signals to arbitrage trader to start a trading round.
    For prices it tracks current spread, highest ask and lowest bid from order book (taken from ticker).
    It accounts for different order types and fees, supports Market and Limit.
    """

    def __init__(self, quote_curr: str, currencies: List, fees: Dict[str, Decimal], order_type: OrderType,
                 gain_min_limit: Decimal, logger: logging.Logger):
        self.quote_curr = quote_curr  # curr against which P&L is tracked, and tri arb loops open and close
        self.fees = fees  # get from exchange (0.2% on exmo)
        self.order_type = order_type
        self.gain_min_limit = gain_min_limit
        self.logger = logger if logger else logging.getLogger(__name__)

        self._signal_handler: Callable[[PairAndRate, PairAndRate, PairAndRate, Decimal, OrderType], None] = None
        self.arb_opps: Dict[str, Tuple[PairAndRate, PairAndRate, PairAndRate, Decimal]] = {}
        # list of currencies between which we're doing arbitrage given available markets/pairs on exchange
        # remove xem, smart, qtum, neo
        # TODO implement currency selection based on stats 24h volume, take most liquid e.g those which Volume>20 BTC

        self.trading_currencies = currencies

        fee_type = 'maker' if order_type == OrderType.MARKET else 'taker'
        self.arb_calculator: AbstractArbCalculator = MarketOrderArbCalculator(fees[fee_type]) \
            if order_type == OrderType.MARKET else LimitOrderArbCalculator(fees[fee_type], Decimal('0.0005'))

        self.logger.info('Starting TriArb Indicator on %s, %s orders, fees: %s=%s', quote_curr, order_type, fee_type,
                         fees[fee_type])

    def update(self, tickers: Dict[str, Ticker]):
        self.arb_calculator.set_tickers(tickers)
        max_gain = Decimal('0')
        max_path = ''

        # determine arbitrage opportunities
        # iterate over all currencies and check all loops starting from and ending with the quote curr
        # via two other currencies
        for curr1 in self.trading_currencies:
            if curr1 == self.quote_curr:  # skip e.g. USD
                continue

            # First: buy curr1 with quote_curr (or sell quote_curr for curr1)
            curr1_quote = self.arb_calculator.get_pair_and_rate(curr1, self.quote_curr)
            if not curr1_quote:
                continue

            for curr2 in self.trading_currencies:
                if curr2 == self.quote_curr or curr2 == curr1:  # e.g. skip USD and BTC
                    continue

                # Second: buy curr2 for curr1 (or sell curr1 for curr2)
                curr2_curr1 = self.arb_calculator.get_pair_and_rate(curr2, curr1)
                if not curr2_curr1:
                    continue

                # Third: selling curr2 for quote curr... (or equivalent is buying our quote curr for curr2)
                quote_curr2 = self.arb_calculator.get_pair_and_rate(self.quote_curr, curr2)
                if not quote_curr2:
                    continue

                gain = curr2_curr1.calc_rate * quote_curr2.calc_rate - 1 / curr1_quote.calc_rate

                if gain > 0:
                    path = curr1_quote.pair + '>' + curr2_curr1.pair + '>' + quote_curr2.pair
                    self.arb_opps[path] = (curr1_quote, curr2_curr1, quote_curr2, gain)  # consider named tuple dto?

                    self.logger.debug('%s orders, gain: %s, path: %s, rates: %s', self.order_type, gain,
                                      path, self.arb_opps[path])

                    if gain > max_gain:
                        max_gain, max_path = gain, path

        if max_gain > 0:  # self.gain_min_limit:
            self.logger.info('%s orders Arb opportunity, %s gain=%s, %s: %s>%s>%s',
                             self.order_type,
                             self.quote_curr,
                             max_gain, max_path,
                             self.arb_opps[max_path][0].order_rate,
                             self.arb_opps[max_path][1].order_rate,
                             self.arb_opps[max_path][2].order_rate,
                             )

            self._signal_arbitrage(max_path, self.quote_curr)
        else:
            self.logger.debug('%s orders, %s, No arb opportunities', self.order_type, self.quote_curr)

    def _signal_arbitrage(self, arb_path, quote_curr):
        # signal to trader, which does all the subsequent arb trading round trip
        if self._signal_handler:
            pair1, pair2, pair3, gain = self.arb_opps[arb_path]
            self._signal_handler(pair1, pair2, pair3, gain, self.order_type, quote_curr)

    def register_signal_handler(self,
                                signal_handler: Callable[[PairAndRate, PairAndRate, PairAndRate, Decimal], None]
                                ) -> None:
        # TODO later if needed change to listener array or even consider moving to RxPy, Observables etc
        self._signal_handler = signal_handler

    def unregister_signal_handler(self, handler):
        if handler == self._signal_handler:
            self._signal_handler = None


class AbstractArbCalculator(abc.ABC):
    """
    Helper strategy to calculate rates and pairs and gains when arbitrages is going to be done with
    different order types - Market or Limit orders.
    1. With limit orders we use buy_price or sell_price from ticker - lower and upper spread boundaries and account for
    exchange fees.
    2. With Market orders we effectively buy at sell price and vice versa, i.e. on opposite sides of spread and
    take into account fees accordingly.
    """
    def __init__(self, fee: Decimal):
        self.fee = fee
        self.tickers: Dict[str, Ticker] = {}

    @abc.abstractmethod
    def get_pair_and_rate(self, curr_a: str, curr_b: str) -> PairAndRate:
        pass

    def set_tickers(self, tickers: Dict[str, Ticker]):
        self.tickers = tickers


class LimitOrderArbCalculator(AbstractArbCalculator):
    def __init__(self, fee: Decimal, rate_offset: Decimal):
        self.rate_offset = rate_offset
        super().__init__(fee)

    def get_pair_and_rate(self, curr_a, curr_b):
        # both combinations where curr_a is base curr or quote are considered
        # we look for buying curr_a for curr_b (or the same is selling curr_b for curr_a)
        pair_ab = curr_a + '_' + curr_b  # e.g. BTC_USD
        pair_ba = curr_b + '_' + curr_a  # e.g. USD_BTC

        # taking prices 0.05% higher than buy or lower than sell, offset inwards current spread,
        # for more likely order execution
        if pair_ab in self.tickers:
            return PairAndRate(pair_ab,
                               calc_rate=(1 - self.fee) / self.tickers[pair_ab].buy_price / (1 + self.rate_offset),
                               order_rate=self.tickers[pair_ab].buy_price * (1 + self.rate_offset),
                               side=OrderSide.BUY)
        elif pair_ba in self.tickers:
            return PairAndRate(pair_ba,
                               calc_rate=(1 - self.fee) * self.tickers[pair_ba].sell_price * (1 - self.rate_offset),
                               order_rate=self.tickers[pair_ba].sell_price * (1 - self.rate_offset),
                               side=OrderSide.SELL)
        else:
            return None  # no such pair on exchange


class MarketOrderArbCalculator(AbstractArbCalculator):

    def get_pair_and_rate(self, curr_a, curr_b):
        # both combinations where curr_a is base curr or quote_curr are considered
        # we look for buying curr_a for curr_b (or selling curr_b for curr_a),
        # in a market order we buy effectively at sell_price (and sell at buy_price), on the opposite side of spread
        pair_ab = curr_a + '_' + curr_b  # e.g. BTC_USD
        pair_ba = curr_b + '_' + curr_a  # e.g. USD_BTC
        if pair_ab in self.tickers:
            return PairAndRate(pair_ab,
                               calc_rate=(1 - self.fee) / self.tickers[pair_ab].sell_price,
                               order_rate=self.tickers[pair_ab].sell_price,
                               side=OrderSide.BUY)
        elif pair_ba in self.tickers:
            return PairAndRate(pair_ba,
                               calc_rate=(1 - self.fee) * self.tickers[pair_ba].buy_price,
                               order_rate=self.tickers[pair_ba].buy_price,
                               side=OrderSide.SELL)
        else:
            return None
