#!/usr/bin/env python
"""
triarbot: Simple triangular arbitrage bot
Python 3+
(C) 2018 SurgeonY

Donate ETH: 0xFA745708C435300058278631429cA910AE175d52
Donate BTC: 16KqCc4zxEWf7CaerWNZdGYwyuU33qDzCv
"""

import logging
from decimal import Decimal
from typing import Dict
import sqlite3
from datetime import datetime
import pathlib
from exchanges.exmo_exchange import ExmoExchange, Ticker
from triarbstrat.tri_arb_indicator import TriangularArbitrageIndicator
from triarbstrat.tri_arb_trader import TriangularArbitrageTrader, PairAndRate, Order, OrderStatus, OrderType, OrderSide

# TODO refactor, extract config to ext store
QUOTE_CURRS = ['USD', 'EUR', 'RUB', 'BTC', 'ETH', 'LTC', 'XRP', 'USDT', 'DASH']
TRADING_AMOUNTS = {'USD': Decimal('5.0'), 'EUR': Decimal('5.0'), 'RUB': Decimal('450.0'), 'BTC': Decimal('0.0015'),
                   'ETH': Decimal('0.03'), 'LTC': Decimal('0.2'), 'XRP': Decimal('32.0'), 'USDT': Decimal('5.0'),
                   'DASH': Decimal('0.125')}
ORDER_TYPE = OrderType.MARKET
PAPER_TRADING = True  # if true, no orders will be submitted to exchange
DEPTH = 40  # order book depth for calculating gain with slippage, for market orders
GAIN_MIN_LIMIT = Decimal('1.0')  # min gain for checking arbitrage round trip
PNL_MIN_LIMIT = 1.0  # min PnL with slippage for starting arbitrage round trip - $0.50
TICKERS_TO_SKIP = 30  # count of ticker requests to skip, while polling, saving space in DB for tickers
POLLING_INTERVAL = 2  # polling exchange for ticker, in seconds

MARKET_DATA_DB = './db/market_data.db'
TRIARB_DATA_DB = './db/triarb_data.db'
pathlib.Path('./db').mkdir(parents=True, exist_ok=True)

MARKET_DATA_DDL = """CREATE TABLE IF NOT EXISTS ticker (
                    pair VARCHAR  NOT NULL,
                    created TIMESTAMP NOT NULL,
                    
                    high DECIMAL NOT NULL,
                    low DECIMAL NOT NULL,
                    avg DECIMAL NOT NULL,
                    vol DECIMAL NOT NULL,
                    vol_curr DECIMAL NOT NULL,
                    
                    last_trade DECIMAL NOT NULL,
                    buy_price DECIMAL NOT NULL,
                    sell_price DECIMAL NOT NULL,
                    updated TIMESTAMP NOT NULL,
                    
                    PRIMARY KEY(pair, created)
                ); 
            """
MD_TICKER_INSERT = "INSERT INTO ticker VALUES(?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?);"

TRIARB_DATA_DDL = """CREATE TABLE IF NOT EXISTS triarb_opportunity (
                    id INTEGER PRIMARY KEY,
                    pair1       TEXT NOT NULL,
                    calc_rate1  DECIMAL NOT NULL,
                    order_rate1 DECIMAL NOT NULL, 
                    pair2       TEXT NOT NULL,
                    calc_rate2  DECIMAL NOT NULL, 
                    order_rate2 DECIMAL NOT NULL, 
                    pair3       TEXT NOT NULL,
                    calc_rate3  DECIMAL NOT NULL, 
                    order_rate3 DECIMAL NOT NULL, 
                    gain        DECIMAL NOT NULL,
                    order_type  TEXT NOT NULL,          --limit or market
                    created     TIMESTAMP NOT NULL
                );
                 
               CREATE TABLE IF NOT EXISTS triarb_order (
                    id                      INTEGER PRIMARY KEY,
                    exch_order_id           INTEGER NOT NULL,
                    triarb_opportunity_id   INTEGER NOT NULL,
                    created                 TIMESTAMP NOT NULL,
                    pair                    TEXT NOT NULL,
                    quantity                DECIMAL NOT NULL,
                    price                   DECIMAL NOT NULL,
                    side                    TEXT NOT NULL,
                    type                    TEXT NOT NULL,  --limit or market
                    status                  TEXT NOT NULL,

                    FOREIGN KEY (triarb_opportunity_id) REFERENCES triarb_opportunity(id)  
               );                
            """
TD_OPP_INSERT = """INSERT INTO triarb_opportunity (
                        pair1, calc_rate1, order_rate1, pair2, calc_rate2, order_rate2, pair3, calc_rate3, 
                        order_rate3, gain, order_type, created)
                   VALUES(:pair1, :calc_rate1, :order_rate1, :pair2, :calc_rate2, :order_rate2, :pair3, :calc_rate3, 
                          :order_rate3, :gain, :order_type, :created); """

TD_ORDER_INSERT = """INSERT INTO triarb_order (exch_order_id, triarb_opportunity_id, created, pair,
                                               quantity, price, side, type, status)
                     VALUES(:exch_order_id, :triarb_opportunity_id, :created, :pair,
                            :quantity, :price, :side, :type, :status); """
TD_ORDER_UPDATE = """UPDATE triarb_order 
                     SET exch_order_id = :exch_order_id, 
                         status = :status 
                     WHERE id = :id; """


class TriangularArbitrageStrategy:
    """
        Read about triangular arbitrage:
            https://corporatefinanceinstitute.com/resources/knowledge/trading-investing/triangular-arbitrage-opportunity/
            https://www.investopedia.com/terms/t/triangulararbitrage.asp
            https://en.wikipedia.org/wiki/Triangular_arbitrage

    """

    def __init__(self, exchange: ExmoExchange, logger):
        self.exchange = exchange
        self.logger = logger.getChild('tri_arb') if logger else logging.getLogger(__name__)

        # Indicators needed on per quote currency basis, can be started as many indicators as currencies to watch
        self.indicators = []
        currencies = exchange.get_currencies()
        for quote_curr in QUOTE_CURRS:
            self.indicators.append(TriangularArbitrageIndicator(quote_curr, currencies, exchange.get_fees(),
                                                                ORDER_TYPE, GAIN_MIN_LIMIT, logger))

        # TODO trader needed only to process round, upon opportunity accepted,
        # init in oppo_signal_handler, delete upon finish round
        self.trader = TriangularArbitrageTrader(PAPER_TRADING, ORDER_TYPE, exchange, logger)
        self._i: int = 0  # counter just to track tickers to skip

    def start(self):
        self._init_db()
        for indicator in self.indicators:
            indicator.register_signal_handler(self.triarb_signal_handler)
        self.trader.register_order_update_handler(self.order_update_handler)

    def update(self):
        if not self.trader.is_loop_in_progress():
            tickers = self.exchange.get_ticker()

            self._i += 1
            if self._i % TICKERS_TO_SKIP == 0:
                self._persist_tickers(tickers)
            for indicator in self.indicators:
                indicator.update(tickers)

        self.trader.update()

    def triarb_signal_handler(self, pair1: PairAndRate, pair2: PairAndRate, pair3: PairAndRate, gain: Decimal,
                              order_type: OrderType, quote_curr: str):
        # persist triarb condition/opportunity at which trader round trip initiated, for stats and analysis
        # TODO add quote_curr to persist arb opp
        triarb_opp_id = self._persist_triarb_opportunity(pair1, pair2, pair3, gain, order_type)
        # if gain <= GAIN_MIN_LIMIT:
        #     self.logger.info('Gain is too small, ignoring opportunity')
        #     return

        # recalculate gain taking into account slippage in order book depth (market orders)
        # should we do it here not in trader?
        trading_amount = TRADING_AMOUNTS[quote_curr]

        if ORDER_TYPE == OrderType.MARKET:
            gain_with_slip, gain_with_slip_amount = self._recalc_gain_with_slippage(pair1, pair2, pair3, trading_amount)
            if gain_with_slip_amount < PNL_MIN_LIMIT:
                self.logger.info('Gain with slippage: %s is too small, ignoring opportunity, PnL: %s %s',
                                 gain_with_slip, gain_with_slip_amount, quote_curr)
                return
            self.logger.info("Gain with slippage: %s, PnL: %s", gain_with_slip, gain_with_slip_amount)

        self.trader.start_arb_loop(pair1, pair2, pair3, triarb_opp_id, trading_amount)

    def _recalc_gain_with_slippage(self, pair1, pair2, pair3, trading_amount):
        # get order book by each pair, depth 30 is enough?
        order_book = self.exchange.get_order_book("%s,%s,%s" % (pair1.pair, pair2.pair, pair3.pair), DEPTH)

        p1_order_book = order_book[pair1.pair]
        p2_order_book = order_book[pair2.pair]
        p3_order_book = order_book[pair3.pair]

        acquired_amount = trading_amount

        # sum up down the book until amount is up to given trading amount, do along the pairs in sequence
        p1_weighted_rate, acquired_amount = self._get_weighted_rate(acquired_amount, p1_order_book, pair1.side)
        p2_weighted_rate, acquired_amount = self._get_weighted_rate(acquired_amount, p2_order_book, pair2.side)
        p3_weighted_rate, acquired_amount = self._get_weighted_rate(acquired_amount, p3_order_book, pair3.side)

        gain = p2_weighted_rate * p3_weighted_rate - 1 / p1_weighted_rate
        gain_amount = acquired_amount - trading_amount

        return gain, gain_amount

    def _get_weighted_rate(self, amount_to_sell, order_book, side: OrderSide):
        quantity_total = Decimal('0')
        amount_total = Decimal('0')
        last_rate = Decimal('0')

        if side == OrderSide.BUY:  # buying curr_a in pair
            sell_orders = order_book['ask']  # list of sell orders, we buy at this side of order book
            for ask in sell_orders:
                last_rate = Decimal(ask[0])
                quantity_total += Decimal(ask[1])  # field is: price, quantity and amount, price -> ask[0]
                amount_total += Decimal(ask[2])
                # count until amount to sell is spent, that's how deep the order will slip
                if amount_total >= amount_to_sell:
                    break

            if amount_total < amount_to_sell:
                msg = "Not enough depth: {} for the whole amount to sell: {}. Increase DEPTH."\
                    .format(DEPTH, amount_to_sell)
                raise Exception(msg, DEPTH, amount_to_sell)
                # TODO think how to avoid exceptions and gracefully just skip this opportunity, what to return?

            # adjust quantity and amount to required amount_to_sell
            if amount_total > amount_to_sell:
                quantity_total = quantity_total - ((amount_total - amount_to_sell) / last_rate)
                amount_total = amount_to_sell

        else:  # selling curr_a in pair, acquiring curr_b
            buy_orders = order_book['bid']  # list of buy orders, we sell at this side of order book
            for bid in buy_orders:
                last_rate = Decimal(bid[0])
                quantity_total += Decimal(bid[1])  # field is: price, quantity and amount, price -> ask[0]
                amount_total += Decimal(bid[2])
                # count until acquired amount is sold as quantity, that how deep the order will slip
                if quantity_total >= amount_to_sell:
                    break

            if quantity_total < amount_to_sell:
                msg = "Not enough depth {} for the whole amount to sell: {}. Increase DEPTH."\
                    .format(DEPTH, amount_to_sell)
                raise Exception(msg, DEPTH, amount_to_sell)

            # adjust quantity and amount to required amount_to_sell
            if quantity_total > amount_to_sell:
                amount_total = amount_total - ((quantity_total - amount_to_sell) * last_rate)
                quantity_total = amount_to_sell

        fee = (1 - self.exchange.get_fees()['taker'])
        weighted_rate = fee * quantity_total / amount_total if side == OrderSide.BUY else \
            fee * amount_total / quantity_total
        acquired_amount = quantity_total if side == OrderSide.BUY else amount_total

        return weighted_rate, acquired_amount

    def order_update_handler(self, order: Order) -> Order:
        # persist triarb condition/opportunity at which trader round trip initiated, for stats and analysis
        return self._persist_order(order)
        # TODO should we track orders here? not in trader?

    def _persist_tickers(self, tickers: Dict[str, Ticker]):
        values = []
        for key in tickers:
            values.append((key, datetime.now()) + tickers[key])
        self.sqlite_market.executemany(MD_TICKER_INSERT, values)

    def _persist_triarb_opportunity(self, pair1: PairAndRate, pair2: PairAndRate, pair3: PairAndRate, gain: Decimal,
                                    order_type: OrderType) -> int:
        values = {}
        for k, v in pair1._asdict().items():
            values[k + '1'] = v
        for k, v in pair2._asdict().items():
            values[k + '2'] = v
        for k, v in pair3._asdict().items():
            values[k + '3'] = v
        values['gain'] = gain
        values['order_type'] = str(order_type)
        values['created'] = datetime.now()

        cursor = self.sqlite_triarb.cursor()
        cursor.execute(TD_OPP_INSERT, values)
        return cursor.lastrowid

    def _persist_order(self, order: Order) -> Order:
        values = order._asdict()
        values['side'] = str(order.side)
        values['type'] = str(order.type)
        values['status'] = str(order.status)

        if order.status == OrderStatus.PLACING:
            cursor = self.sqlite_triarb.cursor()
            cursor.execute(TD_ORDER_INSERT, values)
            return order._replace(id=cursor.lastrowid)
        else:
            self.sqlite_triarb.execute(TD_ORDER_UPDATE, values)
            return order

    def shutdown(self):
        # stop running strategy, finish round, cancel unfilled orders etc
        self.sqlite_market.commit()
        self.sqlite_triarb.commit()
        self.sqlite_market.close()
        self.sqlite_triarb.close()
        for indicator in self.indicators:
            indicator.unregister_signal_handler(self.triarb_signal_handler)
        self.trader.unregister_order_update_handler(self.order_update_handler)

    def _init_db(self):
        self.sqlite_market = sqlite3.connect(MARKET_DATA_DB, isolation_level=None,
                                             detect_types=sqlite3.PARSE_DECLTYPES)
        self.sqlite_triarb = sqlite3.connect(TRIARB_DATA_DB, isolation_level=None,
                                             detect_types=sqlite3.PARSE_DECLTYPES)

        self.sqlite_market.executescript(MARKET_DATA_DDL)
        self.sqlite_triarb.executescript(TRIARB_DATA_DDL)


def register_sqlite_adapters_and_converters():
    # date and timestamp types are registered already for datetime.time and datetime.datetime classes
    sqlite3.register_adapter(Decimal, lambda d: '#'+str(d))
    sqlite3.register_converter("decimal", lambda s: Decimal(s[1:]))


register_sqlite_adapters_and_converters()

# Clean up namespace
del register_sqlite_adapters_and_converters
