#!/usr/bin/env python
"""
triarbot: Simple triangular arbitrage bot
Python 3+
(C) 2018 SurgeonY, Planet Earth

Donate ETH: 0xFA745708C435300058278631429cA910AE175d52
Donate BTC: 16KqCc4zxEWf7CaerWNZdGYwyuU33qDzCv
"""

import logging
from datetime import datetime
from enum import Enum
from typing import NamedTuple, Callable, Iterator
from decimal import Decimal
from exchanges.exmo_exchange import ExmoExchange


class OrderType(Enum):
    MARKET = 'Market'
    LIMIT = 'Limit'

    def __str__(self):
        return self._name_


class OrderSide(Enum):
    BUY = 'buy'
    SELL = 'sell'

    def __str__(self):
        return self._name_


class OrderStatus(Enum):
    PLACING = 'placing'
    OPEN = 'open'
    PARTIAL = 'partial'
    COMPLETED = 'completed'
    CANCELED = 'canceled'

    def __str__(self):
        return self._name_


class PairAndRate(NamedTuple):
    pair: str
    # the rate at which arb opportunity was calculated and which is effective in terms of profits and losses,
    # includes all fees and offsets/margins, it's normalized, so always multiply to get quantity despite buy or sell
    calc_rate: Decimal
    # original rate from exchange plus needed offsets/margins, rate at which order is placed
    order_rate: Decimal
    # side of the order
    side: OrderSide


class Order(NamedTuple):
    id: int
    exch_order_id: int
    triarb_opportunity_id: int
    created: datetime
    pair: str
    quantity: Decimal
    price: Decimal
    side: OrderSide
    type: OrderType  # limit or market
    status: OrderStatus


class TriangularArbitrageTrader:
    """
    TODO should be split to limit and market order traders
    """

    def __init__(self, paper_trading: bool, order_type: OrderType, exchange: ExmoExchange, logger: logging.Logger):
        self.paper_trading = paper_trading
        self.order_type = order_type
        self.exchange = exchange
        self.initial_amount = 0
        self.logger = logger if logger else logging.getLogger(__name__)

        self.order_update_handler: Callable[[Order], Order] = None

        # self.tickers: Dict[str, Ticker] = {}
        # self.legs: List[PairAndRate] = []
        self.pairs_iter: Iterator[PairAndRate]
        self.triarb_opp_id: int = 0
        # self.orders: List[Order] = []

        self.current_pair: PairAndRate = None
        self.cur_pair_idx: int = 0
        self.current_order: Order = None
        self.current_acquired_curr: str = ''
        self.current_acquired_amount: Decimal = 0

    def start_arb_loop(self, pair1: PairAndRate, pair2: PairAndRate, pair3: PairAndRate, triarb_opp_id: int,
                       trading_amount: Decimal):
        # accepts signals only when all orders placed are filled and sequence finished or canceled
        # TODO pass initial_amount as trading amount here not in constructor

        # create order1 for pair1 at rate_curr1_quote
        # wait for order1 filled completely/executed
        # store amount of curr1 received

        # create order2 for pair2 at rate_curr2_curr1
        # wait for order2 filled completely/executed
        # store amount of curr2 received

        # create order3 for pair3 at rate_quote_curr2
        # wait for order3 filled completely/executed
        # store amount of quote curr received = result_amount
        # calculate PnL = result_amount - trading_amount, store PnL

        if self.paper_trading or self.current_order:
            return  # arb loop in progress

        self.initial_amount = trading_amount
        self.current_acquired_amount = trading_amount

        self.pairs_iter = iter([pair1, pair2, pair3])
        self.triarb_opp_id = triarb_opp_id

        # First pair process: buy curr1 with quote_curr (or sell quote_curr for curr1)
        self.current_pair = next(self.pairs_iter)   # switch leg
        self.cur_pair_idx += 1
        self.logger.info('Triarb sequence started: %s', (pair1.pair, pair2.pair, pair3.pair))
        self._place_order(self.current_pair, self.triarb_opp_id)

    def update(self):
        if self.paper_trading:
            return

        # track order filling: check orders fill status, order filling trades prices - trace pair1 order
        # if no open orders or no order_id order open then it's completed
        self._update_current_order_status()

        if self.current_order and self.current_order.status == OrderStatus.COMPLETED:
            # Second: buy curr2 for curr1 (or sell curr1 for curr2)
            # Third: selling curr2 for quote curr... (or equivalent is buying our quote curr for curr2)
            try:
                self.current_pair = next(self.pairs_iter)
                self.cur_pair_idx += 1
                self._place_order(self.current_pair, self.triarb_opp_id)
            except StopIteration:
                self.logger.info('Triarb sequence finished')
                self.current_order = None
                self.cur_pair_idx = 0
                # update PnL from tickers and order filling trades prices
                pnl = self.current_acquired_amount - self.initial_amount
                self.logger.info('Result of round: %s %s', pnl, self.current_acquired_curr)
                # TODO persist result of round for stats

    def is_loop_in_progress(self):
        return self.current_order is not None

    def _place_order(self, pair: PairAndRate, triarb_opp_id: int):
        if self.paper_trading:
            return

        if self.order_type == OrderType.LIMIT and pair.side == OrderSide.BUY:
            quantity = self.current_acquired_amount / pair.order_rate
        else:
            quantity = self.current_acquired_amount

        self.current_order = Order(-1, -1, triarb_opp_id, datetime.now(), pair.pair, quantity,
                                   pair.order_rate, pair.side, self.order_type, OrderStatus.PLACING)
        self._handle_order_update()  # persisted, updated with db id

        if self.order_type == OrderType.LIMIT:
            if pair.side == OrderSide.BUY:
                exch_order_id = self.exchange.place_limit_buy(pair.pair, quantity, pair.order_rate)
            else:
                exch_order_id = self.exchange.place_limit_sell(pair.pair, quantity, pair.order_rate)
        else:  # Market
            if pair.side == OrderSide.BUY:
                # rate is not needed as quantity is effectively an amount in buy_total order
                exch_order_id = self.exchange.place_market_buy_total(pair.pair, quantity)
            else:
                exch_order_id = self.exchange.place_market_sell(pair.pair, quantity)
        # TODO update order in case of error as well - status PLACING and error msg if occurred

        self.current_order = self.current_order._replace(exch_order_id=exch_order_id, status=OrderStatus.OPEN)
        self._handle_order_update()  # persisted, updated with exchange id

        self.logger.info('Order placed for pair[%s] : %s', self.cur_pair_idx, pair.pair)

    def _update_current_order_status(self):
        if self.current_order and self.current_order.exch_order_id != -1:
            open_orders = self.exchange.get_user_open_orders()
            if open_orders and self.current_pair.pair in open_orders:
                pair_orders = open_orders[self.current_pair.pair]  # list
                for o in pair_orders:
                    if int(o['order_id']) == self.current_order.exch_order_id:
                        return  # order remains open

            # otherwise it's completed
            # or canceled, TODO need to track by order trades and partial filling, later
            order_trades = self.exchange.get_order_trades(self.current_order.exch_order_id)
            if order_trades:
                self.current_acquired_curr = order_trades['in_currency']
                # amount should be actual on balance, accounting for fees
                # it turned out that exmo returns in_amount as of only per order, not deducting fees
                # which obviously is different from actual amount landing on your balance
                fee = self.exchange.get_fees()['taker']
                self.current_acquired_amount = Decimal(order_trades['in_amount']) * (1 - fee)
                self.logger.info('Order executed %s, acquired %s %s', self.current_order.exch_order_id,
                                 self.current_acquired_amount, self.current_acquired_curr)

                self.current_order = self.current_order._replace(status=OrderStatus.COMPLETED)
                self._handle_order_update()
            else:
                self.logger.error('Something went wrong on exchange, order executed but no trades found, order %s',
                                  self.current_order.exch_order_id)
                raise Exception('Error: order executed but no trades found', self.current_order.exch_order_id)

        else:
            # clearing current order after error placing order, when exch_order_id = -1 remains not updated
            self.current_order = None

    def _handle_order_update(self):
        if self.order_update_handler:
            self.current_order = self.order_update_handler(self.current_order)  # persisted, updated with db id

    # -
    # -
    def register_order_update_handler(self, handler: Callable[[Order], Order]) -> None:
        self.order_update_handler = handler

    def unregister_order_update_handler(self, handler):
        if handler == self.order_update_handler:
            self.order_update_handler = None
