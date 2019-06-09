#!/usr/bin/env python
"""
triarbot: Simple triangular arbitrage bot
Python 3+
(C) 2018 SurgeonY, Planet Earth

Donate ETH: 0xFA745708C435300058278631429cA910AE175d52
Donate BTC: 16KqCc4zxEWf7CaerWNZdGYwyuU33qDzCv
"""

from threading import Thread, Event
import time
import logging

from exchange_apis.exmo_api import ExmoError
from triarbstrat import tri_arb_strategy


class StrategyRunner(Thread):

    def __init__(self, strategy: tri_arb_strategy.TriangularArbitrageStrategy, logger, interval: int):
        Thread.__init__(self, name='TriArbRunner')
        self.strategy = strategy
        # TODO configure own file handler for runner
        self.logger = logger.getChild('runner') if logger else logging.getLogger(__name__)
        # update strategies with specified polling interval
        self.interval = interval

        self.shutdown_flag = Event()

    def run(self):
        self.logger.info('Thread #%s:%s started', self.ident, self.name)
        self.strategy.start()

        while not self.shutdown_flag.is_set():
            # main event loop
            try:
                time.sleep(self.interval)
                self.strategy.update()
            except OSError as e:
                self.logger.error('OS Error: %s', e, exc_info=1)
            except ExmoError as e2:
                self.logger.error('Exchange Error: %s', e2, exc_info=1)

        # ... Clean shutdown code here ...
        self.strategy.shutdown()
        self.logger.info('Thread #%s:%s stopped', self.ident, self.name)

