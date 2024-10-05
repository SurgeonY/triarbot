#!/usr/bin/env python
"""
triarbot: Simple triangular arbitrage bot
Python 3+
(C) 2018 SurgeonY

Donate ETH: 0xFA745708C435300058278631429cA910AE175d52
Donate BTC: 16KqCc4zxEWf7CaerWNZdGYwyuU33qDzCv
"""

import sys
import http.client
import time
from exchange_apis import exmo_api
import logging.handlers
import pathlib
# import yaml
from ruamel.yaml import YAML
from argparse import ArgumentParser
from exchanges.exmo_exchange import *
from strategy_runner import StrategyRunner
from triarbstrat.tri_arb_strategy import TriangularArbitrageStrategy
from triarbstrat.tri_arb_strategy import POLLING_INTERVAL


LOG_FILENAME = './logs/triarbot.out'
pathlib.Path('./logs').mkdir(parents=True, exist_ok=True)

log = logging.getLogger('triarbot')
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s - %(message)s'))
log.addHandler(handler)

handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=2096000, backupCount=5)
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s - %(message)s'))
log.addHandler(handler)

arg_parser = ArgumentParser(description="Simple triangular arbitrage trading bot")
arg_parser.add_argument("-s", "--secrets", dest="SECRETS_PATH", default="secrets_conf_template.yml",
                        help="path to a config file with api keys and secrets", metavar="FILE")
arg_parser.add_argument("-c", "--config", dest="CONFIG_PATH", default="config.yml",
                        help="path to a config file with bot parameters", metavar="FILE")

yaml = YAML(typ='safe')   # default, if not specfied, is 'rt' (round-trip)


def run():
    log.info("Starting")

    args = arg_parser.parse_args()

    secrets = yaml.load(open(args.SECRETS_PATH))

    api_instance = exmo_api.ExmoAPI(secrets["exmo"]["API_KEY"], secrets["exmo"]["API_SECRET"],
                                    secrets["exmo"]["API_URL"])
    global exmo_exchange
    exmo_exchange = ExmoExchange(api_instance, log)  # exchange can be abstract, TODO consider CCXT lib, exctract exchange interface and make pluggable exchanges

    try:
        while True:
            # TODO remove interactive cmd, make normal cmdline params parsing
            print()
            print("*************************************************************************************************************************************")
            print(" 1:", '\t', "Get ticker ", '\n',
                  "2:", '\t', "Get balance ", '\n',
                  "3:", '\t', "Get last trade ", '\n',
                  "4:", '\t', "Get open orders ", '\n',
                  "5:", '\t', "Get markets ", '\n',
                  "6:", '\t', "!!! Cancel all orders !!!", '\n',
                  "90:", '\t', "Start triangular arbitrage ", '\n',
                  "91:", '\t', "Stop triangular arbitrage ", '\n'
                  )
            print(" 00", '\t', "Exit")
            print()

            try:
                oper = int(input("Input val:... "))
            except Exception:
                continue

            if oper == 00:
                shutdown_tri_arb()
                break

            if oper == 1:
                ticker = exmo_exchange.get_ticker(PAIR)
                log.info('Current spread: buy = %s, sell= %s, date: %s',
                         ticker.buy_price, ticker.sell_price, str(ticker.updated))
                continue

            if oper == 2:
                account = get_account_info()
                log.info("User account as on: %s", account['server_date'])

                s = ""
                for curr in account['balances']:
                    if account['balances'][curr] > 0:
                        s = s + curr + "=" + str(account['balances'][curr]) + ", "
                log.info("--Cash balance: %s", s)
                s = ""
                for curr in account['reserved']:
                    if account['reserved'][curr] > 0:
                        s = s + curr + "=" + str(account['reserved'][curr]) + ", "
                log.info("--In orders: %s", s)

                continue

            if oper == 3:
                last_trade = get_last_trade(PAIR)
                log.info("Last trade: %s", last_trade)
                continue

            if oper == 4:
                get_open_orders(PAIR)
                continue

            if oper == 5:
                markets = exmo_exchange.get_markets()
                s = ''
                for pair in markets:
                    s += pair + ', '
                log.info('Markets available %s: %s', len(markets), s)
                continue

            if oper == 6:
                cancel_all_orders()
                continue

            if oper == 90:
                global tri_arb_runner
                log.info('Initializing triangualar arbitrage strategy...')
                tri_arb_strat = TriangularArbitrageStrategy(exmo_exchange, log)
                tri_arb_runner = StrategyRunner(tri_arb_strat, log, POLLING_INTERVAL)
                tri_arb_runner.start()
                log.info('Triangular arbitrage strategy is running...')
                continue

            if oper == 91:
                shutdown_tri_arb()
                continue

    except BaseException as e:
        log.error("Error processing: %s", e, exc_info=1)

    log.info("Stopping")
    return


def shutdown_tri_arb():
    try:
        tri_arb_runner.shutdown_flag.set()
        tri_arb_runner.join()
        log.info('Triangular arbitrage runner is stopped')
    except NameError:
        log.warning('Triangular arbitrage should be sarted first')


def cancel_all_orders():
    log.info("Canceling orders...")

    my_orders = get_open_orders()
    for order in my_orders:
        exmo_exchange.cancel_order(order['order_id'])

    log.info("All Orders canceled")


def get_account_info():
    try:
        api_instance = exmo_api.ExmoAPI(API_KEY, API_SECRET)
        response = api_instance.api_query("user_info")

        response['server_date'] = from_timestamp(response['server_date'])
        for curr in response['balances']:
            response['balances'][curr] = float(response['balances'][curr])
        for curr in response['reserved']:
            response['reserved'][curr] = float(response['reserved'][curr])

        return response

    except BaseException as e:
        log.error("Error getting balance: %s", e)
        raise e


def get_open_orders():
    """
    Returns open orders for provided currency pair
    :param pair: currency pair to filter out orders
    :return:
            order_id - order identifier
            created - date and time of order creation
            type - type of order
            pair - currency pair
            price - price in the order
            quantity – quantity in the order
            amount – sum of the order
    """

    try:
        api_instance = exmo_api.ExmoAPI(API_KEY, API_SECRET)
        orders_pairs: Dict = api_instance.api_query("user_open_orders")

        result = []
        # orders = orders[pair]
        for order_pair in orders_pairs.values():
            for order in order_pair:
                order["created"] = str(from_timestamp(order["created"]))
                print("\t" + str(order))
                result.append(order)

        return result

    except BaseException as err:
        log.error('Error getting my orders: %s', err, exc_info=1)
        return -1


if __name__ == '__main__':
    run()
