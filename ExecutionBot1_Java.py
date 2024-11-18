"""This module implements an ExecutionBot for trading strategies."""

import sys
import time
import logging
from time import sleep
import argparse
from Management import Management  # pylint: disable=import-error
import requests
import json

sys.path.append('../')
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s.%(funcName)s() line: %(lineno)d: %(message)s"

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                    level=logging.INFO)


class ExecutionBot(Management):
    """ExecutionBot class for implementing trading strategies."""

    def __init__(self, strategy, starting_money,
                 market_event_securities, market_event_queue, securities,
                 host=None, bot_id=None):

        super().__init__(strategy, starting_money,
                         market_event_securities, market_event_queue, securities,
                         host, bot_id)

        self.stat = {}
        self.start()
        self.penalty = .125
        sleep(10)

    def start_task(self, sym, action, size):
        self.stat = {
            'strategy': self.strategy,
            'sym': sym,
            'action': action,
            'qty_target': size,
            'bp': self.mid_market[sym],
            'vwap': self.vwap[sym]
        }

    def task_complete(self, pv, qty, time_t, slices):
        self.stat['pv'] = pv
        self.stat['qty'] = qty
        self.stat['time_t'] = time_t
        self.stat['slices'] = slices
        self.stop(self.stat, log=True)

    def aggressive_orders(self, qty, action, exec_t=2, log=True):
        sym = self.securities[0]

        book_side = 'Ask' if action == 'buy' else 'Bid'
        side = 'B' if action == 'buy' else 'S'

        benchmark_price = self.mid_market[sym]
        benchmark_vwap = self.vwap[sym]

        qty_target = qty

        t_start = time.time()

        pv = 0

        while qty > 0 and time.time() - t_start < exec_t:

            book_levels = self.market_event_queue.copy()

            size = 0
            order_prices = []
            order_qty = []
            
            while size < qty and len(book_levels) > 0:
                market_dict_local = self.market_dict

                payload = {
                    "qty": qty,
                    "action": action,
                    "sym": sym,
                    "book_side": book_side,
                    "side": side,
                    "book_levels": book_levels,
                    "market_dict_local": market_dict_local,
                    "vwap_sym": self.vwap[sym],
                    "strategy": self.strategy,
                    "internalID": self.internalID,
                    "size": size
                }

                response = requests.post("http://localhost:8000/api/placeAggressiveOrders", data=payload).json()
                
                # try:
                #     level_size = self.market_dict[sym][level][book_side + 'Size'] # market_dict is in Management.py
                #     level_price = self.market_dict[sym][level][book_side + 'Price']
                #     print(
                #         f'level is {level}, size in this level is {level_size}, '
                #         f'price in this level is {level_price}'
                #     )
                #     size_level = min(
                #         qty-size, self.market_dict[sym][level][book_side + 'Size'])
                #     size += int(size_level)

                #     order_prices.append(
                #         self.market_dict[sym][level][book_side + 'Price'])
                #     order_qty.append(size_level)
                #     print(f'pty is {qty}, size_leve is {size_level}')
                # except Exception:
                #     pass
            
            print(response)

            order_prices = response["order_prices"]
            order_qty = response["order_qty"]
            size = response["size"]

            print(order_prices)
            print(order_qty)
            orders = []
            for p, q in zip(order_prices, order_qty):
                order = {'symb': sym,
                         'price': p,
                         'origQty': q,
                         'status': "A",
                         'remainingQty': q,
                         'action': "A",
                         'side': side,
                         'FOK': 0,
                         'AON': 0,
                         'strategy': self.strategy,
                         'orderNo': self.internalID
                         }

                self.send_order(order)
                logging.info("Aggressive order sent: \n"
                             "\t %s: "
                             "%s | "
                             "%s | "
                             "%s | "
                             "%s | "
                             "%s",
                             order['symb'],
                             order['orderNo'],
                             order['side'],
                             order['origQty'],
                             order['remainingQty'],
                             order['price'])

                orders.append(order)
                self.internalID += 1
            qty = 0

            for order in orders:

                in_id = order["orderNo"]

                if in_id in self.inIds_to_orders_confirmed:
                    order = self.inIds_to_orders_confirmed[in_id]
                    order['orderNo'] = self.inIds_to_exIds[in_id]

                    self.cancel_order(order)
                    self.logger.info("Cancelled order: \n"
                                     "\t %s: "
                                     "%s | "
                                     "%s | "
                                     "%s | "
                                     "%s | "
                                     "%s",
                                     order['symb'],
                                     order['orderNo'],
                                     order['side'],
                                     order['origQty'],
                                     order['remainingQty'],
                                     order['price'])

                    qty += order['remainingQty']
                    pv += order['price'] * \
                        (order['origQty'] - order['remainingQty'])
                else:
                    self.logger.info("Fully filled aggressive order: \n"
                                     "\t %s: "
                                     "%s | "
                                     "%s | "
                                     "%s | "
                                     "%s",
                                     order['symb'],
                                     order['orderNo'],
                                     order['side'],
                                     order['remainingQty'],
                                     order['price'])

                    pv += order['price'] * order['origQty']

        try:
            cost_qty = pv / (qty_target - qty) - benchmark_price*1.
        except Exception:
            cost_qty = 999.99
            benchmark_price = 999.99
        if action == 'buy':
            cost_qty *= -1

        logging.info('\n\t Aggressive order: %s %s %s given %s seconds: \n'
                     '\t Transaction cost: %s per share\n'
                     '\t Benchmark price %s\n'
                     '\t Benchmark VWAP: %s',
                     action, qty_target -
                     qty, sym, min(time.time() - t_start, exec_t),
                     cost_qty, benchmark_price, benchmark_vwap)
        _, pv_final = self.final_liquidation(qty, action)

        cost_qty = (pv + pv_final) / qty_target - benchmark_price
        if action == 'buy':
            cost_qty *= -1

        return pv, qty

    def execute_twap_orders(self, qty, action, n_slices, exec_t=3.0):
        sym = self.securities[0] # From Management.py
        benchmark_price = self.mid_market[sym]
        benchmark_vwap = self.vwap[sym]
        t_start = time.time()

        book_side = 'Ask' if action == 'buy' else 'Bid'
        side = 'B' if action == 'buy' else 'S'
        pre_vwap = benchmark_vwap
        qty_target = qty
        
        max_time = exec_t * n_slices
        pv = 0
        qty_slice = 0
        for i in range(n_slices):
            book_levels = self.market_event_queue.copy()
            market_dict_local = self.market_dict

            if qty <= 0:
                break

            payload = {
                "qty": qty,
                "action": action,
                "n_slices": n_slices,
                "sym": sym,
                "book_side": book_side,
                "side": side,
                "pre_vwap": pre_vwap,
                "book_levels": book_levels,
                "market_dict_local": market_dict_local,
                "vwap_sym": self.vwap[sym],
                "strategy": self.strategy,
                "internalID": self.internalID,
                "n_slices_iterator": i
            }
            
            ################
            # TEST PAYLOAD #
            ################

            # payload = {
            #     "sym": "ZBH0:MBO",
            #     "action": "buy",
            #     "qty": 100,
            #     "n_slices": 10,
            #     "book_levels": ["L1", "L2", "L3"],
            #     "market_dict_local": {
            #         "ZBH0:MBO": {
            #             "L1": {"buySize": 10, "buyPrice": 100},
            #             "L2": {"buySize": 20, "buyPrice": 101},
            #             "L3": {"buySize": 30, "buyPrice": 102}
            #         }
            #     },  
            #     "book_side": "buy",
            #     "side": "long",
            #     "vwap_sym": 100.5,
            #     "pre_vwap": 100.2,
            #     "n_slices_iterator": 0,
            #     "strategy": "TWAP",
            #     "internalID": 1
            # }

            payload = json.dumps(payload)

            response = (requests.post("http://localhost:8000/api/placeOrders", data=payload)).json()
            
            order_prices = response["order_prices"]
            order_qty = response["order_qty"]
            size = response["size"][0]
            target_q = response["target_q"][0]

            orders = []
            for p, q in zip(order_prices, order_qty):
                order = {'symb': sym,
                         'price': p,
                         'origQty': q,
                         'status': "A",
                         'remainingQty': q,
                         'action': "A",
                         'side': side,
                         'FOK': 0,
                         'AON': 0,
                         'strategy': self.strategy,
                         'orderNo': self.internalID
                         }
                self.send_order(order)
                logging.info("Slice %s - twap order sent: \n"
                             "\t %s: "
                             "%s | "
                             "%s | "
                             "%s | "
                             "%s | "
                             "%s",
                             i+1, order['symb'], order['orderNo'], order['side'],
                             order['origQty'], order['remainingQty'], order['price'])
                orders.append(order)
                self.internalID += 1
            
            for order in orders:
                in_id = order["orderNo"]
                if in_id in self.inIds_to_orders_confirmed:
                    order = self.inIds_to_orders_confirmed[in_id]
                    order['orderNo'] = self.inIds_to_exIds[in_id]
                    self.cancel_order(order)
                    self.logger.info("Cancelled limit order %s out of %s: \n"
                                     "\t %s: "
                                     "%s | "
                                     "%s | "
                                     "%s | "
                                     "%s",
                                     order['remainingQty'], order['origQty'],
                                     order['symb'], order['orderNo'], order['side'],
                                     order['remainingQty'], order['price'])
                    qty_slice += order['remainingQty']
                    pv += order['price'] * \
                        (order['origQty'] - order['remainingQty'])
                else:
                    self.logger.info("Fully filled limit order: \n"
                                     "\t %s: "
                                     "%s | "
                                     "%s | "
                                     "%s | "
                                     "%s",
                                     order['symb'], order['orderNo'], order['side'],
                                     order['remainingQty'], order['price'])
                    pv += order['price'] * order['origQty']

            # qty -= order['size'] - qty_slice # order['size'] is same irrespective of the order iterator in orders - handled in Java
            # qty_slice += order['target_q'] - order['size'] # order['size'] is same irrespective of the order iterator in orders - handled in Java

            qty -= size - qty_slice
            qty_slice += target_q - size

            if max_time + t_start - time.time() < 1 and qty > 0:
                pv_slice, qty_slice = self.aggressive_orders(qty, action)
                pv += pv_slice
                break
        try:
            cost_qty = pv / (qty_target - qty_slice) - benchmark_price * 1.
        except Exception:
            cost_qty = 999.99
            benchmark_price = 999.99
        if action == 'buy':
            cost_qty *= -1
        logging.info('\n\t Slicing order: %s %s %s\n'
                     '\t Given %s slices per %s seconds: \n'
                     '\t Transaction cost: %s per share\n'
                     '\t Benchmark price: %s\n'
                     '\t Benchmark VWAP: %s',
                     action, qty_target-qty_slice, sym, n_slices, exec_t,
                     cost_qty, benchmark_price, benchmark_vwap)
        _, pv_final = self.final_liquidation(qty_slice, action)
        cost_qty = (pv + pv_final) / qty_target - benchmark_price
        if action == 'buy':
            cost_qty *= -1
        return pv, qty

    def final_liquidation(self, remaining_qty, action, exec_t=30):
        penalty = 0
        pv_final = 0
        if remaining_qty > 0:
            pv_final, _ = self.aggressive_orders(remaining_qty, action, exec_t)
            penalty = self.penalty * remaining_qty
        return penalty, pv_final


if __name__ == "__main__":
    myargparser = argparse.ArgumentParser()
    myargparser.add_argument('--strategy', type=str,
                             const="TWAP", nargs='?', default="TWAP")
    myargparser.add_argument('--symbol', type=str,
                             const="ZNH0:MBO", nargs='?', default="ZBH0:MBO")
    myargparser.add_argument('--action', type=str,
                             const="buy", nargs='?', default="buy")
    myargparser.add_argument(
        '--size', type=int, const=1000, nargs='?', default=100)
    myargparser.add_argument('--maxtime', type=int,
                             const=120, nargs='?', default=120)
    myargparser.add_argument('--username', type=str, default='test')
    myargparser.add_argument('--password', type=str, default='test')
    myargparser.add_argument('--bot_id', type=str,
                             const='text', nargs='?', default='text')
    args = myargparser.parse_args()

    market_event_securities = [args.symbol]
    market_event_queue = ["L1", "L2", "L3", "L4", "L5"]
    securities = market_event_securities
    SERVER_HOST = "localhost"
    strategy = args.strategy
    execution_bot_id = args.bot_id
    STARTING_MONEY = 1000000000.0

    start_time = time.time()
    exec_bot = ExecutionBot(strategy, STARTING_MONEY, market_event_securities,
                            market_event_queue, securities, SERVER_HOST, execution_bot_id)
    exec_bot.start_task(args.symbol, args.action, args.size)

    pv, qty, num_slices = 0, 0, 10
    pv, qty = exec_bot.execute_twap_orders(
        args.size, args.action, num_slices, int(args.maxtime/num_slices))

    end_t = time.time()
    exec_bot.task_complete(pv, qty, end_t-start_time, num_slices)
