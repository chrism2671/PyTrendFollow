import datetime
import os
import threading
from time import sleep
import pandas as pd
from ib.ext.Contract import Contract
from ib.ext.ExecutionFilter import ExecutionFilter
from ib.ext.Order import Order
from ib.ext.TagValue import TagValue
from ib.opt import ibConnection
import config.instruments
import config.portfolios
import config.settings
import data.db_mongo as db
import core.utility
from core.ib_connection import get_next_id
from core.logger import get_logger
from trading.account import Account

logger = get_logger('ibstate')


class IBstate(object):
    """
    Stateful object to help us interact with Interactive Brokers.
    """
    def __init__(self):
        self.orders_cache = {}      # market orders placed during the current trading session
        self.open_orders_raw = []   # list of orders that are placed, but not yet executed
        self.order_status_raw = []
        self.clientId = get_next_id()   # client ID fot TWS terminal
        self.port = getattr(config.settings, 'ib_port', 4001)   # socket port for TWS terminal
        if os.environ.get('D_HOST') is not None:    # host for TWS terminal
            self.host = os.environ.get('D_HOST')
        else:
            self.host = 'localhost'

        self.api_delay = 5              # Minimal interval between IB API requests in seconds
        self.last_error = None          # Last API error code, used for error handling
        self.last_account = None        # Last traded account, used for error handling
        # Flags used to determine if we're ready for trading
        self.accounts_loaded = False
        self.positions_loaded = False
        self.accounts = {}              # List of IB accounts in a multi-account system
        # Events for thread synchronization
        self.next_id_event = threading.Event()
        self.open_orders_event = threading.Event()
        # IB connection objects used to send all API requests
        self.connection = ibConnection(host=self.host, port=self.port, clientId=self.clientId)

    def is_ready(self):
        """
        :return: True when IBstate is ready for trading
        """
        return self.accounts_loaded and self.positions_loaded

    def connect(self):
        """
        Establishes a connection to TWS terminal. Automatically retries if an error occurs.
        """
        while not (hasattr(self.connection, 'isConnected') and self.connection.isConnected()):
            self.accounts_loaded = False
            self.positions_loaded = False
            logger.info('Connecting with clientId %d on %s:%d' % (self.clientId, self.host, self.port))
            r = self.connection.connect()
            if r:
                logger.info('Connection successful!')
                self._subscribe()
                self.update_open_orders()
            else:
                logger.warning("Couldn't establish connection, retrying in %d s." % (self.api_delay))
                sleep(self.api_delay)

    def open_orders(self):
        """
        :return: A pd.Series of orders that were placed on the exchange,
                 but haven't yet been executed
        """
        self.open_orders_event.wait()
        if len(self.open_orders_raw) > 0:
            return pd.Series(
                {(v.contract.m_symbol, v.contract.m_expiry[0:6], v.order.m_account): \
                     v.order.m_totalQuantity * core.utility.direction(v.order.m_action) \
                 for v in self.open_orders_raw}).fillna(0).rename('open'). \
                rename_axis(['instrument', 'contract', 'account'])
        else:
            return pd.Series()

    def update_open_orders(self):
        """
        Send API request to pull the list of open orders for all accounts.
        Callbacks: self._open_order_handler() and self._open_order_end_handler()
        """
        self.open_orders_raw.clear()
        self.open_orders_event.clear()
        self.connection.reqAllOpenOrders()
        self.open_orders_event.wait()

    def place_order(self, instrument, expiry, quantity, acc=None):
        """
        Send API request to place an order on the exchange.
        :param instrument: core.instrument.Instrument object
        :param expiry: contract label
        :param quantity: order size as a signed integer (quantity > 0 means 'BUY'
                         and quantity < 0 means 'SELL')
        :param acc: IB account to place order from, if None - the default account will be used
        """
        contract = Contract()
        contract.m_symbol = instrument.ib_code
        contract.m_secType = 'FUT'
        # place_order expects the contract label here, not the actual expiration date
        contract.m_expiry = expiry
        contract.m_exchange = instrument.exchange
        contract.m_currency = instrument.denomination
        if hasattr(instrument, 'ib_trading_class'):
            contract.m_tradingClass = instrument.ib_trading_class
        if hasattr(instrument, 'ib_multiplier'):
            contract.m_multiplier = instrument.ib_multiplier

        order = Order()
        order.m_orderType = 'MKT'
        order.m_algoStrategy = 'Adaptive'
        order.m_algoParams = [TagValue('adaptivePriority', 'Patient')]
        order.m_totalQuantity = int(abs(quantity))
        order.m_action = quantity > 0 and 'BUY' or 'SELL'
        if acc is not None:
            order.m_account = acc.name
            self.last_account = acc
        logger.warning(
            ' '.join(['Order:', str(self.order_id), contract.m_symbol, contract.m_expiry, \
                      order.m_action, str(order.m_totalQuantity)]))
        self.connection.placeOrder(self.order_id, contract, order)
        self.orders_cache[self.order_id] = {'contract': contract,
                                            'order': order}
        # order_id may not update just after the order is submitted so we save the previous one and
        # keep requesting until it's updated or we hit the time/iterations limit
        prev_id = self.order_id
        i = 0
        while prev_id >= self.order_id:
            sleep(self.api_delay)
            i += 1
            logger.debug('Requesting next order_id..')
            self.connection.reqIds(1)
            self.next_id_event.wait(timeout=(self.api_delay * 30))
            self.next_id_event.clear()
            if i > 60:
                logger.warning("Couldn't obtain next valid order id. Next orders may not be"
                               "submitted correctly!")
                return

    def sync_portfolio(self, portfolio, acc=None, trade=True):
        """
        Calculate the ideal positions for a portfolio and place orders on the market
        :param portfolio: trading.portfolio.Portfolio object
        :param acc: trading.account.Account object
        :param trade: bool, if False - will print the positions but won't actually trade
        """

        if acc is None:
            acc = list(self.accounts.values())[0]

        assert acc.is_valid()

        # if trade is True:
        #     self.connection.reqGlobalCancel()

        frontier = portfolio.frontier(capital=acc.net)

        positions = acc.portfolio
        if positions.empty:
            trades = frontier
            trades['position'] = pd.Series(0, index=trades.index)
            logger.info('No position')
        else:
            logger.info('\n' + str(positions['pos']))
            positions.rename(columns={'pos': 'position'}, inplace=True)
            trades = positions.join(frontier, how='outer').fillna(0)[['position', 'frontier']]

        oord = self.open_orders()
        # check if we have open orders for specific account
        hasorders = (len(oord) > 0) and (
        acc.name in oord.index.levels[oord.index.names.index('account')])
        if hasorders:
            trades = trades.join(oord[:, :, acc.name], how='outer').fillna(0)
            logger.error("Account {} has open orders, not trading".format(acc.name))
            return
        else:
            trades['open'] = pd.Series(0, index=trades.index)

        trades['trade'] = trades['frontier'] - trades['position'] - trades['open']
        trades['inst'] = trades.index.get_level_values(0)
        trades['inst'] = trades['inst'].apply(portfolio.ibcode_to_inst)
        trades = trades[trades['trade'].abs() > 0]
        # trades_close = trades[trades.isnull()['inst']]
        trades = trades[~trades.isnull()['inst']]
        logger.info('\n' + str(trades))
        # pprint.pprint(trades)
        sleep(self.api_delay * 10)
        # # close any opened positions for instruments not in the portfolio
        # p_temp = Portfolio(instruments=config.portfolios.p_all)
        # trades_close['inst'] = trades_close.index.to_frame()['instrument'].apply(p_temp.ibcode_to_inst)
        # trades_close['trade'] = -(trades_close['position'] + trades_close['open'])
        if trade:
            # [self.place_order(k.inst, k.Index[1], k.trade, acc=acc) for k in trades_close.itertuples()]
            [self.place_order(k.inst, k.Index[1], k.trade, acc=acc) for k in
             trades.itertuples()]
        else:
            logger.info("Dry run, not actually trading.")
        return trades


    """ ===== API Events subscription and handlers ===== """


    def _register(self):
        self.connection.register(self._error_handler, 'Error')
        self.connection.register(self._account_summary_handler, 'AccountSummary')
        self.connection.register(self._account_summary_end_handler, 'AccountSummaryEnd')
        self.connection.register(self._next_valid_id_handler, 'NextValidId')
        self.connection.register(self._execution_handler, 'ExecDetails')
        self.connection.register(self._commission_report_handler, 'CommissionReport')
        self.connection.register(self._open_order_handler, 'OpenOrder')
        self.connection.register(self._open_order_end_handler, 'OpenOrderEnd')
        self.connection.register(self._order_status_handler, 'OrderStatus')
        self.connection.register(self._managed_accounts_handler, 'ManagedAccounts')
        self.connection.register(self._positions_handler, 'Position')
        self.connection.register(self._positions_end_handler, 'PositionEnd')

    def _subscribe(self):
        self._register()
        self.connection.reqExecutions(2, ExecutionFilter())
        self.connection.reqAccountSummary(1, 'All', 'NetLiquidation')
        self.connection.reqIds(1)
        # in theory this should help to avoid errors on connection break/restore in the middle of
        # trading, but needs some testing
        logger.debug('Connection: waiting for next valid ID..')
        self.next_id_event.wait()
        logger.debug('Obtained next ID, connection is completed')
        self.next_id_event.clear()

    def _order_status_handler(self, msg):
        self.order_status_raw.append(msg)

    def _open_order_handler(self, msg):
        if msg.contract.m_secType == 'FUT':
            self.open_orders_raw.append(msg)
        db.insert_order(msg)

    def _open_order_end_handler(self, msg):
        self.open_orders_event.set()

    def _managed_accounts_handler(self, msg):
        logger.info('Managed accounts: %s' % msg.accountsList)
        acc_names = msg.accountsList.split(',')
        try:  # there may be an empty element due to leading comma
            acc_names.remove('')
        except ValueError:
            pass
        self.accounts = {x: Account(x) for x in acc_names}
        # subscribe to all accounts updates
        self.connection.reqPositions()

    def _positions_handler(self, msg):
        i = {v['ib_code']: v for v in config.instruments.instrument_definitions
                             if v.get('ib_code') is not None}
        try:
            expiration_month = i[msg.contract.m_symbol]['expiration_month']
        except KeyError:
            expiration_month = 0

        if msg.contract.m_secType == 'FUT':
            # m_expiry represents the actual expiration date here, so need to convert
            #  that to a contract label
            date = datetime.datetime.strptime(msg.contract.m_expiry, '%Y%m%d') +\
            datetime.timedelta(weeks = (-4 * expiration_month))
            contract = datetime.datetime.strftime(date, '%Y%m')
            contract_index = pd.MultiIndex.from_tuples([(str(msg.contract.m_symbol), contract)],\
                                                       names=['instrument','contract'])
            pd_msg = pd.DataFrame(dict(msg.items()), index=contract_index)
            self.pd_msg = pd_msg

            if msg.account in self.accounts:
                acc = self.accounts[msg.account]
                if contract_index[0] in acc.portfolio.index:
                    acc.portfolio.update(pd_msg)
                else:
                    acc.portfolio = acc.portfolio.append(pd_msg)

    def _positions_end_handler(self, msg):
        self.positions_loaded = True
        logger.debug('Accounts positions loaded')

    def _account_summary_handler(self, msg):
        acc = self.accounts[msg.account]
        acc.summary = dict(zip(msg.keys(), msg.values()))
        acc.net = float(msg.value)
        acc.base_currency = msg.currency
        try:
            assert msg.currency == config.settings.base_currency
        except AssertionError:
            logger.error("IB currency %s, system currency %s", msg.currency, config.settings.base_currency)
        db.insert_account_summary(msg)

    def _account_summary_end_handler(self, msg):
        self.accounts_loaded = True
        logger.debug('Accounts summary loaded')

    def _next_valid_id_handler(self, msg):
        self.order_id = msg.orderId
        logger.debug('Next valid id = %d' % msg.orderId)
        self.next_id_event.set()

    def _error_handler(self, msg):
        if type(msg.errorMsg) == ConnectionResetError:
            msg.errorMsg = 'Connection Reset Error'
            self.clientId = get_next_id()
            logger.warning("Trying clientId %s", self.clientId)

        if (msg.id != -1) and (msg.id != None):
            if (self.last_account is None) and (len(self.accounts) > 0):
                self.last_account = list(self.accounts.values())[0]
            db.insert_error(msg, self.last_account.name)

        try:
            # Stop repeated disconnection errors
            if self.last_error != msg.errorCode and msg.errorCode != 1100:
                logger.error(' '.join([str(msg.id), str(msg.errorCode), msg.errorMsg]))
        except:
            logger.error('Null error - TWS gateway has probably closed.')

        self.last_error = msg.errorCode

        # Client Id in Use
        if msg.id == -1 and msg.errorCode == 326:
            self.clientId = get_next_id()
            logger.warning("Updated clientId %s", self.clientId)

        # Connection restored
        if msg.id == -1 and msg.errorCode == 1102:
            self._subscribe()

        # Not connected
        if msg.id == -1 and msg.errorCode == 504:
            self.connection.connect()
            self._subscribe()

        # Could not connect
        if msg.id == -1 and msg.errorCode == 502:
            sleep(self.api_delay*6)

    def _execution_handler(self, msg):
        print("Execution:", msg.execution.m_acctNumber, msg.execution.m_orderId,
              msg.execution.m_side, msg.execution.m_shares, msg.execution.m_price)
        db.insert_execution(msg.execution)

    def _commission_report_handler(self, msg):
        db.insert_commission_report(msg.commissionReport)
