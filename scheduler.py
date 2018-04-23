#!/usr/bin/env python
from trading.ibstate import IBstate
import trading.portfolio
import config.portfolios
import config.strategy
from trading.account import Account
from core.utility import notify_send
from time import sleep
import schedule
import sys
import traceback
from core.logger import get_logger
from functools import partial
logger = get_logger('scheduler')
p = trading.portfolio.Portfolio(instruments=config.portfolios.p_trade)
i = p.instruments


def init_ib():
    ib = IBstate()
    ib.connect()
    print("connecting")
    while not ib.is_ready():
        print("Not ready")
        sleep(5)
    if len(ib.open_orders()>0):
        print('Open orders:', ib.open_orders())
    print_net(ib.accounts)
    jobs = []
    jobs.append(schedule.every(6).hours.do(partial(print_net, ib.accounts)))
    jobs.append(schedule.every(15).seconds.do(ib.connect))
    jobs.append(schedule.every(15).seconds.do(ib.update_open_orders))
    return ib, jobs


def sync_trades():
    ib, ib_jobs = init_ib()
    print(1)
    accs = [a for a in ib.accounts.values() if a.name.startswith(('U', 'DU'))]
    notify('Running Sync Trades for {0} accounts: {1}'.format(len(accs), [a.name for a in accs]))
    trade = False if "--dryrun" in sys.argv else True
    p.cache_clear()
    # print("Starting validation")
    # validate = p.validate()[['carry_forecast', 'currency_age', 'panama_age', 'price_age', 'weighted_forecast']]
    # logger.info('\n' + str(validate))
    # do for all accounts
    for a in accs:
        print_net(a)
        notify('Running Sync Trades for account %s' % a.name)
        try:
            ib.sync_portfolio(p, acc=a, trade=trade)
        except AssertionError:
            notify('No sync for account %s, validation failed' % a.name, level='warning')
            continue
        notify('Portfolio synced')
        print_net(a)
    # cancel ib-specific scheduler jobs
    [schedule.cancel_job(j) for j in ib_jobs]


def print_net(accs):
    """
    :param accs: Account object, or dict, or None
    """
    if isinstance(accs, Account):
        accs = {accs.name: accs}
    for a in accs.values():
        notify('Net liquidation for account %s: %.2f %s ' % (a.name, a.net, a.base_currency))


def set_schedule(time):
    schedule.every().monday.at(time).do(sync_trades)
    schedule.every().tuesday.at(time).do(sync_trades)
    schedule.every().wednesday.at(time).do(sync_trades)
    schedule.every().thursday.at(time).do(sync_trades)
    schedule.every().friday.at(time).do(sync_trades)


def main():
    # width = pd.util.terminal.get_terminal_size() # find the width of the user's terminal window
    # rows, columns = os.popen('stty size', 'r').read().split()
    # pd.set_option('display.width', width[0]) # set that as the max width in Pandas
    # pd.set_option('display.width', int(columns)) # set that as the max width in Pandas

    set_schedule(config.strategy.portfolio_sync_time)

    if "--now" in sys.argv:
        sync_trades()

    if "--quit" not in sys.argv:
        while True:
            schedule.run_pending()
            sleep(1)


def notify(msg, level='info'):
    notify_send(level, msg)
    try:
        getattr(logger, level)(msg)
    except:
        logger.info(msg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Shutdown requested...exiting")
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        logger.exception(e, exc_info=True)
        sys.exit(0)
