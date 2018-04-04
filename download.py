#!/usr/bin/env python
from core.instrument import Instrument
from core.currency import Currency
from core.spot import Spot
from data.providers_factory import get_provider
from core.contract_store import QuotesType
import config.settings
import config.portfolios
import sys, traceback
import core.logger
logger = core.logger.get_logger('download')
import pyprind
from functools import partial
from core.utility import ConnectionException


"""
Script usage: python download.py <data_provider_name> [--recent] [--concurrent]
<data_provider_name> should be either 'ib' (for Interactive Brokers) or 'quandl'
--recent flag: if provided, will only download historical contracts for the last year
                (only applies to quandl)
--concurrent flag: if provided, will download data in multiple CPU threads (only applies to quandl,
                multi-threaded downloads is a paid feature, so make sure you have a subscription) 
"""

# download a single instrument's contracts
def dl_inst(i, prov_name, recent):
    dp = get_provider(prov_name)
    try:
        if prov_name in i.contract_data:
            dp.download_instrument(i, recent=recent)
    except ConnectionException as e:
        raise e
    except Exception as e:
        logger.warning(e)
        logger.warning('Contract download error, ignoring')


# download currency exchange rate
def dl_cur(c, prov_name):
    dp = get_provider(prov_name)
    try:
        if prov_name in c.currency_data:
            dp.download_currency(c)
    except ConnectionException as e:
        raise e
    except Exception as e:
        logger.warning(e)
        logger.warning('Currency download error, ignoring')


# download spot prices
def dl_spot(s, prov_name):
    dp = get_provider(prov_name)
    try:
        if prov_name in s.price_data:
            dp.download_spot(s)
    except ConnectionException as e:
        raise(e)
    except Exception as e:
        logger.warning(e)
        logger.warning('Spot price download error, ignoring')


def download_all(prov_name, qtype, recent, concurrent):
    if qtype == QuotesType.futures:
        instruments = Instrument.load(config.portfolios.p_all)
        dl_fn = partial(dl_inst, prov_name=prov_name, recent=recent)
        attr = 'name'
        title_name = 'contracts'
    elif qtype == QuotesType.currency:
        instruments = Currency.load_all()
        dl_fn = partial(dl_cur, prov_name=prov_name)
        attr = 'code'
        title_name = 'currencies'
    elif qtype == QuotesType.others:
        instruments = Spot.load_all()
        dl_fn = partial(dl_spot, prov_name=prov_name)
        attr = 'name'
        title_name = 'spot prices'
    else:
        raise Exception('Unknown quotes type')

    title = 'Downloading %s history for %s' % (title_name, prov_name)
    if concurrent: title += ' (parallel)'
    bar = pyprind.ProgBar(len(instruments.values()), title=title)

    if concurrent:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            download_futures = {executor.submit(lambda v: dl_fn(v), x):
                                    getattr(x, attr) for x in instruments.values()}
            for future in concurrent.futures.as_completed(download_futures):
                bar.update(item_id=download_futures[future])
    else:
        for i in instruments.values():
            dl_fn(i)


if __name__ == "__main__":
    try:
        if len(sys.argv) < 2 or sys.argv[1] not in ['quandl', 'ib']:
            print('Usage: download.py [quandl|ib] [--recent] [--concurrent]')
            sys.exit(1)
        provider = sys.argv[1]
        recent = '--recent' in sys.argv
        concurrent = '--concurrent' in sys.argv
        download_all(provider, QuotesType.futures, recent, concurrent)
        download_all(provider, QuotesType.currency, recent, concurrent)
        download_all(provider, QuotesType.others, recent, concurrent)
    except KeyboardInterrupt:
        print("Shutdown requested...exiting")
    except ConnectionException as e:
        print('Connection error:', e)
        print('exiting...')
    except Exception:
        traceback.print_exc(file=sys.stdout)
        sys.exit(0)
