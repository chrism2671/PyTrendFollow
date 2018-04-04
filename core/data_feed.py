from core.contract_store import Store, QuotesType
import core.logger
from config.settings import data_sources
logger = core.logger.get_logger('data_feed')

"""
This file defines functions to pull the contracts data from the local storage for multiple
data providers and merge them into a single pd.DataFrame. On duplicate index, data providers are
prioritized in the order they appear in the list in the instrument definition (config/instruments.py) 
"""


def get_instrument(instrument):
    """
    Get all contracts data for an instrument
    :param instrument: core.trading.Instrument object
    :return: price data as pd.DataFrame
    """
    # choose data providers with respect to the global list
    providers = [x for x in instrument.contract_data if x in data_sources]
    data = None
    for p in providers:
        if p == 'ib':
            db = instrument.exchange
            symbol = instrument.ib_code
        elif p == 'quandl':
            db = instrument.quandl_database
            symbol = instrument.quandl_symbol
        else:
            raise Exception('Unknown data provider string: %s' % p)
        p_data = _get_data(p, QuotesType.futures, db, symbol)
        data = p_data if data is None else data.combine_first(p_data)
    return data


def get_currency(currency):
    """
    Get data for a currency
    :param currency: core.currency.Currency object
    :return: rate data as pd.DataFrame
    """
    # choose data providers with respect to the global list
    providers = [x for x in currency.currency_data if x in data_sources]
    data = None
    for p in providers:
        if p == 'ib':
            db = currency.ib_exchange
            symbol = currency.ib_symbol + currency.ib_currency
        elif p == 'quandl':
            db = currency.quandl_database
            symbol = currency.quandl_symbol
        else:
            raise Exception('Unknown data provider string: %s' % p)
        p_data = _get_data(p, QuotesType.currency, db, symbol)
        data = p_data if data is None else data.combine_first(p_data)
    return data


def get_spot(spot):
    """
    Get data for a spot
    :param spot: core.spot.Spot object
    :return: price data as pd.DataFrame
    """
    # choose data providers with respect to the global list
    providers = [x for x in spot.price_data if x in data_sources]
    data = None
    for p in providers:
        if p == 'ib':
            db = spot.ib_exchange
            symbol = spot.ib_symbol
        elif p == 'quandl':
            db = spot.quandl_database
            symbol = spot.quandl_symbol
        else:
            raise Exception('Unknown data provider string: %s' % p)
        p_data = _get_data(p, QuotesType.others, db, symbol)
        data = p_data if data is None else data.combine_first(p_data)
    return data


def get_quotes(provider, **kwargs):
    """
    General method to get any quotes of type "others" from a single data provider
    :param provider: data provider name string
    :return: price data as pd.DataFrame
    """
    return _get_data(provider, QuotesType.others, kwargs['database'], kwargs['symbol'])


def _get_data(library, q_type, database, symbol, **kwargs):
    """
    General method to get quotes data from storage
    :param library: storage library name (usually corresponds to a data provider name)
    :param q_type: one of 'futures' | 'currency' | 'others'
    :param database: local storage database name
    :param symbol: local storage symbol name
    :return: pd.DataFrame or None in case of error
    """
    try:
        return Store(library, q_type, database + '_' + symbol).get()
    except Exception as e:
        logger.warning("Something went wrong on symbol %s_%s request from storage: %s" %
                       (database, symbol, e))
        return None
