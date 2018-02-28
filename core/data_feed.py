from core.contract_store import Store, QuotesType
import core.logger
from config.settings import data_sources
logger = core.logger.get_logger('data_feed')


def get_instrument(instrument):
    """
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
        elif p == 'bitmex':
            db = ''
            symbol = instrument.bitmex_symbol
        else:
            raise Exception('Unknown data provider string: %s' % p)
        p_data = _get_data(p, QuotesType.futures, db, symbol)
        data = p_data if data is None else data.combine_first(p_data)
    return data


def get_currency(currency):
    """
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
        elif p == 'bitmex':
            db = ''
            symbol = currency.bitmex_symbol
        else:
            raise Exception('Unknown data provider string: %s' % p)
        p_data = _get_data(p, QuotesType.currency, db, symbol)
        data = p_data if data is None else data.combine_first(p_data)
    return data


def get_spot(spot):
    """
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
        elif p == 'bitmex':
            db = ''
            symbol = spot.bitmex_symbol
        else:
            raise Exception('Unknown data provider string: %s' % p)
        p_data = _get_data(p, QuotesType.others, db, symbol)
        data = p_data if data is None else data.combine_first(p_data)
    return data


def get_quotes(provider, **kwargs):
    return _get_data(provider, QuotesType.others, kwargs['database'], kwargs['symbol'])


def _get_data(library, q_type, database, symbol, **kwargs):
    try:
        return Store(library, q_type, database + '_' + symbol).get()
    except Exception as e:
        logger.warning("Something went wrong on symbol %s_%s request from storage: %s" %
                       (database, symbol, e))
        return None
