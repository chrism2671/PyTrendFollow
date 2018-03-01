from config.settings import base_currency

"""
This file defines currencies exchange rates. Loaded into core.currency.Currency object.
Used for instrument's returns calculation and for some spot prices.

{
    'code': 'USD' + base_currency,              # Currency pair unique code that we can refer by
    'currency_data': ['ib', 'quandl'],          # Data providers we should collect and merge data from 
    'quandl_database': 'CURRFX',                # Database on Quandl providing the data
    'quandl_symbol': 'USD' + base_currency,     # Symbol on Quandl representing this currency pair
    'quandl_rate': lambda x: x,                 # See 'ib_rate'
    'ib_exchange': 'IDEALPRO',                  # Exchange identifier on Interactive Brokers.
    'ib_symbol': base_currency,                 # Symbol on Interactive Brokers
    'ib_currency': 'USD',                       # Denomination currency on Interactive Brokers
    'ib_rate': lambda x: 1.0 / x,               # Sometimes data provider may not have data for
                                                  the currency pair we need, but have the reversed
                                                  one (e.g. we need USD/GPB, but there is only
                                                  GPB/USD). In such case we reverse the rate with a
                                                  lambda expression to obtain the data we need.
                                                  This can also be used to multiply the price by
                                                  some factor or do any other transformation if
                                                  necessary.
}

"""
currencies_definitions = [
    {
        # this dummy definition helps to avoid additional key-checks for the special case
        # when denomination == base_currency
        'code': base_currency * 2,
        'currency_data': []
    },
    {
        'code': 'USD' + base_currency,
        'currency_data': ['ib', 'quandl'],
        'quandl_database': 'CURRFX',
        'quandl_symbol': 'USD' + base_currency,
        'quandl_rate': lambda x: x,
        'ib_exchange': 'IDEALPRO',
        'ib_symbol': base_currency,
        'ib_currency': 'USD',
        'ib_rate': lambda x: 1.0 / x,
    },
    {
        'code': 'CHF' + base_currency,
        'currency_data': ['ib', 'quandl'],
        'quandl_database': 'CURRFX',
        'quandl_symbol': 'CHF' + base_currency,
        'quandl_rate': lambda x: x,
        'ib_exchange': 'IDEALPRO',
        'ib_symbol': base_currency,
        'ib_currency': 'CHF',
        'ib_rate': lambda x: 1.0 / x,
    },
    {
        'code': 'HKD' + base_currency,
        'currency_data': ['ib', 'quandl'],
        'quandl_database': 'CURRFX',
        'quandl_symbol': 'HKD' + base_currency,
        'quandl_rate': lambda x: x,
        'ib_exchange': 'IDEALPRO',
        'ib_symbol': base_currency,
        'ib_currency': 'HKD',
        'ib_rate': lambda x: 1.0 / x,
    },
    {
        'code': 'EUR' + base_currency,
        'currency_data': ['ib', 'quandl'],
        'quandl_database': 'CURRFX',
        'quandl_symbol': 'EUR' + base_currency,
        'quandl_rate': lambda x: x,
        'ib_exchange': 'IDEALPRO',
        'ib_symbol': 'EUR',
        'ib_currency': base_currency,
        'ib_rate': lambda x: x,
    },
    {
        'code': 'BTC' + base_currency,
        'currency_data': ['quandl'],
        'quandl_database': 'BCHARTS',
        'quandl_symbol': 'LOCALBTC' + base_currency,
        'quandl_rate': lambda x: x,
    },

    {
        'code': 'BTCUSD',
        'currency_data': ['quandl'],
        'quandl_database': 'BCHARTS',
        'quandl_symbol': 'LOCALBTCUSD',
        'quandl_rate': lambda x: x,
    },
    {
        'code': 'MXNUSD',
        'currency_data': ['ib', 'quandl'],
        'quandl_database': 'CURRFX',
        'quandl_symbol': 'MXNUSD',
        'quandl_rate': lambda x: x,
        'ib_exchange': 'IDEALPRO',
        'ib_symbol': 'USD',
        'ib_currency': 'MXN',
        'ib_rate': lambda x: 1.0 / x,
    },
    {
        'code': 'AUDUSD',
        'currency_data': ['ib', 'quandl'],
        'quandl_database': 'CURRFX',
        'quandl_symbol': 'AUDUSD',
        'quandl_rate': lambda x: x,
        'ib_exchange': 'IDEALPRO',
        'ib_symbol': 'AUD',
        'ib_currency': 'USD',
        'ib_rate': lambda x: x,
    },
    {
        'code': 'EURUSD',
        'currency_data': ['ib', 'quandl'],
        'quandl_database': 'CURRFX',
        'quandl_symbol': 'EURUSD',
        'quandl_rate': lambda x: x,
        'ib_exchange': 'IDEALPRO',
        'ib_symbol': 'EUR',
        'ib_currency': 'USD',
        'ib_rate': lambda x: x,
    },
    {
        'code': 'GBPUSD',
        'currency_data': ['ib', 'quandl'],
        'quandl_database': 'CURRFX',
        'quandl_symbol': 'GBPUSD',
        'quandl_rate': lambda x: x,
        'ib_exchange': 'IDEALPRO',
        'ib_symbol': 'GBP',
        'ib_currency': 'USD',
        'ib_rate': lambda x: x,
    },
    {
        'code': 'NZDUSD',
        'currency_data': ['ib', 'quandl'],
        'quandl_database': 'CURRFX',
        'quandl_symbol': 'NZDUSD',
        'quandl_rate': lambda x: x,
        'ib_exchange': 'IDEALPRO',
        'ib_symbol': 'NZD',
        'ib_currency': 'USD',
        'ib_rate': lambda x: x,
    },
    {
        'code': 'JPYUSD',
        'currency_data': ['ib', 'quandl'],
        'quandl_database': 'CURRFX',
        'quandl_symbol': 'JPYUSD',
        'quandl_rate': lambda x: x,
        'ib_exchange': 'IDEALPRO',
        'ib_symbol': 'USD',
        'ib_currency': 'JPY',
        'ib_rate': lambda x: 1.0 / x,
    },
]

currencies_all = {x['code']: x for x in currencies_definitions}
