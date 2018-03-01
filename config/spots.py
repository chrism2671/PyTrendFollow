
"""
This file defines spot prices for futures. Loaded into core.spot.Spot object.
Spots are the underlying prices for future contracts, used for trading with "carry spot" rule.

{
    'name': 'copper',               # Unique name, usually the same as the derivative instrument from config/instruments.py
    'price_data': ['quandl'],       # Data providers we should collect and merge data from
    'quandl_database': 'LME',       # Database on Quandl providing the data
    'quandl_symbol': 'PR_CU',       # Symbol on Quandl
    'quandl_column': 'Cash Buyer',  # Data column on Quandl to be used as a 'close' price
    'multiplier': (1/2204.62)       # A factor to multiply the close price by
    ...
    'ib_symbol': 'HSI',             # Symbol on Interactive Brokers
    'ib_exchange': 'HKFE',          # Exchange identifier on Interactive Brokers
    'denomination': 'HKD',          # Denomination currency on Interactive Brokers
    'sec_type': 'IND',              # Contract security type on Interactive Brokers
    ...                               (currently only 'IND' is used, which stands for 'Index')
    'bitmex_symbol': '.STRXBT',     # Symbol on Bitmex
    'bitmex_column': 'close'        # Data column on Bitmex to be used as a 'close' price
},

"""

spots_definitions = [

    # Hard
    {
        'name': 'copper',
        'price_data': ['quandl'],
        'quandl_database': 'LME',
        'quandl_symbol': 'PR_CU',
        'quandl_column': 'Cash Buyer',
        'multiplier': (1/2204.62)
    },
    {
        'name': 'gold',
        'price_data': ['quandl'],
        'quandl_database': 'LBMA',
        'quandl_symbol': 'GOLD',
        'quandl_column': 'USD (AM)',
    },
    {
        'name': 'pallad',
        'price_data': ['quandl'],
        'quandl_database': 'LPPM',
        'quandl_symbol': 'PALL',
        'quandl_column': 'USD AM',
    },
    {
        'name': 'platinum',
        'price_data': ['quandl'],
        'quandl_database': 'LPPM',
        'quandl_symbol': 'PLAT',
        'quandl_column': 'USD AM',
    },
    {
        'name': 'silver',
        'price_data': ['quandl'],
        'quandl_database': 'LBMA',
        'quandl_symbol': 'SILVER',
        'quandl_column': 'USD',
    },

    # Index
    {
        'name': 'hsi',
        'price_data': ['ib'],
        'ib_symbol': 'HSI',
        'ib_exchange': 'HKFE',
        'denomination': 'HKD',
        'sec_type': 'IND',
    },
    {
        'name': 'smi',
        'price_data': ['ib'],
        'ib_symbol': 'SMI',
        'ib_exchange': 'SOFFEX',
        'denomination': 'CHF',
        'sec_type': 'IND',
    },
    {
        'name': 'eurostoxx',
        'price_data': ['ib'],
        'ib_symbol': 'ESTX50',
        'ib_exchange': 'DTB',
        'denomination': 'EUR',
        'sec_type': 'IND',
    },

    # Bitmex
    {
        'name': 'xbj',
        'price_data': ['bitmex'],
        'bitmex_symbol': '.XBTJPY',
        'bitmex_column': 'close'
    },
    {
        'name': 'bat',
        'price_data': ['bitmex'],
        'bitmex_symbol': '.BATXBT',
        'bitmex_column': 'close'
    },
    {
        'name': 'dash',
        'price_data': ['bitmex'],
        'bitmex_symbol': '.DASHXBT',
        'bitmex_column': 'close'
    },
    {
        'name': 'ethereum',
        'price_data': ['bitmex'],
        'bitmex_symbol': '.ETHXBT',
        'bitmex_column': 'close'
    },
    {
        'name': 'litecoin',
        'price_data': ['bitmex'],
        'bitmex_symbol': '.LTCXBT',
        'bitmex_column': 'close'
    },
    {
        'name': 'monero',
        'price_data': ['bitmex'],
        'bitmex_symbol': '.XMRXBT',
        'bitmex_column': 'close'
    },
    {
        'name': 'stellar',
        'price_data': ['bitmex'],
        'bitmex_symbol': '.STRXBT',
        'bitmex_column': 'close'
    },
    {
        'name': 'ripple',
        'price_data': ['bitmex'],
        'bitmex_symbol': '.XRPXBT',
        'bitmex_column': 'close'
    },
    {
        'name': 'zcash',
        'price_data': ['bitmex'],
        'bitmex_symbol': '.ZECXBT',
        'bitmex_column': 'close'
    },
]

spots_all = {x['name']: x for x in spots_definitions}