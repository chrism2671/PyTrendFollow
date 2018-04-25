import numpy as np
import logging
import pandas as pd
from collections import namedtuple
import datetime
import subprocess

"""
Miscellaneous utility functions
"""


class ConnectionException(Exception):
    """Exception subclass to handle errors with data providers connection"""
    pass


def cbot_month_code(month):
    """Return the CBOT contract month code by the month's number"""
    cbot_month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
    return cbot_month_codes[int(month)-1]


def contract_to_tuple(contract):
    """Return the year and the month for the given contract label"""
    contract = str(contract)
    return (int(contract[0:4]),int(contract[4:6]))


def filter_outliers(s, sigma=4):
    """Filter out samples with more than sigma std deviations away from the mean"""
    return s[~(s-s.mean()).abs() > sigma*s.std()]


def dict_to_namedtuple(dictionary):
    return namedtuple('GenericDict', dictionary.keys())(**dictionary)


def sharpe(p):
    """Sharpe ratio of the returns"""
    try:
        return p.mean()/p.std()*np.sqrt(252)
    except ZeroDivisionError:
        logging.error("Zero volatility, divide by zero in Sharpe ratio.")
        return np.inf


def rolling_sharpe(p):
    """Mean sharpe ratio of the returns in a rolling window of the size 252"""
    p = np.trim_zeros(p)
    return p.rolling(252, min_periods=252).mean()/p.rolling(252, min_periods=252).std()*np.sqrt(252)


def expanding_sharpe(p):
    return p.expanding(min_periods=252).mean()/p.expanding(min_periods=252).std()*np.sqrt(252)


def chunk_trades(j):
    """Take a list of notional positions and filter so that trades are only greater
       than 10% of notional position"""
    return np.around(np.exp(np.around(np.log(np.abs(j)), decimals=1)).multiply(np.sign(j), axis=0))


def drawdown(x):
    maxx = x.rolling(len(x), min_periods=1).max()
    return x - maxx


def norm_vol(df):
    """Arbitrarily normalize the volatility of a series. Useful for where different instruments
     have different price volatilities but we still want to feed them into the same regressor
    WARNING: Lookahead bias here"""
    return (df*10/bootstrap(df, lambda x: x.dropna().std()))
    # return (df*10/df.expanding(50).std())


def contract_from_date(expire_day, months, date):
    """Find the expiry date for a given contract. Currently this function is not used."""
    date = pd.to_datetime(date)
    expiries = [datetime.datetime(date.year, k, expire_day) for k in months]
    expiries.extend([datetime.datetime(date.year+1, k, expire_day) for k in months])
    expiries = pd.Series(expiries)

    try:
        expiry = expiries[(date > expiries).diff().fillna(False)].iloc[0]
    except IndexError:
        expiry = expiries.iloc[0]

    return date_to_contract(expiry)


def generate_roll_progression(roll_day, months, roll_shift):
    """Fast version of contract_from_date, used to generate roll sequence quickly."""
    rolls = []
    for year in range(1960,datetime.datetime.now().year+5):
        rolls.extend([datetime.datetime(year, k, abs(roll_day)) for k in months])

    rolls = pd.Series(rolls, index=rolls, name='rolls').shift(-1)

    dates = pd.date_range(start='1960-01-01', end=datetime.datetime.now()+pd.DateOffset(years=10))
    a = pd.Series(0, index=dates).to_frame().join(rolls).ffill().dropna()['rolls'].to_frame()
    a['contract'] = a['rolls'].apply(date_to_contract)
    a.index = a.index.rename('date')
    b = a['contract']

    if roll_day<1:
        b = b.shift(-30).dropna().apply(int)

    if roll_shift!=0:
        b = b.shift(roll_shift).dropna().apply(int)
    return b[:datetime.datetime.now()]


def date_to_contract(date):
    """Return contract label for the given date"""
    return int(str(date.year)+str("%02d" % (date.month,)))


def capital():
    return 1000000


def direction(action):
    if action == 'BUY':
        return 1
    if action == 'SELL':
        return -1


def draw_sample(df, length=1):
    """Random sample from a dataframe of the given length. Used for bootstrapping"""
    end_of_sample_selection_space = df.index.get_loc(df[-1:].index[0])-length
    s = np.random.choice(range(0, end_of_sample_selection_space))
    return df.iloc[s:s+length]


def weight_forecast(forecasts, weights):
    f = (forecasts*weights).mean(axis=1)
    f = norm_forecast(f)
    return f.clip(-20,20)


def norm_forecast(a):
    """Normalize a forecast, such that it has an absolute mean of 10.
    WARNING: Insample lookahead bias"""
    return (a*10/bootstrap(a, lambda x: x.dropna().abs().mean())).clip(-20,20)


def ibcode_to_inst(ib_code):
    import config.instruments
    a = {k.ib_code: k for k in config.instruments.instrument_definitions}
    return a[ib_code]


def notify_send(title, message):
    """Send a system notification using libnotify if it's installed"""
    try:
        subprocess.Popen(['notify-send', title, message])
    except:
        pass


def contract_format(symbol, expiry, format='cbot'):
    """Return contract label for the given symbol and expiry"""
    year, month = contract_to_tuple(expiry)
    if format == 'cbot':
        return symbol + cbot_month_code(month) + str(year)
    else:
        return False


def generate_random_prices(length=100000):
    a = np.random.rand(1,length)
    return pd.Series(a[0]-0.5).cumsum().rename('random')


def bootstrap(x, f):
    if len(x) == 0:
        return np.nan
    from arch.bootstrap import StationaryBootstrap
    bs = StationaryBootstrap(50, x)
    return bs.apply(f,100).mean()


def sortino(x):
    if type(x) == pd.Series:
        x = x.to_frame()
    return np.trim_zeros(x.sum(axis=1)).mean()/np.std(losses(x))*np.sqrt(252)


def losses(x):
    return [z for z in np.trim_zeros(x).sum(axis=1) if z<0]
