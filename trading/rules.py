import numpy as np
import pandas as pd
from functools import partial
from core.utility import norm_forecast, norm_vol

def pickleable_ewmac(d, x):
    """
    Returns an exponentially weighted moving average crossover for a single series and period. 
    """
    return d.ewm(span = x, min_periods = x*4).mean() - d.ewm(span = x*4, min_periods=x*4).mean()

def ewmac(inst, **kw):
    """
    Exponentially weighted moving average crossover.
    
    Returns a DataFrame with four different periods.
    """
    d = norm_vol(inst.panama_prices(**kw))
    columns = [8, 16, 32, 64]
    f = map(partial(pickleable_ewmac, d), columns)
    f = pd.DataFrame(list(f)).transpose()
    f.columns = pd.Series(columns).map(lambda x: "ewmac"+str(x))
    return norm_forecast(f)

def mr(inst, **kw):
    """
    Mean reversion.
    
    Never managed to make this profitable on futures, however it is presented here if you want to try it.
    """
    d = norm_vol(inst.panama_prices(**kw))
    columns = [2, 4, 8, 16, 32, 64]
    f = map(partial(pickleable_ewmac, d), columns)
    f = pd.DataFrame(list(f)).transpose() * -1
    f = f*10/f.abs().mean()
    f.columns = pd.Series(columns).map(lambda x: "mr"+str(x))
    return f.clip(-20,20)

def carry(inst, **kw):
    """
    Generic carry function. 
    
    If the Instrument has a spot price series, use that for carry (carry_spot), otherwise use the next contract (carry_next)
    """
    if hasattr(inst, 'spot'):
        return carry_spot(inst, **kw).rename('carry')
    else:
        return carry_next(inst, **kw).rename('carry')

def carry_spot(inst, **kw):
    """
    Calculates the carry between the current future price and the underlying spot price.
    """
    f = inst.spot() - inst.market_price().reset_index('contract', drop=True)
    f = f * 365 / inst.time_to_expiry()
    return norm_forecast(f.ewm(90).mean()).rename('carry_spot')

def carry_next(inst, debug=False, **kw):
    """
    Calculates the carry between the current future price and the next contract we are going to roll to.
    """
    #    If not trading nearest contract,  Nearer contract price minus current contract price, divided by the time difference
    #    If trading nearest contract, Current contract price minus next contract price, divided by the time difference

    current_contract = inst.roll_progression().to_frame().set_index('contract', append=True).swaplevel()
    next_contract = inst.roll_progression().apply(inst.next_contract, months=inst.trade_only).to_frame().set_index('contract', append=True).swaplevel()

    current_prices = inst.contracts(active_only=True).join(current_contract, how='inner')['close'].reset_index('contract')

    next_prices = inst.contracts(active_only=True).join(next_contract, how='inner')['close'].reset_index('contract')

    #Replace zeros with nan
    next_prices[next_prices==0] = np.nan

    # Apply a ffill for low volume contracts
    # next_prices.ffill(inplace=True)

    current_prices['contract'] = current_prices['contract'].apply(_get_month)
    next_prices['contract'] = next_prices['contract'].apply(_get_month)
    td = next_prices['contract'] - current_prices['contract']
    td.loc[td<=0] = td.loc[td<=0] + 12
    td = td/12
    # Apply a 5 day mean to prices to stabilise signal
    carry = (current_prices['close'] - next_prices['close'])

    carry = carry.rolling(window=5).mean()/td

    f = norm_forecast(carry).ffill(limit=3)

    # if f.isnull().values.any():
    #     print(inst.name, "has some missing carry values")

    if f.sum() == 0:
        print(inst.name  + ' carry is zero')
    if debug==True:
        return f.rename('carry_next'), current_prices, next_prices, td
    # return f.mean().rename('carry_next')
    return f.interpolate().rolling(window=90).mean().rename('carry_next')

def carry_prev(inst, **kw):
    """
    Analogue of carry_next() but looks at the previous contract. Useful when not trading the nearest contract but one further one. Typically you'd do this for instruments where the near contract has deathly skew - e.g. Eurodollar or VIX.
    """
    
    current_contract = inst.roll_progression().to_frame().set_index('contract', append=True).swaplevel()
    prev_contract = inst.roll_progression().apply(inst.next_contract, months=inst.trade_only,
                            reverse=True).to_frame().set_index('contract', append=True).swaplevel()
    current_prices = inst.contracts(active_only=True).join(current_contract, how='inner')['close'].reset_index('contract')
    prev_prices = inst.contracts(active_only=True).join(prev_contract, how='inner')['close'].reset_index('contract')
    # Replace zeros with nan
    prev_prices[prev_prices == 0] = np.nan

    current_prices['contract'] = current_prices['contract'].apply(_get_month)
    prev_prices['contract'] = prev_prices['contract'].apply(_get_month)
    td = current_prices['contract'] - prev_prices['contract']
    td.loc[td <= 0] = td.loc[td <= 0] + 12
    td = td / 12
    # Apply a 5 day mean to prices to stabilise signal
    carry = prev_prices['close'] - current_prices['close']
    carry = carry.rolling(window=5).mean() / td
    f = norm_forecast(carry).ffill(limit=3)
    if f.sum() == 0:
        print(inst.name + ' carry is zero')
    return f.interpolate().rolling(window=90).mean().rename('carry')


def open_close(inst, **kw):
    """
    Read this one in a book, and it was easy to test. Consistently unprofitable in its current form.
    """
    #yesterdays open-close, divided by std deviation of returns
    inst.panama_prices()
    a = inst.rp().to_frame().set_index('contract', append=True).swaplevel().join(inst.contracts(), how='inner')
    f = ((a['close']-a['open'])/inst.return_volatility).dropna().reset_index('contract', drop=True)
    return norm_forecast(f).rename('open_close')

def weather_rule(inst, **kw):
    """
    They say the most likely weather for tomorrow is today's weather. If today's return was +ve,
    returns +10 for tomorrow and vice versa.
    """
    r = inst.panama_prices().diff()
    #where today's return is 0, just use yesterday's forecast 
    r[r==0]=np.nan
    r = np.sign(r) * 10
    r.ffill()
    return r.rename('weather_rule')
    
    
def buy_and_hold(inst, **kw):
    """
    Returns a fixed forecast of 10.
    """
    return pd.Series(10, index=inst.pp().index).rename('buy_and_hold')

def sell_and_hold(inst, **kw):
    """
    Returns a fixed forecast of -10.
    """
    return pd.Series(-10, index=inst.pp().index).rename('sell_and_hold')

def breakout_fn(data, lookback, smooth=None):
    """
    :param data: prices DataFrame
    :param lookback: lookback window in days
    :param smooth: moving average window for the forecast in days
    :return: forecast DataFrame, range [-1, 1] (unnormed)
    """
    if smooth is None:
        smooth = max(int(lookback / 4.0), 1)
    price_roll = data.rolling(lookback, min_periods=int(min(len(data), np.ceil(lookback / 2.0))))
    roll_min = price_roll.min()
    roll_max = price_roll.max()
    roll_mean = (roll_max + roll_min) / 2.0
    b = (data - roll_mean) / (roll_max - roll_min)
    bsmooth = b.ewm(span=smooth, min_periods=np.ceil(smooth / 2.0)).mean()
    return bsmooth

def breakout(inst, **kw):
    """
    Returns a DataFrame of breakout forecasts based on Rob Carver's work here:
    https://qoppac.blogspot.com.es/2016/05/a-simple-breakout-trading-rule.html
    """
    prices = inst.panama_prices(**kw)
    lookbacks = [40, 80, 160, 320]
    res = map(partial(breakout_fn, prices), lookbacks)
    res = pd.DataFrame(list(res)).transpose()
    res.columns = pd.Series(lookbacks).map(lambda x: "brk%d" % x)
    return norm_forecast(res)

def _get_month(a):
    return int(str(a)[4:6])

