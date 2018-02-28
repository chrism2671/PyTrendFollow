import pandas as pd
import numpy as np
from multiprocessing import cpu_count
from functools import partial
from scipy.optimize import minimize
from trading.accountcurve import accountCurve
from core.utility import draw_sample, weight_forecast
from multiprocessing_on_dill import Pool
from contextlib import closing


""" Bootstrap.py - find the best weights for forecasts on a single instrument. """

def optimize_weights(instrument, sample):
    """Optimize the weights on a particular sample"""
    guess = [1.0] * sample.shape[1]
    bounds = [(0.0,5.0)] * sample.shape[1]
    def function(w, instrument, sample):
        """This is the function that is minimized iteratively using scipy.optimize.minimize to find the best weights (w)"""
        wf = weight_forecast(sample, w)
        # We introduce a capital term, as certain currencies like HKD are very 'numerate', which means we need millions of HKD to get a
        # significant position
        position = instrument.position(forecasts = wf, nofx=True, capital=10E7).rename(instrument.name).to_frame().dropna()
        # position = instrument.position(forecasts = wf, nofx=True).rename(instrument.name).to_frame().dropna()
        l = accountCurve([instrument], positions = position, panama_prices=instrument.panama_prices().dropna(), nofx=True)
        s = l.sortino()
        try:
            assert np.isnan(s) == False
        except:
            print(sample, position)
            raise
        return -s

    result = minimize(function, guess, (instrument, sample),\
                      method = 'SLSQP',\
                      bounds = bounds,\
                      tol = 0.01,\
                      constraints = {'type': 'eq', 'fun': lambda x: sample.shape[1] - sum(x)},\
                      options = {'eps': .1},
                      )
    return result.x

def mp_optimize_weights(samples, instrument, **kw):
    """Calls the Optimize function, on different CPU cores"""
    with closing(Pool()) as pool:
        return pool.map(partial(optimize_weights, instrument), samples)

def bootstrap(instrument, n=(cpu_count() * 4), **kw):
    """Use bootstrapping to optimize the weights for forecasts on a particular instrument. Sets up the samples and gets it going."""
    forecasts = instrument.forecasts(**kw).dropna()
    weights_buffer = pd.DataFrame()
    sample_length = 200
    t = 0
    while t < 1:
        samples = [draw_sample(forecasts, length=sample_length) for k in range(0,n)]
        # This is a one hit for the whole price series
        # samples = [slice(prices.index[0:][0],prices.index[-1:][0])]
        weights = pd.DataFrame(list(mp_optimize_weights(samples, instrument, **kw)))
        weights_buffer = weights_buffer.append(weights).reset_index(drop=True)
        n=cpu_count()
        t = (weights_buffer.expanding(min_periods=21).mean().pct_change().abs()<0.05).product(axis=1).sum()
    weights_buffer.columns = forecasts.columns
    return weights_buffer
