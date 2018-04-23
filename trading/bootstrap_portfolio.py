import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from scipy.optimize import minimize
from core.utility import draw_sample, sortino

""" Find the best weights of instruments in a portfolio """

def bootstrap(portfolio, n=1500, costs=True, **kw):
    data = portfolio.curve(portfolio_weights=1,capital=10E7).returns()
    sample_length = 300
    samples = [draw_sample(data, length=sample_length).index for k in range(0,n)]
    weights = list(mp_optimize_weights(samples, data, **kw))
    weights_buffer = pd.DataFrame([x for x in weights if type(x) == pd.Series])
    print(len(weights_buffer),"samples")
    weights_buffer.mean().plot.bar()
    return weights_buffer

def mp_optimize_weights(samples, data, **kw):
    return ProcessPoolExecutor().map(partial(optimize_weights, data), samples)

def optimize_weights(data, sample):
    data = data.loc[sample].dropna(axis=1, how='all')
    if data.shape[1] < 2:
        return
    guess = [1] * data.shape[1]
    bounds = [(0.0, 10.0)] * data.shape[1]

    def function(w, data, sample):
        wr = (w*data.loc[sample]).sum(axis=1)
        return -sortino(wr)

    result = minimize(function, guess, (data, sample),\
                      method = 'SLSQP',\
                      bounds = bounds,\
                      tol = 0.0001,\
                      constraints = {'type': 'eq', 'fun': lambda x:  data.shape[1] - sum(x)},\
                      options = {'eps': 1e-1},
                      )
    return pd.Series(result.x, index=data.columns)
