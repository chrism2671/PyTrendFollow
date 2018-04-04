import numpy as np
import pandas as pd
from functools import lru_cache
import sys

try:
    import config.strategy
except ImportError:
    print("You need to set up the strategy file at config/strategy.py.")
    sys.exit()
    
try:
    import config.settings
except ImportError:
    print("You need to set up the settings file at config/settings.py.")
    sys.exit()
    
from core.instrument import Instrument
from core.utility import draw_sample, sharpe
from trading.accountcurve import accountCurve
import trading.bootstrap_portfolio as bp
import seaborn
import pyprind
from multiprocessing_on_dill import Pool
from contextlib import closing
from core.logger import get_logger
logger = get_logger('portfolio')


class Portfolio(object):
    """
    Portfolio is an object that is a group of instruments, with calculated positions based on the weighting and volatility target.
    """

    def __init__(self, weights=1, instruments=None):
        self.instruments = Instrument.load(instruments)
        self.weights = pd.Series(config.strategy.portfolio_weights)
        # remove weights for instruments that aren't in the portfolio
        self.weights = self.weights[self.weights.index.isin(instruments)]
        # instruments blacklisted due to validation errors
        self.inst_blacklist = []

    def __repr__(self):
        return str(len(self.valid_instruments())) + " instruments"

    @lru_cache(maxsize=8)
    def curve(self, **kw):
        """
        Returns an AccountCurve for this Portfolio.
        """
        kw2={'portfolio_weights': self.valid_weights()}
        kw2.update(kw)
        return accountCurve(list(self.valid_instruments().values()), **kw2)

    def valid_instruments(self):
        return dict([i for i in self.instruments.items() if i[1].name not in self.inst_blacklist])

    def valid_weights(self):
        return self.weights[~self.weights.index.isin(self.inst_blacklist)]

### Utility Functions ###################################################################

    def validate(self):
        """
        Runs Instrument.validate for every Instrument in the Portfolio and returns a DataFrame. Used for trading.
        """
        import concurrent.futures
        bar = pyprind.ProgBar(len(self.instruments.values()), title='Validating instruments')
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            dl = {executor.submit(x.validate): x.name for x in self.instruments.values()}
            d = {}
            for fut in concurrent.futures.as_completed(dl):
                bar.update(item_id=dl[fut])
                d[dl[fut]] = fut.result()
            d = pd.DataFrame(d).transpose()
        # blacklist instruments that didn't pass validation
        self.inst_blacklist = d[d['is_valid'] == False].index.tolist()
        return d

    def instrument_stats(self):
        """
        Returns individual metrics for every Instrument in the Portfolio. Not used for trading, just for research.
        """
        with closing(Pool()) as pool:
            df = pd.DataFrame(dict(pool.map(lambda x: (x.name, x.curve().stats_list()),
                                            self.valid_instruments().values()))).transpose()
        return df

    @lru_cache(maxsize=1)
    def inst_calc(self):
        """
        Calculate the base positions for every instrument, before applying portfolio-wide weighting and volatility scaling.
        """
        with closing(Pool()) as pool:
            d = dict(pool.map(lambda x: (x.name, x.calculate()), self.valid_instruments().values()))
        return d

    @lru_cache(maxsize=1)
    def panama_prices(self):
        """
        Returns a dataframe with the panama prices of every instrument. Not used for trading.
        """
        return pd.DataFrame({k: v['panama_prices'] for k, v in self.inst_calc().items()})

    def point_values(self):
        """
        Returns a series with the point values of every instrument. Not used for trading.
        """
        return pd.Series({k: v.point_value for k, v in self.valid_instruments().items()})

    def corr(self):
        """
        Returns a correlation matrix of the all the instruments with trading rules applied, with returns bagged by week.
        
        Not used for trading. Intended to be used in Jupyter.
        """
        df = self.curve().returns().resample('W').sum().replace(0, np.nan).corr()
        cm = seaborn.dark_palette("green", as_cmap=True)
        s = df.style.background_gradient(cmap=cm)
        return s

    def corr_pp(self):
        """
        Returns a correlation matrix of all the instruments panama prices, with returns bagged by week.
        
        Not used for trading. Intended to be used in Jupyter.
        """
        
        df = self.panama_prices().diff().resample('W').sum().corr()
        cm = seaborn.dark_palette("green", as_cmap=True)
        s = df.style.background_gradient(cmap=cm)
        return s

    def cov(self):
        """
        Returns the covariance matrix of all the instruments with trading rules applied.
        
        Not used for trading. Intended to be used in Jupyter.
        """
        return self.curve().returns().resample('W').sum().cov()

    def plot(self):
        """
        Returns a plot of the cumulative returns of the Portfolio 
        """
        return self.returns().sum(axis=1).cumsum().plot()

    def instrument_count(self):
        """
        The number of instruments being traded by date.
        """
        return np.maximum.accumulate((~np.isnan(self.panama_prices())).sum(axis=1)).plot()

    def bootstrap_pool(self, **kw):
        """
        Bootstrap forecast weights using all the data in the portfolio.
        """
        # m = ProcessPoolExecutor().map(lambda x: x.bootstrap(), self.instruments.values())
        bs = {k: v.bootstrap(**kw) for k, v in self.valid_instruments().items()}
        self.bs = pd.DataFrame.from_dict(bs)
        self.bs.mean(axis=1).plot.bar(yerr=self.bs.std(axis=1))
        return self.bs

    def bootstrap_portfolio(self, **kw):
        """
        Bootstrap the instrument weights to work out the 'best' combination of instruments to use.
        
        Strongly advise against using this and to handcraft instrument weights yourself as this method may lead to overfitting. 
        """
        self.bp_weights = bp.bootstrap(self, **kw)
        return self.bp_weights

    def bootstrap_rules(self, n=10000, **kw):
        z = self.forecast_returns(**kw)
        a = pd.Series({k: v.shape[0] for k, v in z.items()})
        b=(a/a.sum())
        sharpes = []
        corrs = {}
        for k, v in b.iteritems():
            for x in range(0,int(round(v*n))):
                sample = draw_sample(z[k], 252)
                sharpes.append(sharpe(sample).rename(k))
                corrs[(k, x)] = (sample.resample('W').sum().corr())
        return pd.DataFrame(sharpes), pd.Panel(corrs).mean(axis=0)

    @lru_cache(maxsize=1)
    def forecast_returns(self, **kw):
        """Get the returns for individual forecasts for each instrument, useful for bootstrapping
           forecast Sharpe ratios"""
        with closing(Pool()) as pool:
            d = dict(pool.map(lambda x: (x.name, x.forecast_returns(**kw).dropna()),
                              self.valid_instruments().values()))
        return d

    @lru_cache(maxsize=1)
    def forecasts(self, **kw):
        """
        Returns a dict of forecasts for every Instrument in the Portfolio.
        """
        with closing(Pool()) as pool:
            d = dict(pool.map(lambda x: (x.name, x.forecasts(**kw)),
                              self.valid_instruments().values()))
        return d

    @lru_cache(maxsize=1)
    def weighted_forecasts(self, **kw):
        """
        Returns a dict of weighted forecasts for every Instrument in the Portfolio.
        """
        with closing(Pool()) as pool:
            d = dict(pool.map(lambda x: (x.name, x.weighted_forecast(**kw)),
                              self.valid_instruments().values()))
        return d

    @lru_cache(maxsize=1)
    def market_prices(self):
        """
        Returns the market prices of the instruments, for the currently traded contract.
        """
        mp = pd.DataFrame({k: v['market_price'] for k, v in self.inst_calc().items()}).ffill()
        return pd.DataFrame({k: mp[k].loc[self.inst_calc()[k]['roll_progression']\
                              .to_frame().set_index('contract', append=True).index].reset_index(
                              'contract', drop=True) for k in self.valid_instruments().keys()})

    def ibcode_to_inst(self, ib_code):
        """
        Take an IB symbol and return the Instrument.
        """
        a = {k.ib_code: k for k in self.instruments.values()}
        try:
            return a[ib_code]
        except KeyError:
            logger.warn('Ignoring mystery instrument in IB portfolio ' + ib_code)
            return None

    def frontier(self, capital=500000):
        """
        Returns a DataFrame of positions we want for a given capital. The last line represents today's trade.
        """
        c = self.curve(capital=capital)
        f = c.positions.tail(1).iloc[0]
        f.index = pd.MultiIndex.from_tuples([(k, str(c.inst_calc()[k]['roll_progression'].
                                                     tail(1)[0])) for k in f.index])
        f.rename('frontier', inplace=True)
        f = f.to_frame()
        f.index = pd.MultiIndex.from_tuples(f.index.map(
                                        lambda x: (self.valid_instruments()[x[0]].ib_code, x[1])))
        f.index = f.index.rename(['instrument', 'contract'])
        return f

    def cache_clear(self):
        """
        Clear the functools.lru_cache
        """
        self.inst_calc.cache_clear()
        self.panama_prices.cache_clear()
        self.forecasts.cache_clear()
        self.market_prices.cache_clear()
        self.forecast_returns.cache_clear()
        self.curve.cache_clear()
        [v.cache_clear() for v in self.instruments.values()]
        logger.info("Portfolio LRU Cache cleared")
