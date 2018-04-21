import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pprint
import config.settings
import config.strategy
from core.utility import chunk_trades, sharpe, drawdown
from multiprocessing_on_dill import Pool #, Process, Manager
from contextlib import closing


class accountCurve():
    """
    Account curve object for Portfolio and Instrument.
    
    Calculates the positions we want to be in, based on the volatility target.
    """
    def __init__(self, portfolio, capital=500000, positions=None, panama_prices=None, nofx=False, portfolio_weights = 1, **kw):
        self.portfolio = portfolio
        self.nofx = nofx
        self.weights = portfolio_weights
        self.multiproc = kw.get('multiproc', True)
        # If working on one instrument, put it in a list
        if not isinstance(portfolio, list):
            self.portfolio = [self.portfolio]

        if isinstance(positions, pd.Series):
            positions = positions.rename(self.portfolio[0].name)

        self.capital = capital
        self.panama = panama_prices

        if positions is None:
            self.positions = self.instrument_positions()
            self.positions = self.positions.multiply(self.weights)
        else:
            self.positions = pd.DataFrame(positions)

        # Reduce all our positions so that they fit inside our target volatility when combined.
        self.positions = self.positions.multiply(self.vol_norm(),axis=0)

        # If we run out of data (for example, if the data feed is stopped), hold position for 5 trading days and then close.
        # chunk_trades() is a function that is designed to reduce the amount of trading (and hence cost)

        self.positions = chunk_trades(self.positions).ffill(limit=5).fillna(0)

    def __repr__(self):
        """
        Returns a formatted list of statistics about the account curve.
        """
        return pprint.pformat(self.stats_list())

    def inst_calc(self):
        """Calculate all the things we need on all the instruments and cache it."""
        try:
            return self.memo_inst_calc
        except:
            if len(self.portfolio)>1 and self.multiproc:
                with closing(Pool()) as pool:
                    self.memo_inst_calc = dict(pool.map(lambda x: (x.name, x.calculate()), self.portfolio))
            else:
                self.memo_inst_calc = dict(map(lambda x: (x.name, x.calculate()), self.portfolio))
            return self.memo_inst_calc

    def instrument_positions(self):
        """Position returned by the instrument objects, not the final position in the portfolio"""
        try:
            return self.memo_instrument_positions
        except:
            self.memo_instrument_positions = pd.DataFrame({k: v['position'] for k, v in self.inst_calc().items()})
            return self.memo_instrument_positions

    def rates(self):
        """
        Returns a Series or DataFrame of exchange rates.
        """
        if self.nofx==True:
            return 1
        try:
            return self.memo_rates
        except:
            self.memo_rates = pd.DataFrame({k: v['rate'] for k, v in self.inst_calc().items()})
            return self.memo_rates

    def stats_list(self):
        stats_list = ["sharpe",
                      "gross_sharpe",
                      "annual_vol",
                      "sortino",
                      "cap",
                      "avg_drawdown",
                      "worst_drawdown",
                      "time_in_drawdown",
                      "calmar",
                      "avg_return_to_drawdown"]
        return {k: getattr(self, k)() for k in stats_list}

    def returns(self):
        """
        Returns a Series/Frame of net returns after commissions, spreads and estimated slippage.
        """
        return self.position_returns() + self.transaction_returns() + self.commissions() + self.spreads()

    def position_returns(self):
        """The returns from holding the portfolio we had yesterday"""
        # We shift back 2, as self.positions is the frontier - tomorrow's ideal position.
        return (self.positions.shift(2).multiply((self.panama_prices()).diff(), axis=0).fillna(0) * self.point_values()) * self.rates()

    def transaction_returns(self):
        """Estimated returns from transactions including slippage. Uses the average settlement price of the last two days"""
        # self.positions.diff().shift(1) = today's trades
        slippage_multiplier = .5
        return (self.positions.diff().shift(1).multiply((self.panama_prices()).diff()*slippage_multiplier, axis=0).fillna(0) * self.point_values()) * self.rates()

    def commissions(self):
        commissions = pd.Series({v.name: v.commission for v in self.portfolio})
        return (self.positions.diff().shift(1).multiply(commissions)).fillna(0).abs()*-1

    def spreads(self):
        spreads = pd.Series({v.name: v.spread for v in self.portfolio})
        return (self.positions.diff().shift(1).multiply(spreads * self.point_values() * self.rates())).fillna(0).abs()*-1

    def vol_norm(self):
        return (config.strategy.daily_volatility_target * self.capital / \
                (self.returns().sum(axis=1).shift(2).ewm(span=50).std())).clip(0,1.5)

    def panama_prices(self):
        if self.panama is not None:
            return pd.DataFrame(self.panama)
        else:
            try:
                return self.memo_panama_prices
            except:
                self.memo_panama_prices =  pd.DataFrame({k: v['panama_prices'] for k, v in self.inst_calc().items()})
                return self.memo_panama_prices

    def point_values(self):
        return pd.Series({v.name: v.point_value for v in self.portfolio})

    def gross_sharpe(self):
        return sharpe(np.trim_zeros((self.position_returns() - self.transaction_returns()).sum(axis=1)))

    def sharpe(self):
        return sharpe(np.trim_zeros(self.returns().sum(axis=1)))

    def losses(self):
        return [z for z in np.trim_zeros(self.returns()).sum(axis=1) if z<0]

    def sortino(self):
        return np.trim_zeros(self.returns().sum(axis=1)).mean()/np.std(self.losses())*np.sqrt(252)

    def annual_vol(self):
        return "{0:,.4f}".format(np.trim_zeros(self.returns()).sum(axis=1).std() * np.sqrt(252)/self.capital)

    def plot(self):
        fig, axes = plt.subplots(nrows=1, ncols=1)
        # self.returns.cumsum().plot(ax=axes[0])
        ar = self.annual_returns()
        ar.plot.bar(ax=axes, figsize=(12,1.5))
        axes.set_xlabel("")
        axes.set_xticklabels([dt.strftime('%Y') for dt in ar.index.to_pydatetime()])

    def annual_returns(self):
        return np.trim_zeros(self.returns().sum(axis=1).resample(rule='A').sum()/self.capital * 100)

    def annual_sharpes(self):
        return self.returns().sum(axis=1).resample(rule='A').sum()/(self.returns().sum(axis=1).resample(rule='A').std() * np.sqrt(252))

    def drawdown(self):
        return drawdown(self.returns().sum(axis=1).cumsum())

    def avg_drawdown(self):
        dd = self.drawdown()
        # return "{0:,.0f}".format(np.nanmean(dd.values))
        return np.nanmean(dd.values)/self.capital

    def worst_drawdown(self):
        dd = self.drawdown()
        # return "{0:,.0f}".format(np.nanmin(dd.values))
        return np.nanmin(dd.values)/self.capital
    def cap(self):
        return self.capital

    def time_in_drawdown(self):
        dd = self.drawdown()
        dd = [z for z in dd.values if not np.isnan(z)]
        in_dd = float(len([z for z in dd if z < 0]))
        return "{0:,.4f}".format(in_dd / float(len(dd)))

    def instrument_count(self):
        return np.maximum.accumulate((~np.isnan(self.panama_prices())).sum(axis=1)).plot()

    def underwater(self):
        r = self.returns().sum(axis=1)
        u = (r.cumsum() - r.cumsum().cummax())/self.capital
        return np.trim_zeros(u).plot()

    def cumcapital(self):
        return np.trim_zeros((self.returns().sum(axis=1)/self.capital)+1).cumprod()

    def calmar(self):
        return self.annual_returns().mean() * 0.01 / -self.worst_drawdown()

    def avg_return_to_drawdown(self):
        return self.annual_returns().mean() * 0.01 / -self.avg_drawdown()
