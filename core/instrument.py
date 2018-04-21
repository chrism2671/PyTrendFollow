import datetime
from collections import deque
from functools import lru_cache
import sys
try:
    import config.strategy
except ImportError:
    print("You need to set up the strategy file at config/strategy.py.")
    sys.exit()
    
from core.currency import Currency
import numpy as np
import pandas as pd
from trading.accountcurve import accountCurve

import config.instruments
import config.settings
import trading.rules
from core import data_feed
from core.logger import get_logger
from core.utility import contract_to_tuple, cbot_month_code, generate_roll_progression, weight_forecast

logger = get_logger('instrument')


class Instrument(object):
    """
    Object representing a future instrument
    """

    @classmethod
    def load(self, instruments):
        """Loads all instruments in the system into a dict so that they can be easily worked with.
        
        For example:
        i = Instruments.load()  # Load all the instrument data to memory 
        i['corn'].contracts()   # Show the corn contracts

        """
        if instruments == None:
            return {v['name']: Instrument(**v) for v in config.instruments.instrument_definitions}
        else:
            j = {v['name']: v for v in config.instruments.instrument_definitions}
            return {k: Instrument(**j[k]) for k in instruments}

    def __repr__(self):
        return self.name

    def __init__(self, **kwargs):
        """Initialise the instrument object with defaults, taking overrides from the config/instruments.py file."""
        self.quandl_data_factor = 1                 # Sometimes Quandl data needs to be multiplied/transformed (e.g. MXP)
        self.backtest_from_contract = False         # Back test from contract (not date) e.g. 198512. Doesn't have to be a real contract.
        self.backtest_from_year = 1960
        self.roll_day = 14
        self.roll_shift = 0                         # Move the roll day forwards (nearer) by this many days if negative, and vice-versa
        self.expiration_month = 0
        self.expiry = 15                            # Default expiry day.
        self.commission = 2.50                      # Default broker commission.
        self.spread = 0                             # Default spread.
        self.denomination = 'USD'                   # Default demonination.
        self.months_traded = tuple(range(1, 13))    # 12 months
        self.trade_only = tuple(range(1, 13))       # 12 months
        self.rules = config.strategy.default_rules
        self.weights = config.strategy.rule_weights
        self.broker = 'ib'                          # Default broker
        self.bootstrapped_weights = None            # Only used to store results of bootstrapping. Not used for trading.
        self.first_contract = None
        self.contract_data = ['ib', 'quandl']

        # assign properties from instrument definition
        for key, value in kwargs.items():
            value = tuple(value) if (type(value) == list) else value
            setattr(self, key, value)

        self.weights = pd.DataFrame.from_dict(self.weights, orient='index').transpose().loc[0]
        self.currency = Currency(self.denomination + config.settings.base_currency)

    def calculate(self):
        """
        Utility function that calculates the position we want to be in for this contract.
        
        Called from Portfolio using multiprocessing to speed things up.
        
        Returns a dict with the important things we need in to trade with.
        """
        if self.currency.rate() is 1:
            rate = pd.Series(1, index=self.panama_prices().index)
        else:
            rate = self.currency.rate()
        return {
            'panama_prices': self.panama_prices(),
            'roll_progression': self.roll_progression(),
            'market_price': self.market_price(),
            'position': self.position(),
            'rate': rate,
            }

    def latest_price_date(self):
        """Gets the date of the latest price we've got, to help us calculate how old our data is."""
        current_contract = self.roll_progression().loc[datetime.date.today()]
        latest_price = self.contracts().loc[current_contract].tail(1)
        return latest_price.index[0] # latest price date

    def validate(self):
        """Validates that the instrument has up-to-date data and sane results."""
        d = {
            'is_valid': True,
            'today': pd.to_datetime(datetime.date.today()),
            'latest_price_date': None,
            'carry_forecast': np.nan,
            'weighted_forecast': np.nan,
            'panama_date': None,
            'vol': np.nan,
            'price_age': None,
            'currency_age': None,
            'panama_age': None,
        }
        try:
            # Do we have today's price?
            try:
                lpd = self.latest_price_date()
                d['latest_price_date'] = lpd
            except KeyError:
                raise Exception("couldn't obtain the latest price date")

            # Check carry is not zero
            try:
                d['carry_forecast'] = self.forecasts()['carry'].tail(1)[0]
            except KeyError:
                pass

            d['weighted_forecast'] = self.weighted_forecast().tail(1)[0]

            # Check panama_price data is not cached
            d['panama_date'] = self.panama_prices().tail(1).index[0]

            # Check volatility is not zero
            try:
                d['vol'] = self.return_volatility().loc[lpd]
            except KeyError:
                logger.warning("Validation for instrument %s failed: no volatility data" % self.name)
                d['is_valid'] = False
                return d

            d['price_age'] = (d['today'] - d['latest_price_date']).days
            d['currency_age'] = self.currency.age()
            d['panama_age'] = (d['today'] - d['panama_date']).days
        except Exception as e:
            logger.warning("Validation for instrument %s failed: %s" % (self.name, str(e)))
            d['is_valid'] = False
        finally:
            return d

### Pricing

    def pp(self, **kw):
        """
        Shorthand function for Instrument.panama_prices()
        """
        return self.panama_prices(**kw)

    def rp(self, **kw):
        """
        Shorthand function for Instrument.roll_progression()
        """
        return self.roll_progression(**kw)

    @lru_cache(maxsize=1)
    def panama_prices(self):
        """
        Returns a Series representing a 'continuous future' time series.
        Our system uses the simplest method - 'panama stitching'. 
        Absolute prices won't make any sense, but daily returns and trends are preserved. Perfectly suitable for our purposes.
        """
        return self.contracts()['close'].diff().to_frame().swaplevel().fillna(0).join(
            self.rp().to_frame().set_index('contract',append=True), how='inner').\
            reset_index('contract',drop=True)['close'].cumsum().rename(self.name)

    @lru_cache(maxsize=2)
    def return_volatility(self, **kw):
        """
        Returns a Series with the EWA volatility of returns for this instrument.
        """
        return (self.panama_prices() * self.point_value).\
               diff().ewm(span=36, min_periods=36).std() * self.currency.rate(**kw)

    def market_price(self):
        """
        Returns a series of real market prices for this contract.
        """
        return self.roll_progression().to_frame().set_index('contract', append=True).\
               swaplevel().join(self.contracts())['close'].swaplevel().dropna()

### Forecast & Position

    def position(self, capital=config.strategy.capital, forecasts=None, nofx=False):
        """
        The ideal position with the current capital, rules set and strategy
        """
        if forecasts is None:
            forecasts = self.weighted_forecast()
        position = (forecasts * (config.strategy.daily_volatility_target * capital / 10))\
            .divide(self.return_volatility(nofx=nofx)[forecasts.index], axis=0)
        return np.around(position)

    @lru_cache(maxsize=8)
    def forecasts(self, rules=None):
        """
        Position forecasts for individual trading rules
        """
        if rules is None:
            rules = self.rules
        return pd.concat(list(map(lambda x: getattr(trading.rules, str(x))(self), rules)), axis=1).dropna()

    def weighted_forecast(self, rules=None):
        """
        Returns a Series representing the weighted forecast for this instrument.
        """
        return weight_forecast(self.forecasts(rules=rules), self.weights)

    @lru_cache(maxsize=8)
    def forecast_returns(self, **kw):
        """
        Estimated returns for individual trading rules
        """
        f = self.forecasts(**kw)
        positions = self.position(forecasts=f).dropna()
        curves = positions.apply(lambda x: accountCurve([self], positions=x,
                                 panama_prices = self.panama_prices()))
        return curves.apply(lambda x: x.returns()[self.name]).transpose()

    @lru_cache(maxsize=8)
    def contract_format(self, contract):
        """
        Convert the contract label to the broker's format
        """
        year, month = contract_to_tuple(contract)
        return self.quandl_symbol + cbot_month_code(month) + str(year)

    @lru_cache(maxsize=8)
    def next_contract(self, contract, months=None, reverse=False):
        """
        Given a contract, return the next contract in the sequence.
        """
        if months == None:
            months = self.months_traded
        rot = 1 if reverse else -1
        m_idx = 0 if reverse else -1
        d = pd.to_datetime(str(contract), format='%Y%m')
        months_traded = deque(months)
        months_traded.rotate(rot)
        output_month = months_traded[months.index(d.month)]
        output_year = d.year + (d.month == months[m_idx]) * (-rot)
        return int(str(output_year) + str("%02d" % (output_month,)))

    @lru_cache(maxsize=8)
    def contracts(self, **kw_in):
        """
        Returns the filtered contract data.
        
        Defaults:
        active_only = True  # Only returns days with actual trading volume
        trade_only = True   # Only return contracts we have defined as 'trade_only' in instruments.py
        recent_only = False # Only return contracts from the last 3 years until today.
        
        """
        kw = dict(active_only=True, trade_only=True, recent_only=False)
        kw.update(kw_in)

        data = data_feed.get_instrument(self)

        if data is not None:
            year = data.index.to_frame()['contract'] // 100
            if self.backtest_from_contract:
                data = data.loc[self.backtest_from_contract:]
            if self.backtest_from_year:
                data = data[year >= self.backtest_from_year]
            if kw['recent_only']:
                data = data[year > datetime.datetime.now().year-3]
            if kw['trade_only']:
                month = data.index.to_frame()['contract'] % 100
                data = data[month.isin(self.trade_only)]
            if kw['active_only']:
                data = data[(data['open'] > 0) & (data['volume'] > 1)]
        return data

### Rolling

    def term_structure(self, date=None):
        if date is None:
            date = self.pp().tail(1).index[0]
        return self.contracts(active_only=False).xs(date, level=1)

    def contract_volumes(self):
        return self.contracts(active_only=False).groupby(level=0)['volume'].mean().plot.bar()

    @lru_cache(maxsize=1)
    def roll_progression(self):
        """
        Lists the contracts the system wants to be in depending on the date
        """
        return generate_roll_progression(self.roll_day, self.trade_only,
                                         self.roll_shift + self.expiration_month * 30)

    def expiry_date(self, contract):
        """
        Returns the estimated expiry date for this contract.
        """
        year, month = contract_to_tuple(contract)
        return datetime.date(year, month, self.expiry)

    def expiries(self):
        """
        Returns a Series of expiry dates for every day in the roll progression.  
        
        """
        return pd.to_datetime(self.roll_progression().apply(self.expiry_date))

    def time_to_expiry(self):
        """
        Returns approximate number of days until the contract expires.
        Used to calculate carry when comparing a contract with the spot price underneath (e.g. a currency future and the underly exchange rate)
        """
        return (self.expiries() - self.expiries().index).apply(lambda x: getattr(x, 'days'))

    def plot_contracts(self, start, finish, panama=True):
        """ Function to help visualise individual contract data and stitching. Not used for trading. """
        if panama is False:
            self.roll_progression().to_frame().set_index('contract', append=True).swaplevel().\
                join(self.contracts(active_only=False)).reset_index().\
                pivot(index='date', columns='contract', values='close')[start:finish].\
                dropna(how='all',axis=1).plot()
        else:
            df = self.panama_prices().to_frame()
            r = self.roll_progression().to_frame()
            df = df.join(r, how='inner')
            return df[start:finish].pivot(columns='contract', values=self.name).plot()

    def curve(self, **kw):
        return accountCurve([self], **kw)

### Bootstrap

    def bootstrap(self, **kw):
        """
        Optimize rule weights using bootstrapping
        """
        import trading.bootstrap
        print("Bootstrap", self.name, "starting")
        result = trading.bootstrap.bootstrap(self, **kw)
        self.bootstrapped_weights = result.mean()
        print(accountCurve([self], positions=self.position(forecasts=weight_forecast(
              self.forecasts(), result.mean())), panama_prices=self.panama_prices()))
        return result.mean()

    def cache_clear(self):
        self.forecasts.cache_clear()
        self.roll_progression.cache_clear()
        self.panama_prices.cache_clear()
        self.return_volatility.cache_clear()
        self.forecast_returns.cache_clear()
        self.contract_format.cache_clear()
        self.next_contract.cache_clear()
