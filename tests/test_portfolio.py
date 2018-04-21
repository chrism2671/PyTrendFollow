import trading.portfolio as portfolio
import trading.accountcurve as accountcurve
import config.portfolios
import config.settings


"""
Set of basic tests for portfolio.
Should pass only if historical data are downloaded and up to date.
"""


def test_curve():
    p = portfolio.Portfolio(instruments=config.portfolios.p_trade)
    a = p.curve()
    assert type(a) is accountcurve.accountCurve
    assert a.sharpe() > 0


def test_validate():
    p = portfolio.Portfolio(instruments=config.portfolios.p_trade)
    v = p.validate()
    assert v['is_valid'].any()


def test_frontier():
    p = portfolio.Portfolio(instruments=config.portfolios.p_trade)
    f = p.frontier()
    assert not f.empty
