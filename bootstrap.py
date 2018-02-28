#!/usr/bin/env python
import trading.portfolio as portfolio
import config.portfolios


if __name__ == '__main__':
    p_learn = portfolio.Portfolio(instruments=config.portfolios.p_learn)
    p_test = portfolio.Portfolio(instruments=config.portfolios.p_test)
    p_trade = portfolio.Portfolio(instruments=config.portfolios.p_trade)

    p_learn_weights = p_learn.bootstrap_pool()
    print(p_learn_weights.mean(axis=1))

    p_test_weights = p_test.bootstrap_pool()
    print(p_test_weights.mean(axis=1))

    p_trade_weights = p_trade.bootstrap_pool()
    print(p_trade_weights.mean(axis=1))
