#!/usr/bin/env python
import trading.portfolio
import config.portfolios
import logging
import traceback
import sys
logger = logging.getLogger('validate')


if __name__ == "__main__":
    p = trading.portfolio.Portfolio(instruments=config.portfolios.p_trade)
    try:
        validate = p.validate()[['carry_forecast', 'currency_age', 'panama_age', 'price_age', 'weighted_forecast']]
        print(validate)
    except KeyboardInterrupt:
        print("Shutdown requested...exiting")
    except Exception:
        traceback.print_exc(file=sys.stdout)
        sys.exit(0)

