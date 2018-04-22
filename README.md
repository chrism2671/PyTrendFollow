# PyTrendFollow - Systematic Futures Trading using Trend Following

## Introduction

This program trades futures using a systematic trend following strategy, similar to most managed
 futures hedge funds. It produces returns of around ~20% per year, based on a volatility of 25%.
  You can read more about trend following in the /docs folder. Start with [introduction to trend following](https://github.com/chrism2671/PyTrendFollow/blob/master/docs/Introduction%20to%20Trend%20Following.ipynb). If you just want to play with futures data, see [working with prices](https://github.com/chrism2671/PyTrendFollow/blob/master/docs/Working%20with%20Prices.ipynb).

## Features
* Integration with Interactive Brokers for fully automated trading.
* Automatic downloading of contract data from Quandl & Interactive Brokers.
* Automatic rolling of futures contracts.
* Trading strategies backtesting on historical data
* Designed to use Jupyter notebook as an R&D environment.

## Installation

### Data sources

The system supports downloading of price data from
 1. [Quandl](https://www.quandl.com/)
 1. [Interactive Brokers](https://www.interactivebrokers.com) (IB)

It is recommended (though not required) to have data subscriptions for both Quandl and IB.
 Quandl has more historical contracts and works well for backtesting,
 while IB data is usually updated more frequently and is better for live trading.

To use MySQL as the data storage backend (optional, default is HDF5), you'll need a configured
 server with privileges to create databases and tables.

### Trading

For automated trading with Interactive Brokers, install the latest
 [TWS terminal](https://www.interactivebrokers.com/en/index.php?f=16040)
   or [Gateway](https://www.interactivebrokers.com/en/index.php?f=16457). You'll need to enable the API and set it to port 4001.

### Code

1. Python version 3.* is required
1. Get the code:

    `git clone https://github.com/chrism2671/PyTrendFollow.git`

    `cd PyTrendFollow`
1. Install requirements:
    * install python tkinter (for Linux it's usually present in a distribution repository, e.g.
      for Ubuntu: `apt-get install python3-tk`) if necessary.
    * To compile the binary version of [arch](https://pypi.org/project/arch/4.0/), you will need the
      development lirbary for your version of Python. e.g., for Python 3.5 on Ubuntu, use 
      `apt-get install libpython3.5-dev`.
    * install Python requirements: `pip3 install -r requirements.txt`
1. `cp config/settings.py.template config/settings.py`, update the settings file with your API keys,
 data path, etc. If you don't use one of the data sources (IB or Quandl), comment the corresponding
 line in `data_sources`.
1. `cp config/strategy.py.template config/strategy.py`, review and update the strategy parameters if
 necessary

## Usage

Before you start, run the IB TWS terminal or Gateway, go to `Edit -> Global configuration -> API ->
Settings` and make sure that `Socket port` matches the value of `ib_port` in your local
 `config/settings.py` file (default value is 4001).

* To download contracts data from IB and Quandl:

    `python download.py quandl --concurrent`

    `python download.py ib`

    Use the `--concurrent` flag only if you have the concurrent downloads feature enabled on Quandl,
 otherwise you'll hit API requests limit.

* After the download has completed, make sure the data is valid:

    `python validate.py`

    The output of this script should be a table showing if the data for each instrument in the
    portfolio is not corrupted, is up to date and some other useful information.

* To place your market orders now, update positions and quit:

    `python scheduler.py --now --quit`

* To schedule portfolio update for a specific time every day:

    `python scheduler.py`

For more details on how the system works and how to experiment with it, check out the `docs/`
 directory.

## Status, Disclaimers etc

This project is dev status. Use at your own risk. We are looking for contributors and bug fixes.

Only tested on Linux for now, but should work on Windows / MacOS.

## Acknowledgements

This project is based heavily on the work of Rob Carver & the
 [PySystemTrade](https://github.com/robcarver17/pysystemtrade) project.
