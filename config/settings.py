import quandl
import logging
import os


# The currency of your broker account
base_currency = 'GBP'
# List of data sources used to pull quotes
# Comment a line to disable a data source globally
data_sources = [
    'ib',       # Interactive Brokers (interactivebrokers.com)
    'quandl',   # quandl.com
]

# Backend to be used as a storage for downloaded quotes. Possible values: 'hdf5'
# HDF5 storage only requires pandas
quotes_storage = 'hdf5'

quandl.ApiConfig.api_key = "QUANDL_API_KEY"

# Port configured in IB TWS or Gateway terminal
ib_port = 4001

# Insert MongoDB connection string if you use it as IB logs storage (optional)
iblog_host = "mongodb://[username]:[password]@[host]:[port]"
hdf_path = os.path.expanduser('../data/quotes')

# Define logging settings
console_logger = {
    'enabled': True,
    'level': logging.INFO,
}
file_logger = {
    'enabled': True,
    'level': logging.DEBUG,
    'file_name': os.path.expanduser('~/logs/pytrendfollow.log')
}
