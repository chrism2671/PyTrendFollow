from config.settings import quotes_storage

"""
This file imports data read/write methods for a local storage depending on the user's choice.
These methods are used in core.contract_store.  
"""

if quotes_storage == 'hdf5':
    from core.hdfstore import read_symbol, read_contract, write_data, drop_symbol