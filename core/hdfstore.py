import pandas as pd
from config.settings import hdf_path
import os
import threading

"""
Implementation of the local storage with HDF5 files as the backend.

* Directories structure for HDF storage:  "[settings.hdf_path]/[provider]/[q_type]/[symbol].h5"
* For q_type == 'futures' returnes DataFrame with the multi-index ['contract', 'date'],
  for other quotes types index is ['date']
"""
lock = threading.Lock()


def fname(symbol, q_type, provider):
    """
    Resolve the HDF file name for the given store symbol
    """
    fname = os.path.join(hdf_path, provider, q_type, symbol + '.h5')
    # create directories
    if not os.path.exists(os.path.dirname(fname)):
        os.makedirs(os.path.dirname(fname))
    return fname


def read_contract(symbol, contract, provider):
    """
    Read a single contract for a future instrument
    """
    if os.path.exists(fname(symbol, 'futures', provider)):
        data = pd.read_hdf(fname(symbol, 'futures', provider), 'quotes')
        return data.loc[int(contract), :]
    else:
        c = ['contract', 'date']
        return pd.DataFrame(columns=c).set_index(c)


def read_symbol(symbol, q_type, provider):
    """
    Read data from the corresponding HDF file
    """
    if os.path.exists(fname(symbol, q_type, provider)):
        data = pd.read_hdf(fname(symbol, q_type, provider), 'quotes')
        return data
    else:  # if symbol doesn't exist, an empty df is returned with only index columns
        c = ['contract', 'date'] if q_type == 'futures' else ['date']
        return pd.DataFrame(columns=c).set_index(c)


def write_data(data, symbol, q_type, provider):
    """
    Writes a dataframe to HDF file. If the symbol exists, the existing data will be updated
    on dataframe keys, favoring new data.
    """
    # Use a thread lock here to ensure the read+merge+write operation is atomic
    # and HDF files won't get corrupted on concurrent downloading
    lock.acquire()
    existing_data = read_symbol(symbol, q_type, provider)
    idx = ['contract', 'date'] if q_type == 'futures' else ['date']
    lvl = 0 if q_type == 'futures' else None
    data = data.set_index(idx)
    new_data = data.combine_first(existing_data).sort_index(level=lvl)
    new_data.to_hdf(fname(symbol, q_type, provider), 'quotes', mode='w', format='f')
    lock.release()


def drop_symbol(symbol, q_type, provider):
    """
    Remove the corresponding HDF file
    """
    os.remove(fname(symbol, q_type, provider))
