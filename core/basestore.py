from config.settings import quotes_storage


if quotes_storage == 'hdf5':
    from core.hdfstore import read_symbol, read_contract, write_data, drop_symbol
elif quotes_storage == 'mysql':
    from core.sqlstore import read_symbol, read_contract, write_data, drop_symbol
