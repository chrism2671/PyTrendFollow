from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, Date, REAL, MetaData
from sqlalchemy.dialects.mysql import insert
import sqlalchemy.types as types
from config.settings import mysql_host
import pandas as pd


class DateWrapper(types.TypeDecorator):
    """
    This class converts pandas.datetime to datetime.date before writing to database
    because MySQL-connector doesn't support it
    """
    impl = types.DateTime
    def process_bind_param(self, value, dialect):
        if isinstance(value, pd.datetime) or isinstance(value, pd.Timestamp):
            value = value.date()
        return value


# Connect to the mysql database or create if not exists
db_name = 'quotesdb'
mysql_engine = create_engine(mysql_host)
mysql_engine.execute('create database if not exists `%s`' % db_name)
db_engine = create_engine('%s/%s' % (mysql_host, db_name))
metadata = MetaData()

# Define tables and create if not exist
# Each MySQL table is named according to the quotes type e.g. futures, currency, indices

futures = Table('futures', metadata,
        Column('provider', String(20), nullable=False, primary_key=True),
        Column('symbol', String(50), nullable=False, primary_key=True),
        Column('contract', Integer, nullable=False, primary_key=True),
        Column('date', DateWrapper, nullable=False, primary_key=True),
        Column('open', REAL),
        Column('high', REAL),
        Column('low', REAL),
        Column('close', REAL),
        Column('volume', REAL),
        mysql_charset='utf8',
        mysql_engine='InnoDB'
)

currency = Table('currency', metadata,
        Column('provider', String(20), nullable=False, primary_key=True),
        Column('symbol', String(50), nullable=False, primary_key=True),
        Column('date', DateWrapper, nullable=False, primary_key=True),
        Column('rate', REAL),
        Column('high', REAL),
        Column('low', REAL),
        mysql_charset='utf8',
        mysql_engine='InnoDB'
)

others = Table('others', metadata,
       Column('provider', String(20), nullable=False, primary_key=True),
       Column('symbol', String(50), nullable=False, primary_key=True),
       Column('date', DateWrapper, nullable=False, primary_key=True),
       Column('close', REAL),
       mysql_charset='utf8',
       mysql_engine='InnoDB'
)

metadata.create_all(db_engine)

# Using raw SQL + pd.read_sql for simple read queries since it appears simpler than using
# sqlalchemy objects

def read_contract(symbol, contract, provider):
    """
    Read a single contract for a futures instrument
    :param provider:  data provider name
    :param symbol: symbol key (str)
    :param contract: contract label (int or str)
    :return: pd.DataFrame
    """
    db_engine.dispose()
    with db_engine.connect() as conn:
        return pd.read_sql("select * from `futures` where `symbol`='%s' and `contract`=%d and "
                       "provider='%s'" % (symbol, int(contract), provider), conn).\
                        set_index(['contract', 'date']).sort_index(level=0)


def read_symbol(symbol, q_type, provider):
    """
    Read all data from storage for a given symbol
    :param symbol: symbol key (str)
    :param q_type: quotes data type (i.e. MySQL table name)
    :param provider:  data provider name
    :return: data provider name
    """
    db_engine.dispose()
    with db_engine.connect() as conn:
        data = pd.read_sql("select * from `%s` where `provider`='%s' and `symbol`='%s'" %
                        (q_type, provider, symbol), conn)
    idx = ['contract', 'date'] if q_type == 'futures' else ['date']
    lvl = 0 if q_type == 'futures' else None
    return data.set_index(idx).sort_index(level=lvl)


def write_data(data, symbol, q_type, provider):
    """
    Writes a dataframe to MySQL table. On key collisions update will be performed.
    The actual SQL statement to be executed is:
    "insert into <table_name> (<columns>) values <...>
        on duplicate key update <column>=values(<column>)"
    :param data: pd.DataFrame
    :param symbol: symbol key (str)
    :param q_type: quotes data type (i.e. MySQL table name)
    :param provider: data provider name
    :return: expression result object
    """
    # pick the table according to the provider and type
    tbl = metadata.tables[q_type]
    data['provider'] = pd.Series(provider, index=data.index)
    data['symbol'] = pd.Series(symbol, index=data.index)
    # replace NaNs with None because SQL doesn't support them
    data = data.astype(object).where(pd.notnull(data), None)
    vals = data.to_dict(orient='records')
    insert_stmt = insert(tbl).values(vals)
    kw = {x.name: insert_stmt.inserted[x.name] for x in tbl.columns}
    on_duplicate_stmt = insert_stmt.on_duplicate_key_update(**kw)
    db_engine.dispose()
    with db_engine.connect() as conn:
        return conn.execute(on_duplicate_stmt)


def drop_symbol(symbol, q_type, provider):
    """
    Delete all rows from the table q_type that match the given provider and symbol keys
    :param symbol: symbol key (str)
    :param q_type: quotes data type (i.e. MySQL table name)
    :param provider: data provider name
    :return: expression result object
    """
    tbl = metadata.tables[q_type]
    del_stmt = tbl.delete().where(tbl.c.provider == provider).where(tbl.c.symbol == symbol)
    db_engine.dispose()
    with db_engine.connect() as conn:
        return conn.execute(del_stmt)