import pymongo
from config.settings import iblog_host
import pandas as pd

db_name = 'iblog'
order_col = 'order'
exec_col = 'execution'
cr_col = 'commission_report'
sum_col = 'account_summary'
err_col = 'error'


# Writing
def to_dict(obj):
     return dict((name, getattr(obj, name)) for name in dir(obj)
                 if (not callable(getattr(obj, name))) and (not name.startswith('__')))


def get_db():
    conn = pymongo.MongoClient(iblog_host)
    return conn[db_name]


def insert_order(openOrder):
    orders = get_db()[order_col]
    data = openOrder.order.__dict__
    if data.get('m_algoParams') is not None:
        data['m_algoParams'] = data['m_algoParams'][0].__dict__
    data['contract'] = openOrder.contract.__dict__
    orders.insert(data)


def insert_execution(execution):
    execs = get_db()[exec_col]
    execs.insert(execution.__dict__)


def insert_commission_report(cr):
    crs = get_db()[cr_col]
    crs.insert(cr.__dict__)


def insert_account_summary(summary):
    sums = get_db()[sum_col]
    sums.insert(to_dict(summary))


def insert_error(error, acc=None):
    errs = get_db()[err_col]
    new_entry = to_dict(error)
    if acc is not None:
        new_entry['account'] = acc
    errs.insert(new_entry)

# Reading

def get_all(col_name):
    return pd.DataFrame(list(get_db()[col_name].find()))

def get_orders():
    return get_all(order_col)

def get_account_summary():
    return get_all(sum_col)

def get_errors():
    return get_all(err_col)

def get_commission_report():
    return get_all(cr_col)

def get_executions():
    return get_all(exec_col)
