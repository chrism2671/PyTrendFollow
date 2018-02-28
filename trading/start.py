# import logging
# logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# root_logger = logging.getLogger()

# create path if not exists
# path = os.path.join(config.local.data_dir, 'log')
# if not os.path.exists(path):
#     os.makedirs(path)
# hdlr = logging.FileHandler(os.path.join(path, config.local.environment + '.log'))
# hdlr.setFormatter(formatter)
# root_logger.addHandler(hdlr)
# root_logger.setLevel(logging.DEBUG)

import pandas as pd
from pylab import rcParams
rcParams['figure.figsize'] = 15, 10

idx=pd.IndexSlice


# suppress numpy warnings explicitly, as Pandas doesn't do this in other threads
import warnings
warnings.filterwarnings('ignore')
