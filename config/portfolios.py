import config.instruments
import random

p_all = [v['name'] for v in config.instruments.instrument_definitions]

p_soft = [v['name'] for v in config.instruments.instruments_all['soft']]
p_hard = [v['name'] for v in config.instruments.instruments_all['hard']]
p_currency = [v['name'] for v in config.instruments.instruments_all['currency']]
p_rate = [v['name'] for v in config.instruments.instruments_all['rate']]
p_index = [v['name'] for v in config.instruments.instruments_all['index']]
# p_bitmex = [v['name'] for v in config.instruments.instruments_all['bitmex']]

p_trade = [i for i in p_all if i not in (['dax', 'cac', 'aex', 'sp500', 'r2000', 'ftse', 'bitcoin'])]

# Randomly split our portfolio into learning and testing sets
x = p_trade
random.shuffle(x)
l = int(len(x)/2)
p_learn, p_test = x[:l], x[l:]
