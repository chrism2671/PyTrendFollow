from core.instrument import Instrument
from core.utility import generate_random_prices


class RandomInstrument(Instrument):

    def __init__(self, name='random', denomination='USD', dp_name=None):
        self.random_prices = generate_random_prices()
        super().__init__()

    def panama_prices(self):
        return self.random_prices
