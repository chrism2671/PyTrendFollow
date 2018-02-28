from bravado.requests_client import RequestsClient
from bravado.client import SwaggerClient
import config.settings
from core.bitmex_auth import get_request_client
from core.utility import contract_format

HOST = "https://testnet.bitmex.com"
SPEC_URI = HOST + "/api/explorer/swagger.json"


class BitmexTrade(object):

    def __init__(self):
        self.request_client = get_request_client()

    def net(self):
        response = self.request_client.User.User_getWallet().result()
        balance = response['amount']
        # if number of decimals is always the same, the float below should be constant
        return balance / float(1e8)

    def place_order(self, symbol, expiry, quantity):
        symbol_format = contract_format(symbol, expiry, format='bitmex')
        # if price isn't specified, the order type will default to 'Market'
        response = self.request_client.Order.Order_new(symbol=symbol_format, orderQty=quantity).result()
        return response