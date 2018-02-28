# -*- coding: utf-8 -*-
from urllib.parse import urlparse, urlencode
import time
import hashlib
import hmac
from bravado.requests_client import Authenticator, RequestsClient
from bravado.client import SwaggerClient
import config.settings

HOST = "https://testnet.bitmex.com"
SPEC_URI = HOST + "/api/explorer/swagger.json"
api_config = {'use_models': False, 'validate_responses': False}
_request_client = None


class APIKeyAuthenticator(Authenticator):
    """?api_key authenticator.
    This authenticator adds BitMEX API key support via header.
    :param host: Host to authenticate for.
    :param api_key: API key.
    :param api_secret: API secret.
    """

    def __init__(self, host, api_key, api_secret):
        super(APIKeyAuthenticator, self).__init__(host)
        self.api_key = api_key
        self.api_secret = api_secret

    # Forces this to apply to all requests.
    def matches(self, url):
        if "swagger.json" in url:
            return False
        return True

    # Add the proper headers via the `expires` scheme.
    def apply(self, r):
        expires = int(round(time.time()) + 5) # 5s grace period in case of clock skew
        r.headers['api-expires'] = str(expires)
        r.headers['api-key'] = self.api_key
        r.headers['api-signature'] = self.generate_signature(self.api_secret, r.method, r.url,
                                                r.params if hasattr(r, 'params') else '',
                                                expires, r.data if hasattr(r, 'data') else '')
        return r

    # Generates an API signature.
    # A signature is HMAC_SHA256(secret, verb + path + nonce + data), hex encoded.
    # Verb must be uppercased, url is relative, nonce must be an increasing 64-bit integer
    # and the data, if present, must be serialized as URL parameters string
    #
    # For example, in psuedocode (and in real code below):
    #
    # verb = POST
    # url = /api/v1/order
    # nonce = 1416993995705
    # data = symbol=XBTZ14&quantity=1&price=395.01
    # signature = HEX(HMAC_SHA256(secret, 'POST/api/v1/order1416993995705symbol=XBTZ14&quantity=1&price=395.01'))
    def generate_signature(self, secret, verb, url, params, nonce, data):
        """Generate a request signature compatible with BitMEX."""
        # Parse the url so we can remove the base and extract just the path.
        parsedURL = urlparse(url)
        path = parsedURL.path
        if parsedURL.query:
            path = path + '?' + parsedURL.query

        # If GET params are passed in a separate argument, encode them to URL params
        if params is None or len(params) == 0:
            params = ''
        else:
            params = '?' + urlencode(params)

        # If request data isn't empty, it should be serialized to URL parameters string
        if data is None or len(data) == 0:
            data_str = ''
        else:
            data_str = urlencode(data)
        message = bytes((verb + path + params + str(nonce) + data_str).encode('utf-8'))
        secret_b = bytes(secret.encode('utf-8'))
        signature = hmac.new(secret_b, message, digestmod=hashlib.sha256).hexdigest()
        return signature


def get_request_client():
    global _request_client
    if not _request_client:
        rc = RequestsClient()
        rc.authenticator = APIKeyAuthenticator(HOST, config.settings.bitmex_api_key,
                                               config.settings.bitmex_secret_key)
        _request_client = SwaggerClient.from_url(SPEC_URI, config=api_config,
                                                     http_client=rc)
    return _request_client
