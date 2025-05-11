#!/usr/bin/env python
"""
Exmo API
https://exmo.com/uk/api#/authenticated_api
"""

import http.client
import urllib.parse
import json
import hashlib
import hmac
import time


class ExmoAPI:
    def __init__(self, API_KEY, API_SECRET, API_URL='api.exmo.com', API_VERSION='v1'):
        self.API_URL = API_URL
        self.API_VERSION = API_VERSION
        self.API_KEY = API_KEY
        self.API_SECRET = bytes(API_SECRET, encoding='utf-8')

    def sha512(self, data: str):
        h: hmac.HMAC = hmac.new(key=self.API_SECRET, digestmod=hashlib.sha512)
        h.update(data.encode('utf-8'))
        return h.hexdigest()

    def api_query(self, api_method: str, params: dict = {}) -> object:
        params['nonce'] = int(round(time.time() * 1000))
        params = urllib.parse.urlencode(params)

        sign = self.sha512(params)
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Key": self.API_KEY,
            "Sign": sign
        }
        conn = http.client.HTTPSConnection(self.API_URL)
        conn.request("POST", "/" + self.API_VERSION + "/" + api_method, params, headers)
        response = conn.getresponse().read()

        conn.close()

        try:
            obj = json.loads(response.decode('utf-8'))
            if 'error' in obj and obj['error']:
                # print(obj['error'])
                raise ExmoError(str(obj['error']), obj)
            return obj
        except json.decoder.JSONDecodeError:
            raise ExmoError('Error while parsing response:', response)


class ExmoError(Exception):
    """
    Base Exmo Exception class
    """
    pass


if __name__ == '__main__':
    # Example
    api_key = "YOUR_API_KEY"  # Replace with a placeholder or load from config for testing
    api_secret = "YOUR_API_SECRET"  # Replace with a placeholder or load from config for testing
    ExmoAPI_instance = ExmoAPI(api_key, api_secret)
    response = ExmoAPI_instance.api_query('user_info')
    balances = response['balances']
    print(f"Balances: {balances}") # Added a print statement to see the output  
    