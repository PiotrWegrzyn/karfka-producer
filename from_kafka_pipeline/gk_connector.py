import requests
from typing import Iterable


class GKConnector:
    def send(self, messages: Iterable(dict)):
        url = 'http://localhost:1234/process'
        headers = {'content-type': 'application/json'}

        requests.post(url, data=messages, headers=headers)