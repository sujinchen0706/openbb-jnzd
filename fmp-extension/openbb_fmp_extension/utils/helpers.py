"""FMP Helpers Module."""

import json
from urllib.request import urlopen

import certifi


def get_jsonparsed_data(url):
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)
