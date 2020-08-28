from ..core import Extractor
import requests
import urllib.request, urllib.error, urllib.parse
import json


class NoResultException(Exception):
    pass


class HTTPExtractor(Extractor):
    def __init__(self):
        Extractor.__init__(self)

    def get_json_results(self, url, params={}):
        out = requests.get(url, params=params)
        out.raise_for_status()
        content_type = out.headers["content-type"].split(";")[0]
        if content_type != "application/json":
            raise NoResultException(
                "content-type did not equal application/json : {}".format(
                    out.headers["content-type"]
                )
            )
        return out.json()
