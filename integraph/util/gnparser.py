import requests
from pandas import isna
from urllib.parse import quote


def normalize_names(names):
    """
    Given a list of taxonomic names, return the corresponding canonical forms
    """
    session = requests.Session()
    query = quote("|".join(names))
    url = "http://gnparser:8778/api/v1/" + query
    with session.get(url=url) as resp:
        resp.raw.decode_content = True
        resp.raise_for_status()
        data = resp.json()
        canonical_names = [None] * len(data)
        for i in range(len(data)):
            if data[i].get("canonical"):
                canonical_names[i] = data[i].get("canonical").get("full")
    assert len(names) == len(canonical_names)
    return {
        names[i]: canonical_names[i] if not isna(canonical_names[i]) else ""
        for i in range(len(names))
    }
