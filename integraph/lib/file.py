import requests
import shutil
import logging
import validators
from requests.adapters import HTTPAdapter
from requests_file import FileAdapter
from pyunpack import Archive
from ..util.path import ensure_path
import re


def _get_session():
    session = requests.Session()
    session.mount("file://", FileAdapter())
    session.mount("http://", HTTPAdapter())
    session.mount("https://", HTTPAdapter())
    return session


def _get_filename_from_cd(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall("filename=(.+)", cd)
    if len(fname) == 0:
        return None
    return fname[0].strip('"')


def get_url(url, root_dir):
    if validators.url(url):
        return url
    else:
        path = ensure_path(url)
        if not path.is_absolute():
            path = root_dir / path
        if path.exists():
            return "file://" + str(path)
        else:
            raise FileNotFoundError(str(path))


def fetch(url, output_dir):
    session = _get_session()
    with session.get(url, allow_redirects=True, stream=True) as resp:
        resp.raw.decode_content = True
        resp.raise_for_status()
        if url.startswith("file://"):
            filename = ensure_path(url.split("file://")[-1]).name
        else:
            filename = _get_filename_from_cd(resp.headers.get("content-disposition"))
        output_dir.mkdir(parents=True, exist_ok=True)
        filepath = output_dir / filename
        with open(filepath, "wb") as f:
            logging.debug(f"Write fetched file to {filepath}")
            shutil.copyfileobj(resp.raw, f)
    return str(filepath)


def unpack(filepath, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    Archive(filepath).extractall(output_dir)
    return list(output_dir.iterdir())


def copy(filepath, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    return shutil.copy(filepath, output_dir)


def is_archive(filepath):
    try:
        with open(filepath, "r") as f:
            f.read()
            return False
    except:
        return True
