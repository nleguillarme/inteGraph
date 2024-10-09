import requests
import shutil
import logging
import validators
from requests.adapters import HTTPAdapter
from requests_file import FileAdapter
from pyunpack import Archive
from ..util.path import ensure_path
import re
import mimetypes


def _get_session():
    session = requests.Session()
    session.mount("file://", FileAdapter())
    session.mount("http://", HTTPAdapter())
    session.mount("https://", HTTPAdapter())
    return session


# def _get_filename_from_cd(cd):
#     """
#     Get filename from content-disposition
#     """
#     if not cd:
#         return None
#     fname = re.findall("filename=(.+)", cd)
#     if len(fname) == 0:
#         return None
#     return fname[0].strip('"')


def _get_filename_from_resp(url, resp):
    cd = resp.headers.get("content-disposition")
    if cd:
        fname = re.findall("filename=(.+)", cd)
        if len(fname) != 0:
            return fname[0].strip('"')
    else:
        path = ensure_path(url)
        extension = "".join(path.suffixes)
        if extension:
            return path.name
        else:
            content_type = resp.headers.get("Content-Type")
            if content_type:
                extension = mimetypes.guess_extension(content_type)
                if extension:
                    return path.name + extension
    raise ValueError(f"Cannot determine filename for url {url} : {resp}")


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


def fetch(url, output_dir, filename=None):
    session = _get_session()
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux i686; rv:110.0) Gecko/20100101 Firefox/110.0."
    }
    with session.get(url, allow_redirects=True, stream=True, headers=headers) as resp:
        resp.raw.decode_content = True
        resp.raise_for_status()
        if not filename:
            if url.startswith("file://"):
                filename = ensure_path(url.split("file://")[-1]).name
            else:
                filename = _get_filename_from_resp(url, resp)
        output_dir.mkdir(parents=True, exist_ok=True)
        filepath = output_dir / filename
        with open(filepath, "wb") as f:
            logging.debug(f"Write fetched file to {filepath}")
            shutil.copyfileobj(resp.raw, f)
    return str(filepath)


def unpack(filepath, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    Archive(filepath).extractall(output_dir)
    # unpacked_files = []
    # if files:
    #     for file in files:
    #         path = output_dir / file
    #         if path.exists():
    #             unpacked_files.append(path)
    #         else:
    #             raise FileExistsError(path)
    return [str(path) for path in output_dir.iterdir()]


def copy(filepath, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    return shutil.copy(filepath, output_dir)


def is_archive(filepath):
    import patoolib

    try:
        print(filepath)
        patoolib.test_archive(filepath, verbosity=-1)
    except patoolib.util.PatoolError as error:
        print(str(error))
        if str(error).startswith("unknown archive"):
            return False
        else:
            raise error
    else:
        return True
