import tarfile
import gzip
import io
import shutil
import pathlib
import os
from tika import parser
from math import ceil
import subprocess


def pdf_2_text(url):
    data = parser.from_file(url)
    return data["content"]


def split_file(f_in, prefix, suffix, max_size=1e6):
    size_in_kb = os.path.getsize(f_in) / 1000.0
    nb = ceil(1.0 * size_in_kb / max_size)
    subprocess.call(
        [
            "split",
            "-n",
            "l/" + str(nb),
            "-a",
            str(len(str(nb))),
            "-d",
            "--additional-suffix={}".format(suffix),
            f_in,
            prefix,
        ]
    )


def decompress_file(f_in, delete_archive=False):
    f_out = None
    if tarfile.is_tarfile(f_in):
        f_out = decompress_tar(f_in)
    elif f_in.endswith(".gz"):
        f_out = decompress_gz(f_in)
    else:
        extension = pathlib.Path(f_in).suffixes
        raise NotImplementedError(
            "Cannot decompress files with extension {}".format(extension)
        )
    if delete_archive:
        delete_file(f_in)
    return f_out


def decompress_tar(f_in):
    try:
        print(f_in)
        if f_in.endswith("tar.gz"):
            print("tar.gz")
            tar = tarfile.open(f_in, "r:gz")
        elif f_in.endswith("tar"):
            print("tar")
            tar = tarfile.open(f_in, "r:")
        f_out = tar.getnames()
        tar.extractall()
        tar.close()
        return f_out
    except tarfile.ReadError as e:
        print(e)
        return None


def decompress_gz(f_in):
    basename = os.path.basename(f_in)
    dirname = os.path.dirname(f_in)
    f_out = os.path.join(dirname, os.path.splitext(basename)[0])
    with open(f_out, "wb") as fo, gzip.open(f_in, "rb") as fi:
        shutil.copyfileobj(fi, fo, length=64 * 2 ** 20)
        fi.close()
        fo.close()
        return f_out


def delete_file(filepath):
    try:
        os.remove(filepath)
    except:
        print("Error while deleting file ", filepath)
