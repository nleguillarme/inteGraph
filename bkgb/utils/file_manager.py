import tarfile
import gzip
import shutil
import pathlib
import os

def decompress_file(f_in, delete_archive=False):
    f_out = None
    if tarfile.is_tarfile(f_in):
        f_out = decompress_tar(f_in)
    elif f_in.endswith('.gz'):
        f_out = decompress_gz(f_in)
    else:
        extension = pathlib.Path(f_in).suffixes
        raise NotImplementedError("Cannot decompress files with extension {}".format(extension))
    if delete_archive:
        delete_file(f_in)
    return f_out

def decompress_tar(f_in):
    try:
        print(f_in)
        if (f_in.endswith("tar.gz")):
            print("tar.gz")
            tar = tarfile.open(f_in, "r:gz")
        elif (f_in.endswith("tar")):
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
    with gzip.open(f_in, 'rb') as fi:
        with gzip.open(f_out, 'wb') as fo:
            shutil.copyfileobj(fi, fo)
            return f_out
    return None

def delete_file(filepath):
    try:
        os.remove(filepath)
    except:
        print("Error while deleting file ", filepath)
