import pytest

from bkgb.utils.file_manager import *

def test_decompress_gz():
    f_in = "test_data/dummy.gz"
    assert decompress_gz(f_in) != None

def test_decompress_tar():
    f_in = "test_data/dummy.tar"
    assert decompress_tar(f_in) != None

def test_decompress_file():
    f_in = "test_data/dummy.gz"
    assert decompress_file(f_in) != None

    f_in = "test_data/dummy.tar"
    assert decompress_file(f_in) != None

    f_in = "test_data/dummy.zip"
    with pytest.raises(NotImplementedError):
        decompress_file(f_in) != None
