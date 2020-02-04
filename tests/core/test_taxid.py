from ..context import biodivgraph
from biodivgraph.core import TaxId


def make_empty_taxid():
    return TaxId()


def make_taxid():
    return TaxId(db_prefix="NCBI:", id="52644")


def test_init():
    taxid = make_taxid()
    assert taxid.get_prefix() == "NCBI:"
    assert taxid.get_id() == "52644"


def test_init_from_string():
    taxid = make_empty_taxid()
    taxid = taxid.init_from_string("NCBI:52644")
    assert taxid.get_prefix() == "NCBI:"
    assert taxid.get_id() == "52644"


def test_get_taxid():
    taxid = make_taxid()
    map = taxid.get_taxid()
    assert map["type"] == "taxid"
    assert map["value"] == "NCBI:52644"


def test_split():
    taxid = make_taxid()
    db_prefix, id = taxid.split()
    assert db_prefix == "NCBI:"
    assert id == "52644"


def test_to_string():
    taxid = make_taxid()
    assert str(taxid) == "NCBI:52644"
