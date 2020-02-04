from ..context import biodivgraph
from biodivgraph.core import TaxId
from biodivgraph.building.linking.taxon_linker import (
    TaxNameLinker,
    TaxIdLinker,
    TaxonomyMatching,
)


def make_taxonomy_matching():
    matching = TaxonomyMatching()
    return matching


def make_taxname_linker():
    linker = TaxNameLinker(transforms=None)
    return linker


def make_taxid_linker():
    linker = TaxIdLinker(transforms=None)
    return linker


def make_taxid():
    return TaxId(db_prefix="GBIF:", id="2433670")


def make_unknown_taxid():
    return TaxId(db_prefix="GBIF:", id="R3")


def make_correct_name():
    return "Enhydra lutris"


def make_name_for_scrubbing():
    return "Enhydra    lutris  ssp. nereis"


def make_unknown_name():
    return "Enhidra lutrys"


def test_scrub_taxname():
    matching = make_taxonomy_matching()
    linker = make_taxname_linker()
    name = make_name_for_scrubbing()

    res = matching.scrub_taxname(name)
    assert res == "Enhydra lutris"


def test_get_uri_from_correct_taxname():
    linker = make_taxname_linker()
    name = make_correct_name()
    res = linker.get_uri(name)
    assert res["type"] == "uri"
    assert res["value"] == "http://www.gbif.org/species/2433670"


def test_get_uri_from_incorrect_taxname():
    linker = make_taxname_linker()
    name = make_name_for_scrubbing()
    res = linker.get_uri(name)
    assert res["type"] == "uri"
    assert res["value"] == "http://www.gbif.org/species/2433670"


def test_get_uri_from_unknown_taxname():
    linker = make_taxname_linker()
    name = make_unknown_name()
    res = linker.get_uri(name)
    assert res["type"] == "uri"
    assert res["value"] == None


def test_get_uri_from_taxid():
    linker = make_taxid_linker()
    taxid = make_taxid()
    res = linker.get_uri(taxid)
    assert res["type"] == "uri"
    assert res["value"] == "http://www.gbif.org/species/2433670"


def test_get_uri_from_correct_taxid():
    linker = make_taxid_linker()
    taxid = make_taxid()
    res = linker.get_uri(taxid)
    assert res["type"] == "uri"
    assert res["value"] == "http://www.gbif.org/species/2433670"


def test_get_uri_from_unknown_taxid():
    linker = make_taxid_linker()
    taxid = make_unknown_taxid()
    res = linker.get_uri(taxid)
    assert res["type"] == "uri"
    assert res["value"] == None
