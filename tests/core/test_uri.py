from ..context import biodivgraph
from biodivgraph.core import URIManager, URIMapper, TaxId


def make_uri_mapper():
    return URIMapper()


def make_uri_manager():
    return URIManager(mapper=make_uri_mapper())


def test_uri_mapper_get_uri_prefix_from_db_prefix():
    mapper = make_uri_mapper()
    ncbi_prefix = "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id="
    assert mapper.get_uri_prefix_from_db_prefix("NCBI:") == ncbi_prefix


def test_uri_mapper_get_db_prefix_from_uri():
    mapper = make_uri_mapper()
    uri = "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=52644"
    assert mapper.get_db_prefix_from_uri(uri) == "NCBI:"


def test_uri_helper_get_uri_from_taxid():
    manager = make_uri_manager()
    taxid = TaxId("NCBI:", 52644)
    uri = "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=52644"
    assert manager.get_uri_from_taxid(taxid) == uri


def test_uri_helper_get_taxid_from_uri():
    manager = make_uri_manager()
    uri = "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=52644"
    taxid = manager.get_taxid_from_uri(uri)
    assert taxid.get_prefix() == "NCBI:"
    assert taxid.get_id() == "52644"
