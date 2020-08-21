from ..context import biodivgraph
from biodivgraph.core import TaxId
from biodivgraph.building.linking import TripleLinkingEngine


def make_taxname_config():
    return {
        "rootDir": "./config/test",
        "subject": {
            "columnName": "consumer_gbif_key",
            "transform": [],
            "mapping": "taxName",
        },
        "predicate": {
            "columnName": "interaction_type",
            "mapping": "mappings/inter-to-uri.yml",
        },
        "object": {
            "columnName": "resource_gbif_key",
            "transform": [],
            "mapping": "taxName",
        },
    }


def make_taxid_config():
    return {
        "rootDir": "./config/test",
        "subject": {
            "columnName": "consumer_gbif_key",
            "transform": [{"prefix": "GBIF:"}],
            "mapping": "taxId",
        },
        "predicate": {
            "columnName": "interaction_type",
            "mapping": "mappings/inter-to-uri.yml",
        },
        "object": {
            "columnName": "resource_gbif_key",
            "transform": [{"prefix": "GBIF:"}],
            "mapping": "taxId",
        },
    }


def make_taxname_triple_linker():
    cfg = make_taxname_config()
    linker = TripleLinkingEngine().init_from_dict(cfg)
    return linker


def make_taxid_triple_linker():
    cfg = make_taxid_config()
    linker = TripleLinkingEngine().init_from_dict(cfg)
    return linker


def make_taxname_triple():
    triple = ("Enhydra lutris", "eats", "Crassadoma gigantea")
    return triple


def make_taxid_triple():
    triple = ("2433670", "eats", "2285940")
    return triple


def test_link_taxname_triple():
    triple = make_taxname_triple()
    linker = make_taxname_triple_linker()
    triple = linker.linkTriple(triple)

    assert_correct_links(triple)


def test_link_taxid_triple():
    triple = make_taxid_triple()
    linker = make_taxid_triple_linker()
    triple = linker.linkTriple(triple)

    assert_correct_links(triple)


def assert_correct_links(triple):
    s, p, o = triple
    assert s["value"] == "http://www.gbif.org/species/2433670"
    assert o["value"] == "http://www.gbif.org/species/2285940"
    assert p["value"] == "http://purl.obolibrary.org/obo/RO_0002470"
