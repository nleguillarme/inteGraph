from ..context import biodivgraph, ROOT_DIR
from biodivgraph.core import TaxId
from biodivgraph.building.services import TabData2RDFService
from biodivgraph.utils.config_helper import read_config
import os
import pytest


def assert_exists(file):
    assert os.path.exists(file) == True


def make_config():
    rootDir = os.path.join(ROOT_DIR, "config/test")
    serviceDir = os.path.join(ROOT_DIR, "config/test/services")
    cfg = read_config(os.path.join(serviceDir, "tab.yml"))
    cfg["rootDir"] = rootDir
    cfg["serviceDir"] = serviceDir
    return cfg


def get_properties():
    cfg = make_config()
    property_file = os.path.join(cfg["serviceDir"], cfg["properties"])
    return read_config(property_file)


@pytest.fixture(scope="session", autouse=True)
def tab2rdf_service():
    service = TabData2RDFService(make_config())
    service.create_tmp_dir()
    yield service
    service.delete_tmp_dir()


def test_get_reader(tab2rdf_service):
    df_reader = tab2rdf_service.get_reader()
    p = get_properties()

    for chunk in df_reader:
        assert p["subject"]["columnName"] in chunk.columns
        assert p["predicate"]["columnName"] in chunk.columns
        assert p["object"]["columnName"] in chunk.columns


def test_split(tab2rdf_service):
    tab2rdf_service.split()
    for i in range(0, tab2rdf_service.get_nb_chunks()):
        assert_exists(
            os.path.join(
                tab2rdf_service.get_tmp_dir(),
                tab2rdf_service.get_split_filename_template(i),
            )
        )


def test_link(tab2rdf_service):
    for i in range(0, tab2rdf_service.get_nb_chunks()):
        f_in = os.path.join(
            tab2rdf_service.get_tmp_dir(),
            tab2rdf_service.get_split_filename_template(i),
        )
        f_out = os.path.join(
            tab2rdf_service.get_tmp_dir(), tab2rdf_service.get_link_filename_template(i)
        )
        tab2rdf_service.link(f_in, f_out)
        assert_exists(f_out)


def test_map(tab2rdf_service):
    for i in range(0, tab2rdf_service.get_nb_chunks()):
        f_in = os.path.join(
            tab2rdf_service.get_tmp_dir(), tab2rdf_service.get_link_filename_template(i)
        )
        f_out = os.path.join(
            tab2rdf_service.get_tmp_dir(), tab2rdf_service.get_map_filename_template(i)
        )
        tab2rdf_service.map(f_in, f_out)
        assert_exists(f_out)
