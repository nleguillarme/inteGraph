from airflow import DAG
from urllib.parse import urljoin
from integraph.etl.extract_api import ExtractAPI
from integraph.etl.extract_file import ExtractFile
from integraph.etl.transform_csv import TransformCSV
from integraph.etl.load_db import LoadDB


class UnsupportedSourceException(Exception):
    pass


class UnsupportedFormatException(Exception):
    pass


class UnsupportedDatabaseException(Exception):
    pass


class ETLFactory:
    def __init__(self):
        pass

    @classmethod
    def get_extract(self, root_dir, config):
        if "file" in config:
            return ExtractFile(root_dir, config["file"]).extract
        elif "api" in config:
            return ExtractAPI(root_dir, config["api"]).extract
        else:
            raise UnsupportedSourceException()

    @classmethod
    def get_transform(self, root_dir, config, graph_id):
        if config["format"] == "csv":
            return TransformCSV(root_dir, config, graph_id).transform
        else:
            raise UnsupportedFormatException(config["format"])

    @classmethod
    def get_load(self, root_dir, config):
        if config["id"] == "graphdb":
            return LoadDB(root_dir, config).load
        else:
            raise UnsupportedDatabaseException(config.db)


def create_etl_dag(
    graph_base_iri,
    src_id,
    src_dir,
    extract_cfg,
    transform_cfg,
    load_cfg,
    dag_args,
    default_args=None,
    run_in_test_mode=False,
):
    import pendulum

    with DAG(
        dag_id=src_id,
        schedule="@once",
        start_date=pendulum.today(),
        default_args=default_args,
    ):  # , **dag_args):
        graph_id = urljoin(graph_base_iri, src_id)
        extract = ETLFactory.get_extract(src_dir, extract_cfg)
        transform = ETLFactory.get_transform(
            src_dir,
            transform_cfg,
            graph_id=graph_id,
        )
        load = ETLFactory.get_load(src_dir, load_cfg)

        if run_in_test_mode:
            transform(extract())
        else:
            load(transform(extract()))
