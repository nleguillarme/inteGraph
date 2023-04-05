from airflow import DAG
from urllib.parse import urljoin
from .extract_api import ExtractAPI
from .extract_file import ExtractFile
from .transform_csv import TransformCSV
from .load_db import LoadDB


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
    def get_transform(self, root_dir, config, graph_id, morph_config_filepath):
        if config["format"] == "csv":
            return TransformCSV(
                root_dir, config, graph_id, morph_config_filepath
            ).transform
        else:
            raise UnsupportedFormatException(config["format"])

    @classmethod
    def get_load(self, root_dir, config):
        if config["db"] == "graphdb":
            return LoadDB(root_dir, config).load
        else:
            raise UnsupportedDatabaseException(config.db)


def create_etl_dag(
    graph_base_iri,
    morph_config_filepath,
    src_id,
    src_dir,
    extract_cfg,
    transform_cfg,
    load_cfg,
    dag_args,
    default_args=None,
    run_in_test_mode=False,
):
    with DAG(dag_id=src_id, default_args=default_args, **dag_args):
        graph_id = urljoin(graph_base_iri, src_id)
        extract = ETLFactory.get_extract(src_dir, extract_cfg)
        transform = ETLFactory.get_transform(
            src_dir,
            transform_cfg,
            graph_id=graph_id,
            morph_config_filepath=morph_config_filepath,
        )
        load = ETLFactory.get_load(src_dir, load_cfg)

        transform(extract())
        if run_in_test_mode:
            load(transform())
