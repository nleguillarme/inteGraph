from airflow import DAG
from urllib.parse import urljoin
from integraph.etl.extract_api import ExtractAPI
from integraph.etl.extract_file import ExtractFile
from integraph.etl.transform_csv import TransformCSV
from integraph.etl.load_db import LoadDB
from integraph.lib.annotator import register_annotator


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
    def get_transform(self, root_dir, config, graph_id, metadata):
        if config["format"] == "csv":
            return TransformCSV(root_dir, config, graph_id, metadata).transform
        else:
            raise UnsupportedFormatException(config["format"])

    @classmethod
    def get_load(self, root_dir, config):
        if config.get("id") in ["graphdb", "rdfox"]:
            return LoadDB(root_dir, config).load
        else:
            raise UnsupportedDatabaseException(config.get("id"))


def create_etl_dag(
    base_iri,
    src_dir,
    src_config,
    load_config,
    dag_args,
    default_args=None,
    run_in_test_mode=False,
):
    src_id = src_config["source"]["id"]

    # Register annotators
    annotators_config = src_config["annotators"]
    for annotator in annotators_config:
        config = annotators_config[annotator]
        config["src_dir"] = src_dir
        register_annotator(annotator, config)

    import pendulum

    with DAG(
        dag_id=src_id,
        schedule="@once",
        start_date=pendulum.today(),
        default_args=default_args,
    ):  # , **dag_args):
        extract = ETLFactory.get_extract(src_dir, src_config["extract"])
        transform = ETLFactory.get_transform(
            src_dir,
            src_config["transform"],
            graph_id=urljoin(base_iri, src_id),
            metadata=src_config["source"]["metadata"],
        )
        load = ETLFactory.get_load(src_dir, load_config)

        if run_in_test_mode:
            transform(extract())
        else:
            load(transform(extract()))
