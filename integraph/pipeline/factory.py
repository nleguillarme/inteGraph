from .file_extractor_dag import FileExtractorDAG
from .csv2rdf_dag import CSV2RDFDAG
from .virtuoso_bulk_loader_dag import VirtuosoBulkLoaderDAG
from .rdfox_loader_dag import RDFoxLoaderDAG
from .graphdb_loader_dag import GraphDBLoaderDAG


class UnsupportedSourceException(Exception):
    pass


class UnsupportedFormatException(Exception):
    pass


class UnsupportedDatabaseException(Exception):
    pass


class PipelineFactory:
    def __init__(self):
        pass

    def get_extractor_dag(self, config, default_args=None, parent_dag_name=None):
        if "file_location" in config:
            return self.get_dag_instance(
                FileExtractorDAG, config, default_args, parent_dag_name
            )
        else:
            raise UnsupportedSourceException()

    def get_transformer_dag(self, config, default_args=None, parent_dag_name=None):
        if config.data_format == "csv":
            return self.get_dag_instance(
                CSV2RDFDAG, config, default_args, parent_dag_name
            )
        else:
            raise UnsupportedFormatException(config.data_format)

    def get_loader_dag(self, config, default_args=None, parent_dag_name=None):
        if config.db == "virtuoso":
            return self.get_dag_instance(
                VirtuosoBulkLoaderDAG, config, default_args, parent_dag_name
            )
        elif config.db == "rdfox":
            return self.get_dag_instance(
                RDFoxLoaderDAG, config, default_args, parent_dag_name
            )
        elif config.db == "graphdb":
            return self.get_dag_instance(
                GraphDBLoaderDAG, config, default_args, parent_dag_name
            )
        else:
            raise UnsupportedDatabaseException(config.db)

    def get_dag_instance(self, dag_class, config, default_args, parent_dag_name):
        schedule_interval = (
            config.scheduleInterval
            if "scheduleInterval" in config
            else default_args["schedule_interval"]
        )
        instance = dag_class(config, parent_dag_name)
        dag = instance.create_dag(
            schedule_interval=schedule_interval, default_args=default_args
        )
        return instance.get_dag_id(), dag
