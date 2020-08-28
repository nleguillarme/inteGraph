from .csv2rdf_dag import *

# from .bulk_loading_dag import *
# from .neo4j_rdf_import_dag import *
# from biodivgraph.building.transformers import CSV2RDF

# from biodivgraph.building.services import BulkLoaderService
# from biodivgraph.building.services import Neo4jRDFImportService

from .file_extractor_dag import FileExtractorDAG
from .csv2rdf_dag import CSV2RDFDAG


class UnsupportedSourceException(Exception):
    pass


class UnsupportedFormatException(Exception):
    pass


class WorkflowFactory:
    def __init__(self):
        pass

    def get_extractor_dag(self, config, default_args=None):
        print(config)
        if "file_location" in config:
            return self.get_dag_instance(FileExtractorDAG, config, default_args)
        else:
            raise UnsupportedSourceException()

    def get_transformer_dag(self, config, default_args=None):
        if config.data_format == "csv":
            return self.get_dag_instance(CSV2RDFDAG, config, default_args)
        else:
            raise UnsupportedFormatException(config.data_format)

    # def get_worflow(self, cfg, default_args=None):
    #     if cfg.process == "CSV2RDF":
    #         return self.get_dag_instance(CSV2RDFDAG, CSV2RDF(cfg), cfg, default_args)
    #     # elif cfg["service"] == "BulkLoader":
    #     #     return self.get_dag_instance(
    #     #         BulkLoaderDag, BulkLoaderService(cfg), cfg, default_args
    #     #     )
    #     # elif cfg["service"] == "Neo4jRDFImport":
    #     #     return self.get_dag_instance(
    #     #         Neo4jRDFImportDag, Neo4jRDFImportService(cfg), cfg, default_args
    #     #     )
    #     # elif cfg["service"] == "FetchRDFDump":
    #     #     return DumpImport(cfg)
    #     else:
    #         raise ValueError(cfg["service"])

    def get_dag_instance(self, dag_class, config, default_args):
        schedule_interval = (
            config.scheduleInterval
            if "scheduleInterval" in config
            else default_args["schedule_interval"]
        )
        dag = dag_class(config).create_dag(
            schedule_interval=schedule_interval, default_args=default_args
        )
        return dag

    # def get_dag_instance(self, dag_class, service, cfg, default_args):
    #     schedule_interval = (
    #         cfg["scheduleInterval"]
    #         if "scheduleInterval" in cfg
    #         else default_args["schedule_interval"]
    #     )
    #     dag = dag_class(service).create_dag(
    #         schedule_interval=schedule_interval, default_args=default_args
    #     )
    #     return {dag.dag_id: dag}
