from .csv2rdf_dag import *

# from .bulk_loading_dag import *
# from .neo4j_rdf_import_dag import *
from biodivgraph.building.process import CSV2RDF

# from biodivgraph.building.services import BulkLoaderService
# from biodivgraph.building.services import Neo4jRDFImportService


class WorkflowFactory:
    def __init__(self):
        pass

    def get_worflow(self, cfg, default_args=None):
        if cfg.process == "CSV2RDF":
            return self.get_dag_instance(CSV2RDFDAG, CSV2RDF(cfg), cfg, default_args)
        # elif cfg["service"] == "BulkLoader":
        #     return self.get_dag_instance(
        #         BulkLoaderDag, BulkLoaderService(cfg), cfg, default_args
        #     )
        # elif cfg["service"] == "Neo4jRDFImport":
        #     return self.get_dag_instance(
        #         Neo4jRDFImportDag, Neo4jRDFImportService(cfg), cfg, default_args
        #     )
        # elif cfg["service"] == "FetchRDFDump":
        #     return DumpImport(cfg)
        else:
            raise ValueError(cfg["service"])

    def get_dag_instance(self, dag_class, service, cfg, default_args):
        schedule_interval = (
            cfg["scheduleInterval"]
            if "scheduleInterval" in cfg
            else default_args["schedule_interval"]
        )
        dag = dag_class(service).create_dag(
            schedule_interval=schedule_interval, default_args=default_args
        )
        return {dag.dag_id: dag}
