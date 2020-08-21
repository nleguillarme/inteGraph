from biodivgraph.building.core import Service
from biodivgraph.utils.file_helper import copy_file_to_dir
import glob
import logging
import os
from math import ceil
from neo4j import GraphDatabase
from biodivgraph.utils.file_helper import move_file_to_dir


class ConstraintNotFoundException(Exception):
    pass


class GraphConfigurationException(Exception):
    pass


class SessionException(Exception):
    pass


class Neo4jRDFImportService(Service):
    def __init__(self, cfg):
        Service.__init__(self, cfg["internalId"])
        self.logger = logging.getLogger(__name__)

        self.rootDir = cfg["rootDir"]
        self.nt_dir = os.path.join(self.rootDir, cfg["dumpDirectory"])
        self.store_dir = cfg["remoteDirectory"]

        userName = cfg["userName"] if "userName" in cfg else "neo4j"
        userPassword = cfg["userPassword"] if "userPassword" in cfg else "password"

        self.driver = GraphDatabase.driver(
            "bolt://neo4j:7687", auth=(userName, userPassword), encrypted=False
        )

        self.logger.info("New Neo4jRDFImport service with id {}".format(self.get_id()))

    def check_if_files_available(self, **kwargs):
        files = glob.glob(os.path.join(self.nt_dir, "*.nt"))
        return len(files)

    def create_constraint(self, **kwargs):
        with self.driver.session() as session:
            constraints = session.read_transaction(self._check_constraint)
            if "n10s_unique_uri" not in constraints:
                session.write_transaction(self._create_constraint)
                constraints = session.read_transaction(self._check_constraint)
                if "n10s_unique_uri" not in constraints:
                    raise ConstraintNotFoundException

    def create_graph_configuration(self, **kwargs):
        with self.driver.session() as session:
            graph_config = session.read_transaction(self._check_graph_configuration)
            if not graph_config:
                session.write_transaction(self._create_graph_configuration)
                graph_config = session.read_transaction(self._check_graph_configuration)
                if not graph_config:
                    raise GraphConfigurationException

    def fetch_rdf_data_from_uri(self, **kwargs):
        with self.driver.session() as session:
            files = glob.glob(os.path.join(self.nt_dir, "*.nt"))
            files = [os.path.basename(file) for file in files]
            for file in files:
                uri = "file://" + os.path.join(self.store_dir, file)
                result = session.write_transaction(
                    self._fetch_rdf_data, uri, "N-Triples"
                )
                self.logger.info(
                    "uri : {}\nresult : {}".format(uri, [record for record in result])
                )
                move_file_to_dir(
                    os.path.join(self.nt_dir, file),
                    os.path.join(self.nt_dir, "complete"),
                )

    # def print_all_nodes(self, **kwargs):
    #     self.check_session()
    #     self.session.read_transaction(self._print_all_nodes)

    @staticmethod
    def _check_constraint(tx):
        return [record["name"] for record in tx.run("CALL db.constraints")]

    @staticmethod
    def _check_graph_configuration(tx):
        return [record for record in tx.run("CALL n10s.graphconfig.show")]

    @staticmethod
    def _create_constraint(tx):
        tx.run(
            "CREATE CONSTRAINT n10s_unique_uri ON (r:Resource) ASSERT r.uri IS UNIQUE"
        )

    @staticmethod
    def _create_graph_configuration(tx):
        tx.run("call n10s.graphconfig.init()")

    @staticmethod
    def _fetch_rdf_data(tx, uri, format):
        result = tx.run('call n10s.rdf.import.fetch("{}","{}")'.format(uri, format))
        return result

    # @staticmethod
    # def _print_all_nodes(tx):
    #     for record in tx.run("MATCH (n) RETURN n"):
    #         print(record)


# client = HelloWorldExample()
# client.create_constraint()
# client.create_graph_configuration()
# client.fetch_rdf_data_from_uri(
#     uri="file:///dumps/test_globi_16042020_merged.nt", format="N-Triples"
# )
# client.print_all_nodes()
