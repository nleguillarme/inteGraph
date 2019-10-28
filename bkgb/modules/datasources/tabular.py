from bkgb.modules.core import Job
from bkgb.modules.linking import TripleLinkingEngine
from bkgb.utils.csv_helper import *
import yaml
import os
import logging

"""

The Tabular Data Importer is used to convert tabular data into N-Triples.

"""


class TabularDataImport(Job):
    def __init__(self, cfg):
        Job.__init__(self)
        self.logger = logging.getLogger(__name__)
        self.create_tmp_dir()

        self.root_dir = cfg["rootDir"]
        self.prefix = cfg["graphURI"]
        self.internal_id = cfg["internalId"]
        self.data_source = cfg["dataSource"]
        self.file_properties = cfg["fileProperties"]
        self.delete_tmp = cfg["deleteTempFiles"] == "True"
        self.graph_name = os.path.join(self.prefix, "graph#" + self.internal_id)
        self.properties = self.read_file_properties()
        self.engine = self.get_linking_engine()

        self.logger.info("New tabular data import job with id {}".format(self.job_id))
        self.logger.debug(cfg)
        self.logger.debug(self.properties)

    def read_file_properties(self):
        with open(os.path.join(self.root_dir, self.file_properties), "r") as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        return cfg

    def get_linking_engine(self):
        engine = TripleLinkingEngine()
        engine.init_from_dict(self.properties)
        return engine

    def get_triples(self):
        csv_file = os.path.join(self.properties["fileLocation"])
        read_csv_header(csv_file, delimiter=",")
        columns = [
            self.properties["tripleSubject"]["columnName"],
            self.properties["triplePredicate"]["columnName"],
            self.properties["tripleObject"]["columnName"],
        ]
        df_reader = get_csv_file_reader(
            csv_file, columns, delimiter=",", chunksize=1000
        )
        return df_reader

    def preprocess_triples(self, df):
        if "prefix" in self.properties["tripleSubject"]:
            self.add_prefix(
                df,
                self.properties["tripleSubject"]["columnName"],
                self.properties["tripleSubject"]["prefix"],
            )
        if "prefix" in self.properties["triplePredicate"]:
            self.add_prefix(
                df,
                self.properties["triplePredicate"]["columnName"],
                self.properties["triplePredicate"]["prefix"],
            )
        if "prefix" in self.properties["tripleObject"]:
            self.add_prefix(
                df,
                self.properties["tripleObject"]["columnName"],
                self.properties["tripleObject"]["prefix"],
            )

    def add_prefix(self, df, columnName, prefix):
        df[columnName] = prefix + df[columnName].astype(str)

    def link_triples(self, df):
        for row in df.itertuples(False, None):
            print(row)
            triple = self.engine.linkTriple(row)
            print(triple)
            break

    def write_triples(self):
        pass

    def run(self):
        self.logger.info("Run import job with id {}".format(self.job_id))
        df_reader = self.get_triples()
        for chunk in df_reader:
            self.preprocess_triples(chunk)
            self.link_triples(chunk)
        if self.delete_tmp:
            self.delete_tmp_files()
        self.logger.info("Terminate import job with id {}".format(self.job_id))
