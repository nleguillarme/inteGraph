from bkgb.modules.core import Job
from bkgb.modules.linking import TripleLinkingEngine
from bkgb.modules.mapping.rml import RMLMappingEngine
from bkgb.utils.csv_helper import *
import yaml
import os
import logging
import shutil
from rdflib import Graph
from rdflib.util import guess_format
import glob

"""

The Tabular Data Importer is used to convert tabular data into N-Triples.

"""


class TabularDataImport(Job):
    def __init__(self, cfg):
        Job.__init__(self)
        self.logger = logging.getLogger(__name__)
        self.create_tmp_dir()

        self.root_dir = cfg["rootDir"]
        self.reports_dir = cfg["reportsDir"]
        self.prefix = cfg["graphURI"]
        self.internal_id = cfg["internalId"]
        self.data_source = cfg["dataSource"]
        self.file_properties = cfg["fileProperties"]
        self.delete_tmp = cfg["deleteTempFiles"] == "True"
        self.graph_name = os.path.join(self.prefix, "graph#" + self.internal_id)
        self.properties = self.read_file_properties()
        self.engine = self.get_linking_engine()

        self.mapper = RMLMappingEngine(self.tmp_dir)
        rule_file = "/home/leguilln/workspace/biodiv-kg-builder/example/biodiv-graph/mappings/inter-to-globi.yml"
        self.mapper.set_rules(rule_file)

        self.logger.info("New tabular data import job with id {}".format(self.job_id))
        self.logger.debug(cfg)
        self.logger.debug(self.properties)

        self.quads_dir = cfg["quadsDirectory"]
        self.output_file = os.path.join(self.quads_dir, self.internal_id + ".nq")

        self.nb = 0

    def read_file_properties(self):
        with open(os.path.join(self.root_dir, self.file_properties), "r") as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        return cfg

    def get_linking_engine(self):
        engine = TripleLinkingEngine()
        engine.init_from_dict(self.properties)
        return engine

    def read(self):
        csv_file = self.properties["fileLocation"]
        read_csv_header(csv_file, delimiter=",")
        columns = [
            self.properties["subject"]["columnName"],
            self.properties["predicate"]["columnName"],
            self.properties["object"]["columnName"],
        ]
        df_reader = get_csv_file_reader(
            csv_file, columns, delimiter=",", chunksize=1000000000  # chunksize=1000
        )
        return df_reader

    def link(self, df):
        return self.engine.linkTriples(df)

    def map(self, df):
        df = df.dropna()
        self.mapper.run_mapping(df, self.internal_id + ".{}.nq".format(self.nb))
        self.nb += 1

    def create_report(self, df_ref, df):
        null_rows = df.isnull().any(axis=1)
        null_df = df_ref[null_rows]
        with open(os.path.join(self.reports_dir, str(self.job_id) + ".rep"), "wt") as f:
            null_df.to_string(f)
            f.close()

    def merge(self):
        prefix = os.path.join(self.tmp_dir, self.internal_id + ".")
        quads_files = glob.glob(prefix + "*.nq")
        g = Graph()
        for file in quads_files:
            g.parse(file, format="nquads")
        g.serialize(destination=self.output_file, format="nt")

    def create_graph_file(self):
        with open(self.output_file + ".graph", "wt") as f:
            f.write(self.graph_name)
            f.close()

    def run(self):
        self.logger.info("Run import job with id {}".format(self.job_id))
        df_reader = self.read()
        for chunk in df_reader:
            # chunk = chunk.loc[chunk["consumer_gbif_key"] == "9797892"]
            # chunk = chunk.head()
            triples = self.link(chunk)
            self.map(triples)
            # print(triples)
            self.create_report(chunk, triples)
        self.merge()
        self.create_graph_file()
        if self.delete_tmp:
            self.delete_tmp_files()
        self.logger.info("Terminate import job with id {}".format(self.job_id))
