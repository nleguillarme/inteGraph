import logging
import os
import glob
from SPARQLWrapper import SPARQLWrapper, JSON
from .loader import Loader


class SPARQLLoader(Loader):
    def __init__(self, cfg):
        loader.__init__(self, "sparql_endpoint_loader")
        self.logger = logging.getLogger(__name__)
        self.config = cfg
        self.cfg.graph_dir = os.path.join(self.cfg.output_dir, "graph")
        self.endpoint = SPARQLWrapper(
            self.cfg.endpoint_url
        )  # http://localhost:3030/interactions/data"
        self.endpoint.setReturnFormat(JSON)
        self.logger.info("New SPARQL Loader with id {}".format(self.get_id()))

    def run(self):
        self.load_data()

    def load_data(self, **kwargs):
        for file in glob.glob(os.path.join(self.get_graph_dir(), "*.nq")):
            self.logger.info(
                f"Load file {file} to SPARQL endpoint {self.cfg.endpoint_url}"
            )
            query = f"LOAD <file://{file}>"
            # query = "LOAD <file://{}> INTO <{}>".format(file_path, graph)
            self.endpoint.setQuery(query)
            results = self.endpoint.query().convert()
            self.logger.debug(results)

    def get_graph_dir(self):
        return self.cfg.graph_dir
