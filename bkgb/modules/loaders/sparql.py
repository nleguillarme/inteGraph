from bkgb.modules.core.job import Job
from SPARQLWrapper import SPARQLWrapper, JSON
import logging
import os


class SPARQLLoader(Job):
    def __init__(self, cfg):
        Job.__init__(self)
        self.logger = logging.getLogger(__name__)

        self.quads_dir = cfg["quadsDirectory"]
        self.endpoint = SPARQLWrapper(
            cfg["endpointURL"]
        )  # http://localhost:3030/interactions/data"
        self.endpoint.setReturnFormat(JSON)
        self.storeDir = cfg["remoteDirectory"]

        self.logger.info("New sparql loading job with id {}".format(self.job_id))
        self.logger.debug(cfg)

    def get_files_and_graph(self):
        for file in os.listdir(self.quads_dir):
            if file.endswith(".graph"):
                rdf_file = os.path.splitext(file)[0]
                graph_name = open(os.path.join(self.quads_dir, file), "rt").read()
                yield rdf_file, graph_name

    def run(self):

        for file, graph in self.get_files_and_graph():
            self.logger.debug("Load {} to graph {}".format(file, graph))
            file_path = os.path.join(self.storeDir, file)
            query = "LOAD <file://{}> INTO <{}>".format(file_path, graph)
            self.logger.debug("SPARQL query : " + query)
            self.endpoint.setQuery(query)
            results = self.endpoint.query().convert()
            self.logger.debug(results)


if __name__ == "__main__":

    job = SPARQLStore(cfg={})
    job.run()
