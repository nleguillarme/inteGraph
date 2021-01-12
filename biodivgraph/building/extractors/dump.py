from bkgb.modules.core.job import Job
from bkgb.utils.file_manager import decompress_file, delete_file, split_file
import yaml
import argparse
import os
import requests
import shutil
import magic
import logging
import glob

from rdflib import ConjunctiveGraph, URIRef, plugin, Graph
from rdflib.util import guess_format
from rdflib.store import Store

"""

The Triple/Quad Dump Importer is used to locally replicate dataset dumps from
the Web. The importer supports the following RDF serialization formats:
RDF/XML, N-Triples, N-Quads and Turtle.

"""


class DumpImport(Job):
    def __init__(self, cfg):
        self.internal_id = cfg["internalId"]

        Job.__init__(self, "import_" + self.internal_id)
        self.logger = logging.getLogger(__name__)

        self.prefix = cfg["graphURI"]
        self.data_source = cfg["dataSource"]
        self.dump_url = cfg["dumpLocation"]
        self.triples_dir = cfg["quadsDirectory"]
        self.delete_tmp = cfg["deleteTempFiles"] == "True"
        self.graph_name = os.path.join(self.prefix, "graph#" + self.internal_id)

        self.logger.info("New dump import job with id {}".format(self.job_id))
        self.logger.debug(cfg)

    def download_dump(self):
        dump_file = self.dump_url.split("/")[-1]
        dump_file = os.path.join(self.tmp_dir, dump_file)
        self.logger.debug("Download dataset dump at {}".format(self.dump_url))
        with requests.get(self.dump_url, allow_redirects=True, stream=True) as r:
            with open(dump_file, "wb") as f:
                shutil.copyfileobj(r.raw, f)
        self.logger.debug("File saved as {}".format(dump_file, self.tmp_dir))
        return dump_file

    def decompress_archive(self, dump_file):
        m_res = magic.detect_from_filename(dump_file)
        if ("archive" in m_res.name) or ("compressed" in m_res.name):
            self.logger.debug("Decompress archive {}".format(dump_file))
            dump_file = decompress_file(dump_file, delete_archive=self.delete_tmp)
            self.logger.debug("Archive decompressed as {}".format(dump_file))
        else:
            self.logger.debug("File {} is not an archive".format(dump_file))
        return dump_file

    def split_dump_file(self, dump_file):
        prefix = os.path.join(self.triples_dir, self.internal_id + ".")
        max_size = 5e5
        self.logger.debug(
            "Split file {} into smaller files (max. size = {} kB)".format(
                dump_file, max_size
            )
        )
        split_file(dump_file, prefix=prefix, suffix=".nt", max_size=max_size)
        new_files = glob.glob(prefix + "*.nt")
        return new_files

    def create_graph_files(self, triples_file):
        for filename in triples_file:
            with open(filename + ".graph", "wt") as f:
                f.write(self.graph_name)
                f.close()

    def run(self):
        self.logger.info("Run import job with id {}".format(self.job_id))
        self.create_tmp_dir()
        archive = self.download_dump()
        dump_file = self.decompress_archive(archive)
        triples_files = self.split_dump_file(dump_file)
        self.create_graph_files(triples_files)
        if self.delete_tmp:
            self.delete_tmp_files()
        self.logger.info("Terminate import job with id {}".format(self.job_id))


if __name__ == "__main__":

    parser = argparse.ArgumentParser("dump_import")
    parser.add_argument("config", help="Path to YAML configuration file.", type=str)
    args = parser.parse_args()

    # Parse configuration file
    with open(args.config, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        job = DumpImport(cfg)
        job.run()