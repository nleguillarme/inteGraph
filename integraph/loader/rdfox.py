import logging
import os
import argparse
import glob
import requests
import docker
from docker.errors import NotFound, APIError
from math import ceil
from os import listdir

from .loader import Loader
from ..util.file_helper import *
from ..util.config_helper import read_config
from ..util.rdf_helper import split_rdf_files


class RDFSplittingException(Exception):
    pass


class RDFoxLoadingException(Exception):
    pass


class RDFoxLoader(Loader):
    def __init__(self, config):
        Loader.__init__(self, "rdfox_loader")
        self.logger = logging.getLogger(__name__)
        self.cfg = config
        self.cfg.rdf_format = "nq"

        self.rdfox_url = (
            f"http://{self.cfg.rdfox_role}:{self.cfg.rdfox_password}"
            f"@{self.cfg.rdfox_host}:{self.cfg.rdfox_port}"
        )
        self.headers = {
            "Content-Type": "application/n-triples"
        }  # application/n-quads"}

        self.cfg.graph_dir = os.path.join(self.cfg.output_dir, "graph")
        self.cfg.shared_dir_for_source = os.path.join(
            self.cfg.root_dir, self.cfg.shared_dir, self.cfg.internal_id
        )

        self.logger.info("New RDFox Loader with id {}".format(self.get_id()))

    def run(self):
        if self.test_connection_to_db():
            self.clean_split_dir()
            self.split_data()
            self.load_data()

    def split_data(self, **kwargs):
        path_to_rdf = self.get_path_to_graph()
        if path_to_rdf:
            self.logger.info(f"Split RDF file {path_to_rdf}")
            if not split_rdf_files(
                self.get_graph_dir(), os.path.basename(path_to_rdf), output_dir="split"
            ):
                raise RDFSplittingException()
            # for file in glob.glob(
            #     os.path.join(self.get_split_dir(), f"*.{self.cfg.rdf_format}")
            # ):
            #     copy_file_to_dir(file, self.cfg.shared_dir_for_source)

    def test_connection_to_db(self):
        r = requests.get(self.rdfox_url + "/datastores")
        r.raise_for_status()
        content = r.text
        if content:
            lines = content.splitlines()
            if len(lines) > 1:
                datastores = [x.strip('"') for x in lines[1:]]
                return self.cfg.rdfox_datastore in datastores
        self.logger.error(
            f"Unable to connect to {self.cfg.rdfox_host}:{self.cfg.rdfox_port}"
        )
        return False

    def load_data(self, **kwargs):
        for file in glob.glob(
            os.path.join(self.get_graph_dir(), "*.nq")
        ):  # self.get_split_dir(), "*.nq")):
            self.logger.info(
                f"Load file {file} to datastore {self.cfg.rdfox_datastore}"
            )
            data = open(file)
            r = requests.post(
                self.rdfox_url + f"/datastores/{self.cfg.rdfox_datastore}/content",
                headers=self.headers,
                data=data,
            )
            r.raise_for_status()
            content = r.text
            if content:
                lines = content.splitlines()
                if len(lines) > 0:
                    nb_facts = lines[-1].split(" ")[-1]
                    if int(nb_facts) > 0:
                        self.logger.info(
                            f"Added {nb_facts} new facts to datastore {self.cfg.rdfox_datastore}"
                        )
                        continue
            else:
                raise RDFoxLoadingException(content)

    def get_path_to_graph(self):
        dir = self.get_graph_dir()
        files = [
            f
            for f in listdir(dir)
            if os.path.isfile(os.path.join(dir, f))
            and f.endswith(f".{self.cfg.rdf_format}")
            and not f.startswith(".")
        ]
        if len(files) != 1:
            raise FileNotFoundError(f"No or multiple files found in {dir} : {files}")
        return os.path.join(dir, files[0])

    def get_graph_dir(self):
        return self.cfg.graph_dir

    def get_split_dir(self):
        return os.path.join(self.get_graph_dir(), "split")

    def clean_split_dir(self, **kwargs):
        clean_dir(self.get_split_dir())
        # clean_dir(self.cfg.shared_dir_for_source)
