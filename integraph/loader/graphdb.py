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


class GraphDBLoadingException(Exception):
    pass


class GraphDBLoader(Loader):
    def __init__(self, config):
        Loader.__init__(self, "graphdb_loader")
        self.logger = logging.getLogger(__name__)
        self.cfg = config
        self.cfg.rdf_format = "nq"
        self.base_url = f"http://{self.cfg.db_host}:{self.cfg.db_port}"
        self.auth = (self.cfg.db_user, self.cfg.db_password)
        self.headers = {"Content-Type": "application/n-quads"}
        self.cfg.graph_dir = os.path.join(self.cfg.output_dir, "graph")
        self.logger.info("New GraphDB Loader with id {}".format(self.get_id()))

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
        r = requests.get(
            self.base_url + "/rest/repositories",
            headers={"Accept": "application/json"},
            auth=self.auth,
        )
        r.raise_for_status()
        repo_list = r.json()
        print(repo_list)
        if repo_list:
            for repo in repo_list:
                if repo["id"] == self.cfg.repository_id:
                    return True
        self.logger.error(f"Repository {self.cfg.repository_id} not found")
        return False

    def load_data(self, **kwargs):
        for file in glob.glob(os.path.join(self.get_graph_dir(), "*.nq")):
            self.logger.info(f"Load file {file} to repository {self.cfg.repository_id}")
            r = requests.post(
                self.base_url + f"/repositories/{self.cfg.repository_id}/statements",
                headers=self.headers,
                data=open(file, "rb"),
                auth=self.auth,
            )
            r.raise_for_status()
            # GraphDB does not return any confirmation message

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
