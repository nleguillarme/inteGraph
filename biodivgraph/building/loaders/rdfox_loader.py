from ..core import Loader

import logging
import os
from os import listdir
import argparse
from math import ceil
import glob

import docker
from docker.errors import NotFound, APIError

import requests

from ...utils.config_helper import read_config
from ...utils.file_helper import *
from ...utils.rdf_helper import split_rdf_files


class RDFSplittingException(Exception):
    pass


class RDFoxLoader(Loader):
    def __init__(self, config):
        Loader.__init__(self, "rdfox_loader")
        self.logger = logging.getLogger(__name__)
        self.config = config

        self.rdfox_url = (
            f"http://{self.config.rdfox_role}:{self.config.rdfox_password}"
            f"@{self.config.rdfox_host}:{self.config.rdfox_port}/datastores/{self.config.rdfox_datastore}/content"
        )
        self.headers = {}

        self.config.local_data_dir = os.path.join(self.config.output_dir, "graph")
        self.config.shared_dir_for_source = os.path.join(
            self.config.root_dir, self.config.shared_dir, self.config.internal_id
        )

        self.logger.info("New RDFox Loader with id {}".format(self.get_id()))

    def run(self):
        self.clean_shared_dir()
        self.split_data()
        if self.test_connection_to_db():
            for file in glob.glob(
                os.path.join(self.config.shared_dir_for_source, "*.nq")
            ):
                self.load_data(f_in=file)

    def split_data(self, **kwargs):
        path_to_rdf = self.get_input_filename(self.config.local_data_dir)
        self.logger.debug(f"{self.config.local_data_dir}, {path_to_rdf}")
        if path_to_rdf:
            self.logger.debug(f"Split RDF file {path_to_rdf}")
            filename = os.path.basename(path_to_rdf)
            if not split_rdf_files(self.config.local_data_dir, filename):
                raise RDFSplittingException()
            for file in glob.glob(
                os.path.join(self.config.local_data_dir, "split", "*.nq")
            ):
                copy_file_to_dir(file, self.config.shared_dir_for_source)

    def test_connection_to_db(self):
        r = requests.get(
            f"http://{self.config.rdfox_role}:{self.config.rdfox_password}@localhost:12110/datastores"
        )
        r.raise_for_status()
        content = r.text
        if content:
            lines = content.splitlines()
            if len(lines) > 1:
                datastores = [x.strip('"') for x in lines[1:]]
                return self.config.rdfox_datastore in datastores

    def load_data(self, f_in, **kwargs):
        self.logger.debug(
            f"Load file {f_in} to datastore {self.config.rdfox_datastore}"
        )
        data = open(f_in)
        r = requests.post(self.rdfox_url, headers=self.headers, data=data)
        r.raise_for_status()
        content = r.text
        if content:
            lines = content.splitlines()
            if len(lines) > 0:
                nb_facts = lines[-1].split(" ")[-1]
                if int(nb_facts) > 0:
                    self.logger.info(
                        f"Added {nb_facts} new facts to datastore {self.config.rdfox_datastore}"
                    )
                    return True
        return False

    def get_input_filename(self, dir):
        files = [
            f
            for f in listdir(dir)
            if os.path.isfile(os.path.join(dir, f))
            and f.endswith(".nt")
            and f[0] != "."
        ]
        if len(files) == 1:
            return os.path.join(dir, files[0])
        else:
            return None

    def get_local_data_dir(self):
        return self.config.local_data_dir

    def clean_shared_dir(self, **kwargs):
        clean_dir(self.config.shared_dir_for_source)


def main():
    parser = argparse.ArgumentParser(description="RDFox loader command line interface.")

    parser.add_argument("src_config", help="YAML configuration file for the source.")
    parser.add_argument("loader_config", help="YAML configuration file for the loader.")
    args = parser.parse_args()

    src_config = read_config(args.src_config)
    src_config.output_dir = os.path.join(src_config.source_root_dir, "output")
    src_config.root_dir = "/home/leguilln/workspace/BiodivGraph/dags_test_data"
    src_config.jars_location = "jars"
    loader_config = read_config(args.loader_config)

    process = RDFoxLoader(src_config + loader_config)
    process.run()


if __name__ == "__main__":
    main()
