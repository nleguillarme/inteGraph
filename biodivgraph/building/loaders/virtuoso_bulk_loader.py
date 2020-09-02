from ..core import Loader

import logging
import os
from os import listdir
import argparse
from math import ceil
import docker
from docker.errors import NotFound, APIError

# from fsplit.filesplit import FileSplit

from ...utils.config_helper import read_config
from ...utils.file_helper import *


class VirtuosoBulkLoader(Loader):
    def __init__(self, config):
        Loader.__init__(self, "virtuoso_bulk_loader")
        self.logger = logging.getLogger(__name__)
        self.config = config

        self.cmd_prefix = "isql-v -U{} -P{} exec=".format(
            self.config.user_name, self.config.user_password
        )

        self.config.local_data_dir = os.path.join(self.config.output_dir, "graph")

        self.config.shared_dir_for_source = os.path.join(
            self.config.root_dir, self.config.shared_dir, self.config.internal_id
        )
        self.remote_shared_dir_for_source = os.path.join(
            self.config.remote_dir, self.config.internal_id
        )

        self.script_name = "bulk_load_{}.sh".format(self.config.internal_id)

        self.logger.info("New Virtuoso Bulk Loader with id {}".format(self.get_id()))

    def run(self):
        self.clean_shared_dir()
        self.split_data()
        self.create_loader_script()
        self.run_loader_script()

    def get_input_filename(self):
        dir = self.config.local_data_dir
        files = [
            f
            for f in listdir(dir)
            if os.path.isfile(os.path.join(dir, f))
            and f.endswith(".nt")
            and f[0] != "."
        ]
        if len(files) == 1:
            return os.path.join(self.config.local_data_dir, files[0])
        else:
            return None

    # def copy_data_to_shared_dir(self, **kwargs):
    #     self.clean_shared_dir()
    #     filepath = self.get_input_filename()
    #     if filepath:
    #         copy_file_to_dir(filepath, self.config.shared_dir_for_source)
    #         processed_dir = os.path.join(self.config.local_data_dir, "processed")
    #         clean_dir(processed_dir)

    def split_data(self, **kwargs):
        filepath = self.get_input_filename()
        if filepath:
            copy_file_to_dir(filepath, self.config.shared_dir_for_source)
            # nb_chunks = ceil(self.config.num_processes / 2.5)
            # file_size = os.path.getsize(filepath)
            # chunk_size = ceil((1.0 * file_size) / nb_chunks)
            # fs = FileSplit(
            #     file=filepath,
            #     splitsize=chunk_size,
            #     output_dir=self.config.shared_dir_for_source,
            # )
            # fs.split()

    def create_loader_script(self, **kwargs):
        isql_cmd = self.get_register_cmd() + "\n"
        nb_processes = ceil(self.config.num_processes / 2.5)
        isql_cmd += self.get_run_multiple_loaders_cmd(nb_processes)
        isql_cmd += self.get_isql_cmd("DELETE FROM DB.DBA.load_list;")
        self.logger.debug(
            "No processes = {}. Run iSQL command\n{}".format(nb_processes, isql_cmd)
        )
        with open(
            os.path.join(self.config.shared_dir_for_source, self.script_name), "wt"
        ) as f:
            f.write(isql_cmd)

    def run_loader_script(self, **kwargs):
        container = self.get_container()
        if container != None:
            script_path = os.path.join(
                self.remote_shared_dir_for_source, self.script_name
            )
            print("sh {}".format(script_path))
            container.exec_run("sh {}".format(script_path))
        else:
            raise ValueError(
                "Docker container {} not found".format(self.self.config.container_name)
            )

    def get_container(self):
        container = None
        client = docker.from_env()
        try:
            self.logger.info(client.containers.list())
            container = client.containers.get(self.config.container_name)
        except NotFound as e:
            self.logger.error(e)
        except APIError as e:
            self.logger.error(e)
        return container

    def get_isql_cmd(self, exec_cmd):
        return self.cmd_prefix + '"{}"'.format(exec_cmd)

    def get_register_cmd(self):
        return self.get_isql_cmd(
            "ld_dir ('{}', '*.nt', '{}');".format(
                self.remote_shared_dir_for_source, self.config.graph_uri
            )
        )

    def get_run_loader_cmd(self):
        return self.get_isql_cmd("rdf_loader_run();")

    def get_run_multiple_loaders_cmd(self, nb_processes):
        run_loader_cmd = self.get_run_loader_cmd()
        isql_cmd = ""
        for i in range(0, nb_processes):
            isql_cmd += run_loader_cmd + " &\n"
        isql_cmd += "wait\n"
        isql_cmd += self.get_isql_cmd("checkpoint;") + "\n"
        return isql_cmd

    def get_local_data_dir(self):
        return self.config.local_data_dir

    def clean_shared_dir(self, **kwargs):
        clean_dir(self.config.shared_dir_for_source)


def main():
    parser = argparse.ArgumentParser(
        description="virtuoso bulk loader command line interface."
    )

    parser.add_argument("src_config", help="YAML configuration file for the source.")
    parser.add_argument("loader_config", help="YAML configuration file for the loader.")
    args = parser.parse_args()

    src_config = read_config(args.src_config)
    src_config.output_dir = os.path.join(src_config.source_root_dir, "output")
    loader_config = read_config(args.loader_config)

    process = VirtuosoBulkLoader(src_config + loader_config)
    process.run()


if __name__ == "__main__":
    main()
