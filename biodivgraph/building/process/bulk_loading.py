from biodivgraph.building.core import Service
from biodivgraph.utils.file_helper import copy_file_to_dir
from docker import from_env
from docker.errors import NotFound, APIError
import glob
import logging
import os
from math import ceil


class BulkLoaderService(Service):
    def __init__(self, cfg):
        Service.__init__(self, cfg["internalId"])
        self.logger = logging.getLogger(__name__)

        self.container_name = cfg["containerName"]
        self.container = self.get_container()

        # self.nt_dir = cfg["dumpDirectory"]
        self.rootDir = cfg["rootDir"]
        self.nt_dir = os.path.join(self.rootDir, cfg["dumpDirectory"])
        self.store_dir = cfg["remoteDirectory"]

        self.cmd_prefix = "isql -U{} -P{} exec=".format(
            cfg["userName"], cfg["userPassword"]
        )
        self.nb_processes = ceil(int(cfg["cores"]) / 2.5)

        self.script_name = "bulk_load_{}.sh".format(self.get_id())

        self.logger.info("New BulkLoader service with id {}".format(self.get_id()))

    def get_ntriples_files_filename(self):
        return os.path.join(self.tmp_dir, "nt_files_list.txt")

    def get_loader_script_filename(self):
        return os.path.join(self.tmp_dir, self.script_name)

    def get_container(self):
        container = None
        try:
            client = from_env()
            container = client.containers.get(self.container_name)
        except NotFound as e:
            self.logger.error(e)
        except APIError as e:
            self.logger.error(e)
        return container

    def check_if_files_available(self, **kwargs):
        files = glob.glob(os.path.join(self.nt_dir, "*.graph"))
        return len(files)

    def get_ntriples_files(self, nt_files, **kwargs):
        files = glob.glob(os.path.join(self.nt_dir, "*.nt"))
        files = [os.path.basename(file) for file in files]
        with open(nt_files, "wt") as f:
            f.write("\n".join(files))

    def create_loader_script(self, nt_files, script_file, **kwargs):
        isql_cmd = ""
        with open(nt_files, "rt") as f:
            for line in f:
                line = line.strip("\n")
                isql_cmd += self.get_register_cmd(filepath=line) + "\n"

            isql_cmd += self.get_run_multiple_loaders_cmd(self.nb_processes)
            isql_cmd += self.get_isql_cmd("DELETE FROM DB.DBA.load_list;")
            self.logger.debug(
                "No processes = {}. Run iSQL command\n{}".format(
                    self.nb_processes, isql_cmd
                )
            )
            with open(script_file, "wt") as f:
                f.write(isql_cmd)

    def publish(self, script_file, **kwargs):
        if os.path.exists(script_file):
            copy_file_to_dir(script_file, self.nt_dir)
        else:
            raise FileNotFoundError(script_file)
        self.container.exec_run(
            "sh {}".format(os.path.join(self.nt_dir, self.script_name))
        )

    def get_isql_cmd(self, exec_cmd):
        return self.cmd_prefix + '"{}"'.format(exec_cmd)

    def get_register_cmd(self, filepath=None):
        if filepath:
            return self.get_isql_cmd(
                "ld_dir ('{}', '{}', NULL);".format(self.store_dir, filepath)
            )
        else:
            return self.get_isql_cmd(
                "ld_dir ('{}', '*.nt', NULL);".format(self.store_dir)
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
