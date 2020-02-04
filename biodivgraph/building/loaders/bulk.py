from bkgb.modules.core.job import Job
import logging
import os
import docker
from math import ceil
from docker.errors import NotFound, APIError


class BulkLoader(Job):
    def __init__(self, cfg):
        Job.__init__(self, "load_bulk")
        self.logger = logging.getLogger(__name__)

        self.triples_dir = cfg["quadsDirectory"]
        self.store_dir = cfg["remoteDirectory"]
        self.container_name = cfg["containerName"]
        self.user_name = cfg["userName"]
        self.user_password = cfg["userPassword"]
        self.nb_cores = int(cfg["cores"])
        self.script_name = "bulk_load.sh"
        self.cmd_prefix = "isql -U{} -P{} exec=".format(
            self.user_name, self.user_password
        )
        self.container = None

        self.logger.info("New bulk loading job with id {}".format(self.job_id))
        self.logger.debug(cfg)

    def get_container(self):
        client = docker.from_env()
        try:
            self.container = client.containers.get(self.container_name)
        except NotFound as e:
            self.logger.error(e)
        except APIError as e:
            self.logger.error(e)
        return self.container

    def get_isql_cmd(self, exec_cmd):
        return self.cmd_prefix + '"{}"'.format(exec_cmd)

    def get_register_cmd(self):
        return self.get_isql_cmd("ld_dir ('{}', '*', NULL);".format(self.store_dir))

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

    def create_loader_script(self):
        isql_cmd = self.get_register_cmd() + "\n"
        nb_processes = ceil(self.nb_cores / 2.5)
        isql_cmd += self.get_run_multiple_loaders_cmd(nb_processes)
        isql_cmd += self.get_isql_cmd("DELETE FROM DB.DBA.load_list;")
        self.logger.debug(
            "No processes = {}. Run iSQL command\n{}".format(nb_processes, isql_cmd)
        )
        with open(os.path.join(self.triples_dir, self.script_name), "wt") as f:
            f.write(isql_cmd)
            f.close()
        return os.path.join(self.store_dir, self.script_name)

    def run(self):
        self.logger.info("Run load job with id {}".format(self.job_id))
        if self.get_container() != None:
            self.logger.info("Create bulk loading script")
            script_path = self.create_loader_script()
            self.logger.debug("Run bulk loading script {}".format(script_path))
            # self.container.exec_run("chmod u+x {}".format(script_path))
            self.container.exec_run("sh {}".format(script_path))
        else:
            self.logger.error(
                "Docker container {} not found".format(self.container_name)
            )
        self.logger.info("Terminate load job with id {}".format(self.job_id))
