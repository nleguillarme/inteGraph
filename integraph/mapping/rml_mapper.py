import logging
import os
import sys
import shutil
import docker
from docker.errors import NotFound, APIError
from ..util.file_helper import copy_file_to_dir
from ..util.rml_helper import *


class RMLParsingException(Exception):
    pass


class RMLMappingException(Exception):
    pass


class MissingEnvironmentVariable(Exception):
    pass


class RMLMappingEngine:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.cfg = config

        # self.client = docker.from_env()
        # self.mapper_image = self.client.images.get("rmlmapper:latest")
        # self.parser_image = self.client.images.get("yarrrml-parser:latest")

        # self.path_to_yaml_rules = self.cfg.mapping_file
        self.path_to_rml_rules = None

        self.local_config_dir = os.getenv("INTEGRAPH__CONFIG__HOST_CONFIG_DIR")
        self.docker_config_dir = os.getenv("INTEGRAPH__CONFIG__ROOT_CONFIG_DIR")

        self.path_to_mapping_def = (
            self.cfg.mapping_file.replace(self.docker_config_dir, self.local_config_dir)
            if self.docker_config_dir in self.cfg.mapping_file
            else self.cfg.mapping_file
        )

        # path_to_yaml_rules = (
        #         self.path_to_yaml_rules.replace(
        #             self.docker_config_dir, self.local_config_dir
        #         )
        #         if self.docker_config_dir in self.path_to_yaml_rules
        #         else self.path_to_yaml_rules
        #     )

    def run_mapping(self, df, df_taxon, wdir, f_out):

        # Convert data frame to CSV files for mapping and save them in wdir
        self.df_to_csv(df, df_taxon, wdir)

        # Generate RML rules
        self.path_to_rml_rules = os.path.join(wdir, "mapping")
        rml_filepath = (
            self.path_to_rml_rules.replace(
                self.docker_config_dir, self.local_config_dir
            )
            if self.docker_config_dir in self.path_to_rml_rules
            else self.path_to_rml_rules
        )
        result = run_mapeathor(self.path_to_mapping_def, rml_filepath)
        if result["StatusCode"] != 0:
            raise RMLParsingException(result["logs"])

        self.path_to_rml_rules = os.path.join(wdir, "mapping.rml.ttl")
        if not os.path.exists(self.path_to_rml_rules):
            raise RMLParsingException(f"File {self.path_to_rml_rules} not found")

        # Apply RML mapping rules to convert data into RDF triples
        copy_file_to_dir(self.cfg.mapper_config_file, wdir)
        config_filepath = os.path.join(
            wdir, os.path.basename(self.cfg.mapper_config_file)
        )
        config_filepath = (
            config_filepath.replace(self.docker_config_dir, self.local_config_dir)
            if self.docker_config_dir in config_filepath
            else config_filepath
        )

        result = run_morph_kgc(config_filepath)
        if result["StatusCode"] != 0:
            raise RMLMappingException(result["logs"])

        path_to_rdf = os.path.join(wdir, "result.nq")
        if not os.path.exists(path_to_rdf):
            raise RMLMappingException(
                "Mapping of data to RDF failed ({} file not found)".format(path_to_rdf)
            )
        else:
            os.rename(
                path_to_rdf,
                os.path.join(os.path.dirname(path_to_rdf), os.path.basename(f_out)),
            )
            self.logger.info("Mapping of data to {} successful".format(f_out))

    # def run_yarrrml_parser_container(self, yaml_filename, rml_filename):
    #     remote_yaml_path = os.path.join("/data", yaml_filename)
    #     remote_rml_path = os.path.join("/data", rml_filename)
    #
    #     # Get the path to YARRRML rules on host machine
    #     path_to_yaml_rules = (
    #         self.path_to_yaml_rules.replace(
    #             self.docker_config_dir, self.local_config_dir
    #         )
    #         if self.docker_config_dir in self.path_to_yaml_rules
    #         else self.path_to_yaml_rules
    #     )
    #
    #     volume = {os.path.dirname(path_to_yaml_rules): {"bind": "/data", "mode": "rw"}}
    #     self.logger.debug(f"run_yarrrml_parser_container : mount volume {volume}")
    #
    #     parser_command = f"-i {remote_yaml_path} -o {remote_rml_path}"
    #     self.logger.debug(f"run_yarrrml_parser_container : command {parser_command}")
    #
    #     return self.client.containers.run(
    #         self.parser_image, parser_command, volumes=volume, remove=True
    #     )
    #
    # def run_rmlmapper_container(self, working_dir, rml_filename, rdf_filename):
    #     remote_rml_path = os.path.join("/data", rml_filename)
    #     remote_rdf_path = os.path.join("/data", rdf_filename)
    #
    #     w_dir = (
    #         working_dir.replace(self.docker_config_dir, self.local_config_dir)
    #         if self.docker_config_dir in working_dir
    #         else working_dir
    #     )
    #
    #     # TODO: replace working_dir by named volume (when it will be possible to mount subdir of named volumes)
    #     # See:  - https://stackoverflow.com/questions/50971417/docker-inside-docker-volume-is-mounted-but-empty
    #     #       - https://stackoverflow.com/questions/31381322/docker-in-docker-cannot-mount-volume
    #     #       - https://stackoverflow.com/questions/35841241/docker-compose-named-mounted-volume
    #     #       - https://stackoverflow.com/questions/38164939/can-we-mount-sub-directories-of-a-named-volume-in-docker
    #
    #     volume = {w_dir: {"bind": "/data", "mode": "rw"}}
    #     self.logger.debug(f"run_rmlmapper_container : mount volume {volume}")
    #
    #     mapper_command = f"-m {remote_rml_path} -o {remote_rdf_path} -d -s nquads"
    #     self.logger.debug(f"run_rmlmapper_container : command {mapper_command}")
    #
    #     return self.client.containers.run(
    #         self.mapper_image, mapper_command, volumes=volume, remove=True
    #     )

    # def convert_yarrrml_rules_to_rml(self):
    #
    #     yaml_filename = os.path.basename(self.path_to_yaml_rules)
    #     rml_filename = os.path.splitext(yaml_filename)[0] + ".rml"
    #
    #     self.path_to_rml_rules = os.path.join(
    #         os.path.dirname(self.path_to_yaml_rules), rml_filename
    #     )
    #
    #     self.logger.debug(
    #         f"run_yarrrml_parser_container : {self.path_to_yaml_rules} -> {self.path_to_rml_rules}"
    #     )
    #     response = self.run_yarrrml_parser_container(yaml_filename, rml_filename)
    #     self.logger.debug(f"yarrrml_parser response : {response}")
    #
    #     if os.path.exists(self.path_to_rml_rules):
    #         self.logger.info(
    #             "Conversion of {} to RML successful".format(
    #                 os.path.basename(self.path_to_yaml_rules)
    #             )
    #         )
    #     else:
    #         self.path_to_rml_rules = None
    #         raise YARRRMLParsingException(
    #             "Conversion of {} to RML failed".format(
    #                 os.path.basename(self.path_to_yaml_rules)
    #             )
    #         )
    #     return self.path_to_rml_rules
    #
    # def run_mapping(self, df, df_taxon, wdir, f_out):
    #     # Convert data frame to CSV files for mapping and save them in wdir
    #     self.df_to_csv(df, df_taxon, wdir)
    #
    #     # Convert YARRRML rules to RML rules
    #     if self.path_to_rml_rules == None:
    #         self.convert_yarrrml_rules_to_rml()
    #
    #     # Apply RML rules to transform data in CSV files into RDF triples
    #     if os.path.exists(self.path_to_rml_rules):
    #
    #         copy_file_to_dir(self.path_to_rml_rules, wdir)
    #
    #         rml_filename = os.path.basename(self.path_to_rml_rules)
    #         rdf_filename = os.path.basename(f_out)
    #
    #         self.logger.debug(f"run_rmlmapper_container : {wdir} -> {rdf_filename}")
    #         response = self.run_rmlmapper_container(wdir, rml_filename, rdf_filename)
    #         self.logger.debug(f"rmlmapper response : {response}")
    #
    #         if not os.path.exists(os.path.join(wdir, rdf_filename)):
    #             raise RMLMappingException(
    #                 "Mapping of data to RDF failed ({} file not found)".format(f_out)
    #             )
    #         else:
    #             self.logger.info("Mapping of data to {} successful".format(f_out))
    #     else:
    #         raise FileNotFoundError(self.path_to_rml_rules)

    # TODO : df_to_csv may become unnecessary when using Mapeathor, I can't say for now
    def df_to_csv(self, df, df_taxon, dst):
        sep = "\t"
        os.makedirs(dst, exist_ok=True)
        df["integraph_internal_id"] = df.index
        df.to_csv(os.path.join(dst, "data.tsv"), index=False, sep=sep)
        df_taxon["integraph_internal_id"] = df_taxon.index
        df_taxon.to_csv(os.path.join(dst, "taxa.tsv"), index=False, sep=sep)
