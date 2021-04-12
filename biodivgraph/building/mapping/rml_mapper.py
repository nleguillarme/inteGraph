import logging

# import subprocess
import os
import sys
import shutil
from biodivgraph.utils.file_helper import copy_file_to_dir

import docker
from docker.errors import NotFound, APIError


class YARRRMLParsingException(Exception):
    pass


class RMLMappingException(Exception):
    pass


class MissingEnvironmentVariable(Exception):
    pass


class RMLMappingEngine:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.cfg = config

        self.client = docker.from_env()
        self.mapper_image = self.client.images.get("rmlmapper:latest")
        self.parser_image = self.client.images.get("yarrrml-parser:latest")

        self.path_to_yaml_rules = self.cfg.ontological_mapping_file
        self.path_to_rml_rules = None

    def run_yarrrml_parser_container(self, yaml_filename, rml_filename):
        remote_yaml_path = os.path.join("/data", yaml_filename)
        remote_rml_path = os.path.join("/data", rml_filename)

        volume = {
            os.path.dirname(self.path_to_yaml_rules): {"bind": "/data", "mode": "rw"}
        }
        self.logger.debug(f"run_yarrrml_parser_container : mount volume {volume}")

        parser_command = f"-i {remote_yaml_path} -o {remote_rml_path}"
        self.logger.debug(f"run_yarrrml_parser_container : command {parser_command}")

        return self.client.containers.run(
            self.parser_image, parser_command, volumes=volume, remove=True
        )

    def run_rmlmapper_container(self, working_dir, rml_filename, rdf_filename):
        remote_rml_path = os.path.join("/data", rml_filename)
        remote_rdf_path = os.path.join("/data", rdf_filename)

        volume = {working_dir: {"bind": "/data", "mode": "rw"}}
        self.logger.debug(f"run_rmlmapper_container : mount volume {volume}")

        mapper_command = f"-m {remote_rml_path} -o {remote_rdf_path} -s nquads"
        self.logger.debug(f"run_rmlmapper_container : command {mapper_command}")

        return self.client.containers.run(
            self.mapper_image, mapper_command, volumes=volume, remove=True
        )

    def convert_yarrrml_rules_to_rml(self):

        yaml_filename = os.path.basename(self.path_to_yaml_rules)
        rml_filename = os.path.splitext(yaml_filename)[0] + ".rml"

        self.path_to_rml_rules = os.path.join(
            os.path.dirname(self.path_to_yaml_rules), rml_filename
        )

        self.logger.debug(
            f"run_yarrrml_parser_container : {self.path_to_yaml_rules} -> {self.path_to_rml_rules}"
        )
        response = self.run_yarrrml_parser_container(yaml_filename, rml_filename)
        self.logger.debug(f"yarrrml_parser response : {response}")

        if os.path.exists(self.path_to_rml_rules):
            self.logger.info(
                "Conversion of {} to RML successful".format(
                    os.path.basename(self.path_to_yaml_rules)
                )
            )
        else:
            self.path_to_rml_rules = None
            raise YARRRMLParsingException(
                "Conversion of {} to RML failed".format(
                    os.path.basename(self.path_to_yaml_rules)
                )
            )
        return self.path_to_rml_rules

    def run_mapping(self, df, df_taxon, wdir, f_out):
        # Convert data frame to CSV files for mapping and save them in wdir
        self.df_to_csv(df, df_taxon, wdir)

        # Convert YARRRML rules to RML rules
        if self.path_to_rml_rules == None:
            self.convert_yarrrml_rules_to_rml()

        # Apply RML rules to transform data in CSV files into RDF triples
        if os.path.exists(self.path_to_rml_rules):

            copy_file_to_dir(self.path_to_rml_rules, wdir)

            rml_filename = os.path.basename(self.path_to_rml_rules)
            rdf_filename = os.path.basename(f_out)

            self.logger.debug(f"run_rmlmapper_container : {wdir} -> {rdf_filename}")
            response = self.run_rmlmapper_container(wdir, rml_filename, rdf_filename)
            self.logger.debug(f"rmlmapper response : {response}")

            if not os.path.exists(os.path.join(wdir, rdf_filename)):
                raise RMLMappingException(
                    "Mapping of data to RDF failed ({} file not found)".format(f_out)
                )
            else:
                self.logger.info("Mapping of data to {} successful".format(f_out))
        else:
            raise FileNotFoundError(self.path_to_rml_rules)

    def df_to_csv(self, df, df_taxon, dst):
        sep = "\t"
        os.makedirs(dst, exist_ok=True)
        df = df.rename(
            columns={
                self.cfg.subject_column_name: "sub",
                self.cfg.predicate_column_name: "pred",
                self.cfg.object_column_name: "obj",
                self.cfg.references_column_name: "references",
            }
        )
        df["ID"] = df.index
        df["sub"] = df["sub"].str.lower()
        df["obj"] = df["obj"].str.lower()
        df.to_csv(os.path.join(dst, "s.tsv"), index=False, sep=sep)
        df_taxon["ID"] = df_taxon.index
        df_taxon["src_iri"] = df_taxon["src_iri"].str.lower()
        df_taxon["tgt_iri"] = df_taxon["tgt_iri"].str.lower()
        df_taxon.to_csv(os.path.join(dst, "taxon.tsv"), index=False, sep=sep)
