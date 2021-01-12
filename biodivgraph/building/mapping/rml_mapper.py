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


class RMLMappingEngine:
    def __init__(self, config, working_dir, jar_location):
        self.logger = logging.getLogger(__name__)
        self.config = config

        self.client = docker.from_env()
        self.mapper_image = self.client.images.get("rmlmapper:latest")
        self.parser_image = self.client.images.get("yarrrml-parser:latest")

        self.path_to_yaml_rules = self.config.ontological_mapping_file
        self.path_to_rml_rules = None

    def run_yarrrml_parser_container(self, yaml_filename, rml_filename):
        remote_yaml_path = os.path.join("/data", yaml_filename)
        remote_rml_path = os.path.join("/data", rml_filename)
        volume = {
            os.path.abspath(os.path.dirname(self.path_to_yaml_rules)): {
                "bind": "/data",
                "mode": "rw",
            }
        }
        parser_command = f"-i {remote_yaml_path} -o {remote_rml_path}"
        return self.client.containers.run(
            self.parser_image, parser_command, volumes=volume, remove=True
        )

    def run_rmlmapper_container(self, input_dir, rml_filename, rdf_filename):
        volume = {os.path.abspath(input_dir): {"bind": "/data", "mode": "rw"}}

        remote_rml_path = os.path.join("/data", rml_filename)
        remote_rdf_path = os.path.join("/data", rdf_filename)

        mapper_command = f"-m {remote_rml_path} -o {remote_rdf_path} -s nquads"
        return self.client.containers.run(
            self.mapper_image, mapper_command, volumes=volume, remove=True
        )

    def convert_yarrrml_rules_to_rml(self):

        yaml_filename = os.path.basename(self.path_to_yaml_rules)
        rml_filename = os.path.splitext(yaml_filename)[0] + ".rml"

        self.logger.debug(
            f"run_yarrrml_parser_container : {yaml_filename} -> {rml_filename}"
        )
        response = self.run_yarrrml_parser_container(yaml_filename, rml_filename)
        self.logger.debug(f"yarrrml_parser response : {response}")

        self.path_to_rml_rules = os.path.join(
            os.path.dirname(self.path_to_yaml_rules), rml_filename
        )
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

    def run_mapping(self, df, wdir, f_out):
        # Convert data frame to CSV files for mapping and save them in wdir
        self.df_to_csv(df, wdir)

        # Convert YARRRML rules to RML rules
        if self.path_to_rml_rules == None:
            try:
                self.convert_yarrrml_rules_to_rml()
            except YARRRMLParsingException as err:
                self.logger.error("{}. Abort !".format(err))
                raise

        # Apply RML rules to transform data in CSV files into RDF triples
        if os.path.exists(self.path_to_rml_rules):

            copy_file_to_dir(self.path_to_rml_rules, wdir)

            rml_filename = os.path.basename(self.path_to_rml_rules)
            rdf_filename = os.path.basename(f_out)

            self.logger.debug(f"run_rmlmapper_container : {wdir} -> {rdf_filename}")
            response = self.run_rmlmapper_container(wdir, rml_filename, rdf_filename)
            self.logger.debug(f"rmlmapper response : {response}")

            if not os.path.exists(os.path.join(wdir, f_out)):
                raise RMLMappingException(
                    "Mapping of data to RDF failed ({} file not found)".format(f_out)
                )
            else:
                self.logger.info("Mapping of data to {} successful".format(f_out))
        else:
            raise FileNotFoundError(self.path_to_rml_rules)

    def df_to_csv(self, df, dst):
        sep = "\t"
        os.makedirs(dst, exist_ok=True)

        df = df.rename(
            columns={
                self.config.subject_column_name: "sub",
                self.config.predicate_column_name: "pred",
                self.config.object_column_name: "obj",
                self.config.references_column_name: "references",
            }
        )

        df["id"] = df.index

        df_obj = df.loc[:, ["obj"]]
        df_obj["id"] = df.index

        df_pred = df.loc[:, ["pred"]]
        df_pred["id"] = df.index

        df.to_csv(os.path.join(dst, "s.tsv"), index=False, sep=sep)
        df_pred.to_csv(os.path.join(dst, "p.tsv"), index=False, sep=sep)
        df_obj.to_csv(os.path.join(dst, "o.tsv"), index=False, sep=sep)

        df_sub_taxon = df[
            [
                "sub",
                self.config.subject_name_column_name,
                self.config.subject_rank_column_name,
            ]
        ].rename(
            columns={
                "sub": "iri",
                self.config.subject_name_column_name: "scientific_name",
                self.config.subject_rank_column_name: "taxon_rank",
            }
        )
        df_obj_taxon = df[
            [
                "obj",
                self.config.object_name_column_name,
                self.config.object_rank_column_name,
            ]
        ].rename(
            columns={
                "obj": "iri",
                self.config.object_name_column_name: "scientific_name",
                self.config.object_rank_column_name: "taxon_rank",
            }
        )

        df_taxon = df_sub_taxon.append(df_obj_taxon, ignore_index=True).drop_duplicates(
            subset="iri"
        )

        df_taxon.to_csv(os.path.join(dst, "taxon.tsv"), index=False, sep=sep)
