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
        self.local_config_dir = os.getenv("INTEGRAPH__CONFIG__HOST_CONFIG_DIR")
        self.docker_config_dir = os.getenv("INTEGRAPH__CONFIG__ROOT_CONFIG_DIR")
        self.path_to_mapping_def = (
            self.cfg.mapping_file.replace(self.docker_config_dir, self.local_config_dir)
            if self.docker_config_dir in self.cfg.mapping_file
            else self.cfg.mapping_file
        )
        self.path_to_rml_rules = None

    def map(self, df, df_taxon, wdir, f_out):  # df_taxon, wdir, f_out):

        # Convert data frame to CSV files for mapping and save them in wdir
        self.df_to_csv(df, df_taxon, wdir)  # df_taxon, wdir)

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

    def df_to_csv(self, df, df_taxon, dst):  # df_taxon, dst):
        sep = "\t"
        os.makedirs(dst, exist_ok=True)
        df["integraph_internal_id"] = df.index
        df.to_csv(os.path.join(dst, "data.tsv"), index=False, sep=sep)
        # df_taxon["integraph_internal_id"] = df_taxon.index
        df_taxon.to_csv(os.path.join(dst, "taxa.tsv"), index=False, sep=sep)
