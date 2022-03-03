import logging
import yaml
import os
import numpy as np
import pandas as pd
from owlready2 import *
from ..util.config_helper import read_config


class MappingFileNotFoundException(Exception):
    pass


class ManualMapper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.cfg = config
        self.mapping = None
        self.load_mapping()

    def load_mapping(self):
        # print(self.cfg.ontology, self.cfg.ontologies)
        filepath = self.cfg.mapping_file
        if filepath and os.path.exists(filepath):
            # TODO : Get file or download and cache if needed
            self.mapping = read_config(filepath)
            if self.mapping:
                self.logger.info(f"Loaded mappings from file {filepath}")
        else:
            raise MappingFileNotFoundException(
                "Mapping file {} not found".format(filepath)
            )

    def get_iri(self, entity):
        iri = self.mapping[entity] if entity in self.mapping else None
        if not iri:
            self.logger.debug("No match for entity {}".format(entity))
            return None
        # elif len(iris) == 1:
        return {"type": "uri", "value": iri}
        # else:
        #     self.logger.error(
        #         "Multiple matches for entity {} : {}".format(entity, iris)
        #     )
        # return None

    def map(self, df):
        self.logger.info(
            f"Start mapping entities using mappings from {self.cfg.mapping_file}"
        )

        for column_cfg in self.cfg.columns:

            map = {}

            if column_cfg.uri_column not in df.columns:
                df[column_cfg.uri_column] = None

            df = df.dropna(subset=[column_cfg.column_name])
            entities = df[df[column_cfg.uri_column].isnull()][column_cfg.column_name]
            unique = entities.unique()
            self.logger.info(
                "Start mapping {}/{} unique entities in column {}".format(
                    len(unique), entities.shape[0], column_cfg.column_name
                )
            )

            for entity in unique:
                res = self.get_iri(entity)
                if res and res["type"] == "uri":
                    map[entity] = res["value"] if res["value"] else None

            for entity in map:
                df[column_cfg.uri_column] = np.where(
                    df[column_cfg.column_name] == entity,
                    map[entity],
                    df[column_cfg.uri_column],
                )

        return df
