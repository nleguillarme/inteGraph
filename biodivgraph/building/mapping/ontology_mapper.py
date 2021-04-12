import logging
import yaml
import os
import numpy as np
import pandas as pd
from owlready2 import *


class OntologyMapper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.cfg = config
        self.onto = None

    def load_ontology(self):
        onto_path = f"file:///{os.path.abspath(self.cfg.ontology_file)}"
        self.onto = get_ontology(onto_path)
        self.onto.load()
        if self.onto.loaded:
            self.logger.info(f"Loaded ontology {self.onto.name}")

    def get_iri(self, entity):
        iris = self.onto.search(label=entity)
        if not iris:
            iris = self.onto.search(label=entity.lower())
        if not iris:
            self.logger.debug("No match for entity {}".format(entity))
            return None
        elif len(iris) == 1:
            return {"type": "uri", "value": iris[0].iri}
        else:
            self.logger.error(
                "Multiple matches for entity {} : {}".format(entity, iris)
            )
            return None

    def map(self, df):
        self.logger.info(f"Start mapping entities using ontology {self.onto.name}")

        for column_cfg in self.cfg.columns:

            # mapper = VocabularyBasedEntityMapper(column_config)
            map = {}

            if column_cfg.uri_column not in df.columns:
                df[column_cfg.uri_column] = None  # np.nan

            uris = df[column_cfg.uri_column]
            entities = df[df[column_cfg.uri_column].isnull()][column_cfg.column_name]
            unique = entities.unique()
            self.logger.info(
                "Start mapping {}/{} unique entities in column {}".format(
                    len(unique), entities.shape[0], column_cfg.column_name
                )
            )

            for entity in unique:
                res = self.get_iri(entity)  # mapper.get_uri(entity)
                if res and res["type"] == "uri":
                    map[entity] = res["value"] if res["value"] else None

            for entity in map:
                df[column_cfg.uri_column] = np.where(
                    df[column_cfg.column_name] == entity,
                    map[entity],
                    df[column_cfg.uri_column],
                )

        # uri_colnames = [column_cfg.uri_column for column_cfg in self.cfg.columns]

        return df  # , uri_colnames
