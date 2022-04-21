import logging
import yaml
import os
import numpy as np
import pandas as pd
from owlready2 import *
from ..util.file_helper import cached_path


class OntologyNotFoundException(Exception):
    pass


class MultipleMatchesException(Exception):
    pass


class OntologyMapper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.cfg = config
        self.onto = None

    def map(self, df):
        self.logger.info(f"Start mapping entities using ontology {self.onto.name}")
        for column_cfg in self.cfg.columns:
            map = {}
            if column_cfg.uri_column not in df.columns:
                df[column_cfg.uri_column] = None
            temp_df = df.dropna(subset=[column_cfg.column_name])
            entities = temp_df[temp_df[column_cfg.uri_column].isnull()][
                column_cfg.column_name
            ]
            unique = entities.unique()
            self.logger.info(
                f"Start mapping {len(unique)} unique entities in column {column_cfg.column_name}"
            )
            for entity in unique:
                try:
                    res = self.get_iri(entity)
                except MultipleMatchesException as e:
                    self.logger.error(e)
                    res = None
                map[entity] = (
                    res["value"]
                    if (res and res["type"] == "uri" and res["value"])
                    else None
                )
            for entity in map:
                df[column_cfg.uri_column] = np.where(
                    df[column_cfg.column_name] == entity,
                    map[entity],
                    df[column_cfg.uri_column],
                )
        return df

    def load_ontology(self):
        onto_path = self.cfg.ontologies[self.cfg.ontology]
        if onto_path:
            self.onto = get_ontology(onto_path)
            self.onto.load()
            if self.onto.loaded:
                self.logger.info(f"Loaded ontology {self.onto.name}")
        else:
            raise OntologyNotFoundException(f"Path to ontology {self.onto} not found")

    def get_iri(self, entity):
        iris = self.search(entity)
        if not iris:
            self.logger.debug(f"No match for entity {entity}")
            return None
        elif len(iris) == 1:
            return {"type": "uri", "value": iris[0].iri}
        else:
            raise MultipleMatchesException(
                f"Multiple matches for entity {entity} : {iris}"
            )
        return None

    def search(self, entity):
        iris = self.onto.search(label=entity, _case_sensitive=False)
        if not iris:
            iris = self.onto.search(hasExactSynonym=entity, _case_sensitive=False)
        if not iris:
            iris = self.onto.search(hasRelatedSynonym=entity, _case_sensitive=False)
        if not iris:
            iris = self.onto.search(hasBroadSynonym=entity, _case_sensitive=False)
        return iris
