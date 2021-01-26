import logging
import yaml
import numpy as np
import pandas as pd


class VocabularyBasedEntityMapper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.cfg = config
        self.mapping = self.get_mapping()

    def get_mapping(self):
        with open(self.cfg.mapping_file, "r") as ymlfile:
            mapping = yaml.load(ymlfile, Loader=yaml.FullLoader)
            self.logger.debug(f"{mapping}")
            return mapping

    def get_uri(self, entity):
        if entity in self.mapping:
            return {"type": "uri", "value": self.mapping[entity]}
        else:
            self.logger.error("No mapping for entity : {}".format(entity))
            return {"type": "uri", "value": None}


class VocabularyMapper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.cfg = config
        # self.mapper = VocabularyBasedEntityMapper(self.get_dict())

    # def get_dict(self, path):
    #     with open(self.cfg.interaction_mapping_file, "r") as ymlfile:
    #         cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    #         return cfg

    def map(self, df):
        self.logger.info("Start mapping entities")

        for column_config in self.cfg.columns:

            mapper = VocabularyBasedEntityMapper(column_config)
            map = {}

            if column_config.uri_column not in df.columns:
                df[column_config.uri_column] = np.nan

            uris = df[column_config.uri_column]
            entities = df[df[column_config.uri_column].isnull()][
                column_config.column_name
            ]
            unique = entities.unique()
            self.logger.info(
                "Start mapping {}/{} unique entities using {}".format(
                    len(unique), entities.shape[0], column_config.mapping_file
                )
            )
            for entity in unique:
                res = mapper.get_uri(entity)
                if res["type"] == "uri":
                    map[entity] = res["value"] if res["value"] else None

            # df[column_config.uri_column] = pd.Series(
            #     list(map.values()), index=list(map.keys())
            # )

            for entity in map:
                df[column_config.uri_column] = np.where(
                    df[column_config.column_name] == entity,
                    map[entity],
                    df[column_config.uri_column],
                )

        uri_colnames = [column_config.uri_column for column_config in self.cfg.columns]

        return df, uri_colnames

    # def map(self, df):
    #     self.logger.info("Start mapping interactions")
    #     interactions = df[self.config.predicate_column_name]
    #     map = {}
    #     unique = interactions.unique()
    #     self.logger.info(
    #         "Start mapping {}/{} unique interactions using {}".format(
    #             len(unique), interactions.shape[0], self.mapper.__class__.__name__
    #         )
    #     )
    #     for interaction in unique:
    #         res = self.mapper.get_uri(interaction)
    #         if res["type"] == "uri":
    #             map[interaction] = res["value"] if res["value"] else None
    #     self.logger.debug("Mapper {} terminated".format(self.mapper.__class__.__name__))
    #     interactions = interactions.replace(map)
    #     df[self.config.predicate_column_name] = interactions
    #     return df
