import logging
import yaml
from ..core import Linker


class VocabularyBasedEntityMapper(Linker):
    def __init__(self, mapping, transforms=None):
        Linker.__init__(self, transforms)
        self.logger = logging.getLogger(__name__)
        self.mapping = mapping

    def get_uri(self, entity):
        entity = self.apply_transforms(entity)
        if entity in self.mapping:
            return {"type": "uri", "value": self.mapping[entity]}
        else:
            self.logger.error("No mapping for entity : {}".format(entity))
            return {"type": "uri", "value": None}


class VocabularyMapper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.mapper = VocabularyBasedEntityMapper(self.get_dict())

    def get_dict(self):
        with open(self.config.interaction_mapping_file, "r") as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
            return cfg

    def map(self, df):
        self.logger.info("Start mapping interactions")
        interactions = df[self.config.predicate_column_name]
        map = {}
        unique = interactions.unique()
        self.logger.info(
            "Start mapping {}/{} unique interactions using {}".format(
                len(unique), interactions.shape[0], self.mapper.__class__.__name__
            )
        )
        for interaction in unique:
            res = self.mapper.get_uri(interaction)
            if res["type"] == "uri":
                map[interaction] = res["value"] if res["value"] else None
        self.logger.debug("Mapper {} terminated".format(self.mapper.__class__.__name__))
        interactions = interactions.replace(map)
        df[self.config.predicate_column_name] = interactions
        return df
