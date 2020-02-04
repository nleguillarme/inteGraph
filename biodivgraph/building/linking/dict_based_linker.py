import logging
import yaml
from ..core import Linker


class DictBasedLinker(Linker):
    def __init__(self, path, transforms):
        Linker.__init__(self, transforms)
        self.logger = logging.getLogger(__name__)
        self.fileLocation = path
        self.mapping = self.read_dict()

    def read_dict(self):
        with open(self.fileLocation, "r") as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
            return cfg

    def get_uri(self, entity):
        entity = self.apply_transforms(entity)
        if entity in self.mapping:
            return {"type": "uri", "value": self.mapping[entity]}
        else:
            self.logger.error("No mapping for entity : {}".format(entity))
            return {"type": "uri", "value": None}
