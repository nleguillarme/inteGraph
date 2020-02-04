from .taxon_linker import *
from .dictionary_linker import *
from ..core import TransformFactory
import os


class LinkerFactory:
    def get_linker(self, cfg):
        tranforms = TransformFactory().get_transforms(cfg)
        mapping = cfg["mapping"]
        if mapping == "taxName":
            return TaxNameLinker(tranforms)
        elif mapping == "taxId":
            return TaxIdLinker(tranforms)
        elif os.path.exists(mapping):
            return DictBasedLinker(mapping, tranforms)
        else:
            raise ValueError(mapping)
