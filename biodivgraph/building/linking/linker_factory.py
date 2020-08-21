from .taxon_linker import *
from .dict_based_linker import *
from ..core import TransformFactory
import os


class LinkerFactory:
    def get_linker(self, cfg):
        tranforms = TransformFactory().get_transforms(cfg)
        if "mapping" not in cfg:
            return None
        mapping = cfg["mapping"]
        if mapping == "taxName":
            return TaxNameLinker(tranforms)
        elif mapping == "taxId":
            return TaxIdLinker(tranforms)
        elif os.path.exists(os.path.join(cfg["rootDir"], mapping)):
            return DictBasedLinker(os.path.join(cfg["rootDir"], mapping), tranforms)
        else:
            raise ValueError(mapping)
