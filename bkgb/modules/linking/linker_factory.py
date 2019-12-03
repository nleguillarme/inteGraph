from .taxonomy_linker import *
from .dictionary_linker import *
from bkgb.modules.core import TransformFactory
import os


class LinkerFactory:
    def get_linker(self, cfg):
        tranforms = TransformFactory().get_transforms(cfg)
        mapping = cfg["mapping"]
        if mapping == "taxonName":
            return TaxonNameLinker(tranforms)
        elif mapping == "taxonId":
            return TaxonIdLinker(tranforms)
        elif os.path.exists(mapping):
            return DictBasedLinker(mapping, tranforms)
        else:
            raise ValueError(dataType)
