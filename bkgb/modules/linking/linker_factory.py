from .taxonomy_matching import *


class LinkerFactory:
    def get_linker(self, dataType):
        if dataType == "taxonName":
            return TaxonNameLinker()
        elif dataType == "taxonId":
            return TaxonIdLinker()
        else:
            raise ValueError(dataType)
