from . import *


class UnsupportedFormatException(Exception):
    pass


class TransformerFactory:
    def __init__(self):
        pass

    def get_transformer(self, config):
        if config.data_format == "csv":
            return CSV2RDF(config)
        else:
            raise UnsupportedFormatException(config.data_format)
