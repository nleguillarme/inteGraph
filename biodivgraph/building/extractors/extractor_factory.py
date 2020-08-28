from . import *


class UnsupportedSourceException(Exception):
    pass


class ExtractorFactory:
    def __init__(self):
        pass

    def get_transformer(self, config):
        if "file_location" in config:
            return FileExtractor(config)
        else:
            raise UnsupportedSourceException()
