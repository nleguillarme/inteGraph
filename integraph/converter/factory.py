from .funguild_converter import FUNGuildConverter


class UnsupportedConverterException(Exception):
    pass


class ConverterFactory:
    def __init__(self):
        pass

    def get_converter(self, config):
        if "data_converter" in config:
            if config.data_converter == "FUNGuildConverter":
                return FUNGuildConverter(config)
            else:
                raise UnsupportedConverterException()
        return None
