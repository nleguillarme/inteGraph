from .fungaltraits_converter import FungalTraitsConverter
from .rainford_converter import RainfordConverter
from .larochelle1991_converter import LaRochelle1991Converter
from .predator_prey_converter import PredatorPreyConverter

import os
import importlib.util


def import_module_from_path(path):
    name = os.path.splitext(os.path.basename(path))[0]
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class UnsupportedConverterException(Exception):
    pass


class ConverterFactory:
    def __init__(self):
        pass

    def get_converter(self, config):
        if "data_converter" in config:
            converter_path = os.path.join(config.source_root_dir, config.data_converter)
            if os.path.exists(converter_path):
                mod = import_module_from_path(converter_path)
                return mod.convert
            # if config.data_converter == "FungalTraitsConverter":
            #     return FungalTraitsConverter(config)
            # elif config.data_converter == "RainfordConverter":
            #     return RainfordConverter(config)
            # elif config.data_converter == "LaRochelle1991Converter":
            #     return LaRochelle1991Converter(config)
            else:
                raise UnsupportedConverterException(
                    "Unsupported converter {}".format(converter_path)
                )
        return None
