from box import Box  # Advanced Python dictionaries with dot notation access
import yaml
import os


def read_config(filename):
    if os.path.exists(filename):
        config = Box.from_yaml(filename=filename, Loader=yaml.FullLoader)
        return config
    return None


# def read_config(file):
#     if os.path.exists(file):
#         with open(file, "r") as ymlfile:
#             cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
#             return cfg
#     return None
