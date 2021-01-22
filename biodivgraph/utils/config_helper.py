from box import Box  # Advanced Python dictionaries with dot notation access
import yaml
import os


def read_config(filename):
    if os.path.exists(filename):
        config = Box.from_yaml(filename=filename, Loader=yaml.FullLoader)
        return config
    raise FileNotFoundException("f{filename} not found.")
