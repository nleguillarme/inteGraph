import inspect
from cerberus import Validator
from confection import Config

SCHEMA_GRAPH = {
    "graph": {
        "required": True,
        "type": "dict",
        "schema": {
            "id": {"type": "string", "required": True, "minlength": 2},
        },
    },
    "sources": {
        "required": True,
        "type": "dict",
        "schema": {
            "dir": {"type": "string", "required": True, "minlength": 2},
        },
    },
    "load": {
        "required": True,
        "type": "dict",
    },
    "ontologies": {
        "required": False,
        "type": "dict",
        "valuesrules": {
            "type": "string",
        },
    },
}

SCHEMA_TAXONOMY = {
    "type": {
        "type": "string",
        "required": True,
        "allowed": ["taxonomy"],
    },
    "source": {
        "type": "string",
        "required": False,
        "allowed": ["GBIF", "NCBI", "IF", "default"],
    },
    "filter_on_ranks": {
        "type": ["string", "list"],
        "required": False,
    },
    "targets": {
        "type": "list",
        "required": False,
    },
    "multiple_match": {
        "type": "string",
        "required": False,
        "allowed": ["ignore", "warning", "strict"],
    },
    "include_synonym": {
        "type": "boolean",
        "required": False,
    },
}

SCHEMA_ONTOLOGY = {
    "type": {
        "type": "string",
        "required": True,
        "allowed": ["ontology"],
    },
    "shortname": {
        "type": "string",
        "required": True,
    },
}

SCHEMA_MAP = {
    "type": {
        "type": "string",
        "required": True,
        "allowed": ["map"],
    },
    "mapping_file": {
        "type": "string",
        "required": True,
    },
}

SCHEMA_SOURCE = {
    "source": {
        "required": True,
        "type": "dict",
        "schema": {
            "id": {"type": "string", "required": True, "minlength": 2},
            "metadata": {"type": "dict", "required": False},
        },
    },
    "annotators": {
        "required": True,
        "type": "dict",
        "valuesrules": {
            "type": "dict",
            "anyof_schema": [
                SCHEMA_TAXONOMY,
                SCHEMA_ONTOLOGY,
                SCHEMA_MAP,
            ],
        },
    },
    "extract": {
        "required": True,
        "type": "dict",
    },
    "transform": {
        "required": True,
        "type": "dict",
        "schema": {
            "format": {
                "type": "string",
                "required": True,
                "allowed": ["csv"],
            },
            "delimiter": {
                "type": "string",
                "required": True,
                "minlength": 1,
            },
            "chunksize": {
                "type": "integer",
                "required": True,
                "min": 1,
                "max": 5000,
            },
            "cleanse": {
                "type": "dict",
                "required": False,
                "schema": {
                    "script": {
                        "type": "string",
                        "required": True,
                    }
                },
            },
            "ets": {
                "type": "dict",
                "required": False,
                "schema": {
                    "na": {
                        "type": ["string", "number"],
                        "required": False,
                    },
                    "taxon_col": {
                        "type": "string",
                        "required": False,
                    },
                    "measurement_cols": {
                        "type": "list",
                        "required": False,
                    },
                    "additional_cols": {
                        "type": "list",
                        "required": False,
                    },
                    "index_col": {
                        "required": False,
                    },
                    "units": {
                        "required": False,
                    },
                },
            },
            "annotate": {
                "type": "dict",
                "required": True,
                "valuesrules": {
                    "type": "dict",
                    "schema": {
                        "id": {
                            "type": "string",
                            "required": False,
                        },
                        "label": {
                            "type": "string",
                            "required": False,
                        },
                        "annotators": {
                            "type": "list",
                            "schema": {"type": "string"},
                        },
                    },
                },
            },
            "triplify": {
                "type": "dict",
                "required": True,
                "schema": {
                    "mapping": {
                        "type": "string",
                        "required": True,
                    },
                },
            },
        },
    },
}


class ConfigurationError(Exception):
    pass


def read_config(filepath):
    return Config().from_disk(filepath)


def validate_config(config, schema):
    v = Validator(schema)
    valid = v.validate(config)
    if not valid:
        raise ConfigurationError(v.errors)
    return valid


class registry:
    ontologies = dict()

    @classmethod
    def register(cls, qname, qkey, qvalue):
        registry.get_registry(qname).update(qkey=qvalue)

    @classmethod
    def get_registry(cls, qname):
        """List all available registries."""
        names = []
        for name, value in inspect.getmembers(cls):
            if not name.startswith("_") and isinstance(value, dict) and name == qname:
                return value
