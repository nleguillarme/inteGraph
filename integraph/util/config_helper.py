import inspect
from pathlib import Path


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
