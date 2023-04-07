from pathlib import Path
from ...util.config import registry
from .taxonomy import TaxonomyAnnotator
from .ontology import OntologyAnnotator
from .dictionary import DictionaryAnnotator


class AnnotatorFactory:
    @classmethod
    def get_annotator(self, label):
        if label in {"gbif", "ncbi", "indexfungorum", "silva", "eol"}:
            return TaxonomyAnnotator()
        if label in registry.ontologies:
            return OntologyAnnotator(registry.ontologies[label])
        if Path(label).suffix == ".yml":
            return DictionaryAnnotator(label)
        raise Exception
