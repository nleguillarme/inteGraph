from pathlib import Path
import text2term
from .taxonomy import TaxonomyAnnotator
from .ontology import OntologyAnnotator
from .dictionary import DictionaryAnnotator

TAXONOMIES = {"gbif", "ncbi", "indexfungorum", "silva", "eol"}

def needs_mapping(entity_cfg):
    for ann in entity_cfg.get("target"):
        if ann in TAXONOMIES:
            return True
    return False


class AnnotatorFactory:
    @classmethod
    def get_annotator(self, label):
        if label in TAXONOMIES:
            return TaxonomyAnnotator()
        if text2term.cache_exists(str(label)):
            return OntologyAnnotator(label)
        if Path(label).suffix == ".yml":
            return DictionaryAnnotator(label)
        raise Exception
