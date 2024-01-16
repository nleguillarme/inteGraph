from .taxonomy import TaxonomyAnnotator
from .ontology import OntologyAnnotator
from .dictionary import DictionaryAnnotator

annotators = {
    "taxonomy": TaxonomyAnnotator,
    "ontology": OntologyAnnotator,
    "map": DictionaryAnnotator,
}
annotators_config = {}


def register_annotator(src_id, annotator, config):
    if not annotators_config.get(src_id):
        annotators_config[src_id] = {}
    annotators_config[src_id][annotator] = config


def get_annotator_config(src_id, annotator):
    if annotators_config.get(src_id):
        return annotators_config.get(src_id).get(annotator, None)
    return None


TAXONOMIES = {"gbif", "ncbi", "indexfungorum", "silva", "eol"}


class AnnotatorFactory:
    @classmethod
    def get_annotator(self, src_id, name):
        annotator_cfg = get_annotator_config(src_id, name)
        if annotator_cfg:
            ann_type = annotator_cfg.get("type")
            annotator = annotators.get(ann_type, None)
            if not annotator:
                raise Exception(f"Unknown annotator type {ann_type}")
            return annotator(annotator_cfg)
        else:
            raise Exception(
                f"Cannot find configuration information for annotator {name}"
            )
