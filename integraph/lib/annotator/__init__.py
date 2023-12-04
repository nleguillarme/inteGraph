from pathlib import Path
import text2term
from .taxonomy import TaxonomyAnnotator
from .ontology import OntologyAnnotator
from .dictionary import DictionaryAnnotator

annotators = {
    "taxonomy": TaxonomyAnnotator,
    "ontology": OntologyAnnotator,
    "map": DictionaryAnnotator,
}
annotators_config = {}


def register_annotator(name, config):
    annotators_config[name] = config


def get_annotator_config(name):
    return annotators_config.get(name, None)


TAXONOMIES = {"gbif", "ncbi", "indexfungorum", "silva", "eol"}


# def needs_mapping(entity_cfg):
#     for annotator in entity_cfg.get("annotators"):
#         annotator_cfg = annotators_config.get(annotator, None)
#         if annotator_cfg:
#             if annotator_cfg.get("type") == "taxonomy":
#                 return True
#     return False


class AnnotatorFactory:
    @classmethod
    def get_annotator(self, name):
        annotator_cfg = annotators_config.get(name, None)
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
        # annotator_cfg = get_annotator(name)
        # if annotator_cfg:
        #     ann_type = annotator_cfg.get("type")
        #     if ann_type in "taxonomy":
        #         return TaxonomyAnnotator(annotator_cfg)
        #     if ann_type in "ontology":
        #         # if text2term.cache_exists(str(label)):
        #         return OntologyAnnotator(annotator_cfg)
        #     if ann_type in "map":
        #         # if Path(label).suffix == ".yml":
        #         return DictionaryAnnotator(annotator_cfg)
        #     raise Exception
        # else:
        #     raise Exception(
        #         f"Cannot find configuration information for annotator {name}"
        #     )
