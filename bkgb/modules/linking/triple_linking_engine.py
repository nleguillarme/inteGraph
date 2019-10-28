import logging
from .linker_factory import LinkerFactory


class TripleLinkingEngine:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.sub_linker = None
        self.pred_linker = None
        self.obj_linker = None

    def init_from_dict(self, cfg):
        sub_cfg = cfg["tripleSubject"]
        self.setSubjectLinker(LinkerFactory().get_linker(sub_cfg["dataType"]))
        # pred_cfg = cfg["triplePredicate"]
        # self.setPredicateLinker(LinkerFactory().get_linker(pred_cfg["dataType"]))
        obj_cfg = cfg["tripleObject"]
        self.setObjectLinker(LinkerFactory().get_linker(obj_cfg["dataType"]))

    def setSubjectLinker(self, linker):
        self.logger.info(
            "Register {} as subject linker".format(linker.__class__.__name__)
        )
        self.sub_linker = linker

    def setPredicateLinker(self, linker):
        self.logger.info(
            "Register {} as predicate linker".format(linker.__class__.__name__)
        )
        self.pred_linker = linker

    def setObjectLinker(self, linker):
        self.logger.info(
            "Register {} as object linker".format(linker.__class__.__name__)
        )
        self.obj_linker = linker

    def linkTriple(self, triple):
        s, p, o = triple
        s_uri = self.sub_linker.get_uri(s)
        p_uri = p  # self.pred_linker.get_uri(p)
        o_uri = self.obj_linker.get_uri(o)
        return s_uri, p_uri, o_uri

    def linkTriples(self, triples):
        for triple in triples:
            yield self.linkTriple(triple)
