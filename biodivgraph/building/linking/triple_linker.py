import logging
from .linker_factory import LinkerFactory
import pandas as pd
import multiprocessing as mp
import numpy as np


class TripleLinkingEngine:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.sub_linker = None
        self.pred_linker = None
        self.obj_linker = None

    def init_from_dict(self, cfg):
        print(cfg)
        sub_cfg = cfg["subject"]
        sub_cfg["rootDir"] = cfg["rootDir"]
        self.setSubjectLinker(LinkerFactory().get_linker(sub_cfg))
        pred_cfg = cfg["predicate"]
        pred_cfg["rootDir"] = cfg["rootDir"]
        self.setPredicateLinker(LinkerFactory().get_linker(pred_cfg))
        obj_cfg = cfg["object"]
        obj_cfg["rootDir"] = cfg["rootDir"]
        self.setObjectLinker(LinkerFactory().get_linker(obj_cfg))
        return self

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
        s_uri = self.sub_linker.get_uri(s) if self.sub_linker else s
        p_uri = self.pred_linker.get_uri(p) if self.pred_linker else p
        o_uri = self.obj_linker.get_uri(o) if self.obj_linker else o
        return s_uri, p_uri, o_uri

    def linkEntities(self, entities, linker):
        map = {}
        unique = entities.unique()
        self.logger.info(
            "Start linking {}/{} unique entities using {}".format(
                len(unique), entities.shape[0], linker.__class__.__name__
            )
        )
        for entity in unique:
            res = (
                linker.get_uri(entity)
                if linker != None
                else {"type": "uri", "value": entity}
            )
            if res["type"] == "uri":
                map[entity] = res["value"] if res["value"] else np.NaN
        self.logger.debug("Linker {} terminated".format(linker.__class__.__name__))
        return entities.replace(map)

    def linkTriples(self, triples):
        self.logger.info("Start linking triples")
        sub = self.linkEntities(triples.iloc[:, 0], self.sub_linker)
        pred = self.linkEntities(triples.iloc[:, 1], self.pred_linker)
        obj = self.linkEntities(triples.iloc[:, 2], self.obj_linker)
        df = pd.concat([sub, pred, obj], axis=1, sort=False)
        df.columns = ["s", "p", "o"]
        return df

    # def linkTriples(self, triples):
    #     self.logger.info("Start linking triples")
    #     pool = mp.Pool(mp.cpu_count())
    #     sub = pool.apply_async(
    #         self.linkEntities, args=(triples.iloc[:, 0], self.sub_linker)
    #     )
    #     pred = pool.apply_async(
    #         self.linkEntities, args=(triples.iloc[:, 1], self.pred_linker)
    #     )
    #     obj = pool.apply_async(
    #         self.linkEntities, args=(triples.iloc[:, 2], self.obj_linker)
    #     )
    #     return pd.concat([sub.get(), pred.get(), obj.get()], axis=1, sort=False)
