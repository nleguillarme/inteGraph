from abc import ABC, abstractmethod
import logging


class TransformFactory:
    def get_transforms(self, cfg):
        transforms = []
        if "transform" in cfg:
            t_list = cfg["transform"]
            for t in t_list:
                transforms.append(self.get_transform(t))
        return transforms

    def get_transform(self, cfg):
        if "prefix" in cfg:
            return T_AddPrefix(cfg["prefix"])
        else:
            raise ValueError(cfg)


class Transform(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def apply(self, entity):
        pass


class T_AddPrefix(Transform):
    def __init__(self, prefix):
        Transform.__init__(self)
        self.prefix = prefix

    def apply(self, entity):
        return self.prefix + str(entity)
