from abc import ABC, abstractmethod
import logging


class Linker(ABC):
    def __init__(self, transforms):
        self.logger = logging.getLogger(__name__)
        self.transforms = transforms if transforms else []
        super().__init__()

    @abstractmethod
    def get_uri(self, entity):
        pass

    def apply_transforms(self, entity):
        for t in self.transforms:
            entity = t.apply(entity)
        return entity
