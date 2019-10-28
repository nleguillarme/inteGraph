from abc import ABC, abstractmethod
import logging


class Linker(ABC):
    def __init__(self):
        self.logger = logging.getLogger(__name__)

        super().__init__()

    @abstractmethod
    def get_uri(self, entity):
        pass
