from abc import ABC, abstractmethod
import logging
import os


class Converter(ABC):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        super().__init__()

    @abstractmethod
    def convert(self, f_in, out_dir, **kwargs):
        pass
