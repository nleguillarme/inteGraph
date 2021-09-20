from abc import ABC, abstractmethod
import logging
import os


class Component(ABC):
    def __init__(self, component_name):
        self.logger = logging.getLogger(__name__)
        self.component_id = component_name
        super().__init__()

    def get_id(self):
        return self.component_id
