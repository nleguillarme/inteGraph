from abc import ABC, abstractmethod
import logging
import os
from datetime import date


class Component(ABC):
    def __init__(self, component_name):
        self.logger = logging.getLogger(__name__)
        today = date.today()
        self.component_id = component_name + "_" + today.strftime("%d%m%Y")

        super().__init__()

    def get_id(self):
        return self.component_id
