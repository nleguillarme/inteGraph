from .component import Component
import logging


class Loader(Component):
    def __init__(self, loader_name):
        self.logger = logging.getLogger(__name__)
        Component.__init__(self, loader_name)
