from ..core.component import Component
import logging


class Extractor(Component):
    def __init__(self, extractor_name):
        self.logger = logging.getLogger(__name__)
        Component.__init__(self, extractor_name)
