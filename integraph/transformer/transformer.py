from ..core.component import Component
import logging


class Transformer(Component):
    def __init__(self, transformer_name):
        self.logger = logging.getLogger(__name__)
        Component.__init__(self, transformer_name)
