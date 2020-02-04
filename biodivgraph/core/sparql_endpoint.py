from . import DataSource
from abc import abstractmethod
import logging
from SPARQLWrapper import SPARQLWrapper, JSON


class SPARQLEndpoint(DataSource):
    def __init__(self, endpoint):
        self.logger = logging.getLogger(__name__)
        self.endpoint = endpoint
        self.sparql = SPARQLWrapper(self.endpoint)
        self.sparql.setReturnFormat(JSON)
        super().__init__()

    @abstractmethod
    def create_query(self, **kwargs):
        pass

    @abstractmethod
    def format_results(self, results):
        pass
