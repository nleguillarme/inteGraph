from abc import ABC, abstractmethod
import logging


class DataSource(ABC):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        super().__init__()


class InteractionSource(DataSource):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        DataSource.__init__(self)

    @abstractmethod
    def get_interactions(self, **kwargs):
        pass

    @abstractmethod
    def resolve_interaction_name(self, **kwargs):
        pass


class OccurrenceSource(DataSource):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        DataSource.__init__(self)

    @abstractmethod
    def get_occurrences(self, **kwargs):
        pass


class TaxonomicSource(DataSource):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        DataSource.__init__(self)

    @abstractmethod
    def get_lineage(self, **kwargs):
        pass
