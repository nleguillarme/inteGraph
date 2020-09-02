from abc import ABC, abstractmethod
import logging


class DAGTemplate(ABC):
    def __init__(self, dag_id):
        self.logger = logging.getLogger(__name__)
        self.dag_id = dag_id
        super().__init__()

    @abstractmethod
    def create_dag(self, schedule_interval, default_args, **kwargs):
        pass

    def get_dag_id(self):
        return self.dag_id

    def end(self, **kwargs):
        self.logger.info("Ending pipeline {}".format(self.get_dag_id()))
