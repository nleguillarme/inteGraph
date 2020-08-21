from abc import ABC, abstractmethod
import logging
import os
from biodivgraph.utils.file_helper import delete_file
from datetime import date


class Job(ABC):
    def __init__(self, job_name):
        self.logger = logging.getLogger(__name__)
        today = date.today()
        self.job_id = job_name + "_" + today.strftime("%d%m%Y")
        self.tmp_dir = ".tmp/" + str(self.job_id)

        super().__init__()

    @abstractmethod
    def run(self):
        pass

    def get_id(self):
        return self.job_id

    def get_tmp_dir(self):
        return self.tmp_dir

    def create_tmp_dir(self):
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)

    def delete_tmp_dir():
        if os.path.exists(self.tmp_dir):
            self.logger.debug("Delete temporay files in {}".format(self.tmp_dir))
            for filename in os.listdir(self.tmp_dir):
                delete_file(os.path.join(self.tmp_dir, filename))
            self.logger.debug("Delete directory {}".format(self.tmp_dir))
            os.rmdir(self.tmp_dir)
