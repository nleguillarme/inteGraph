from abc import ABC, abstractmethod
import logging
import uuid
import os
from bkgb.utils.file_manager import delete_file


class Job(ABC):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.job_id = uuid.uuid1()
        self.tmp_dir = ".tmp/" + str(self.job_id)

        super().__init__()

    @abstractmethod
    def run(self):
        pass

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
