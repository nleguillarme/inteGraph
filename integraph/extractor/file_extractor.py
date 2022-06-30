import os
import argparse
import logging
import shutil
import requests
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from requests_file import FileAdapter
from .extractor import Extractor
from ..util.config_helper import read_config
from ..util.file_helper import clean_dir, copy_file_to_dir
from pyunpack import Archive, PatoolError
import functools


class FileExtractor(Extractor):
    def __init__(self, config):
        Extractor.__init__(self, "file_extractor")
        self.logger = logging.getLogger(__name__)
        self.cfg = config
        self.cfg.fetched_data_dir = os.path.join(self.cfg.output_dir, "fetched")
        self.cfg.ready_to_process_data_dir = os.path.join(
            self.cfg.output_dir, "ready_to_process"
        )
        parsed = urlparse(self.cfg.file_location)
        if parsed.scheme not in ("http", "https"):
            filepath = os.path.join(self.cfg.source_root_dir, self.cfg.file_location)
            if os.path.exists(filepath):
                self.cfg.file_location = "file://" + filepath
            else:
                raise FileNotFoundError("file {} not found".format(filepath))
        self.logger.info("New File Extractor with id {}".format(self.get_id()))

    def run(self):
        self.clean_data_dir()
        self.fetch_dataset()
        if not self.is_txt_file():
            self.unpack_archive()

    def clean_data_dir(self, **kwargs):
        clean_dir(self.cfg.output_dir)
        clean_dir(self.get_fetched_data_dir())
        clean_dir(self.get_ready_to_process_data_dir())

    def fetch_dataset(self, f_out, **kwargs):
        self.init_requests_session()
        self.logger.debug(f"Fetch dataset at {self.cfg.file_location}")
        with self.session.get(
            self.cfg.file_location, allow_redirects=True, stream=True
        ) as resp:
            resp.raw.decode_content = True
            resp.raise_for_status()
            # See https://github.com/psf/requests/issues/2155#issuecomment-50771010
            # resp.raw.read = functools.partial(resp.raw.read, decode_content=True)
            with open(f_out, "wb") as f:
                self.logger.debug(f"Write dataset to {f_out}")
                shutil.copyfileobj(resp.raw, f)

    def is_txt_file(self, **kwargs):
        try:
            with open(self.get_path_to_fetched_data(), "rb") as f:
                f.read()
                return True
        except:
            return False

    def unpack_archive(self, **kwargs):
        Archive(self.get_path_to_fetched_data()).extractall(self.cfg.fetched_data_dir)

    def set_ready(self, **kwargs):
        copy_file_to_dir(
            self.get_path_to_fetched_data(), self.get_ready_to_process_data_dir()
        )

    def get_path_to_fetched_data(self):
        return os.path.join(
            self.get_fetched_data_dir(),
            self.cfg.internal_id + "." + self.cfg.data_format,
        )

    def get_fetched_data_dir(self):
        return self.cfg.fetched_data_dir

    def get_ready_to_process_data_dir(self):
        return self.cfg.ready_to_process_data_dir

    def init_requests_session(self):
        self.session = requests.Session()
        self.session.mount("file://", FileAdapter())
        self.session.mount("http://", HTTPAdapter())
        self.session.mount("https://", HTTPAdapter())
