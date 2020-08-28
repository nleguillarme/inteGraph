from ..core import Extractor
import os
import argparse
import logging
import shutil
import requests
from requests.adapters import HTTPAdapter
from requests_file import FileAdapter
from ...utils.config_helper import read_config
from ...utils.file_helper import clean_dir
from pyunpack import Archive, PatoolError


class FileExtractor(Extractor):
    def __init__(self, config):
        Extractor.__init__(self, "file_extractor")
        self.logger = logging.getLogger(__name__)

        self.config = config
        self.config.output_dir = os.path.join(self.config.source_root_dir, "output")
        self.config.output_data_dir = os.path.join(self.config.output_dir, "extracted")

    def run(self):
        self.clean_output_dir()
        self.fetch_dataset()
        if not self.is_txt_file():
            self.unpack_archive()

    def clean_output_dir(self, **kwargs):
        clean_dir(self.config.output_data_dir)

    def fetch_dataset(self, **kwargs):
        self.init_requests_session()
        with self.session.get(
            self.config.file_location, allow_redirects=True, stream=True
        ) as resp:
            resp.raise_for_status()
            with open(self.get_fetched_filename(), "wb") as f:
                shutil.copyfileobj(resp.raw, f)

    def unpack_archive(self, **kwargs):
        Archive(self.get_fetched_filename()).extractall(self.config.output_data_dir)

    def is_txt_file(self, **kwargs):
        try:
            with open(self.get_fetched_filename(), "tr") as f:
                f.read()
                return True
        except:
            return False

    def get_fetched_filename(self):
        return os.path.join(
            self.config.output_data_dir, self.config.file_location.rsplit("/", 1)[1]
        )

    def init_requests_session(self):
        self.session = requests.Session()
        self.session.mount("file://", FileAdapter())
        self.session.mount("http://", HTTPAdapter())
        self.session.mount("https://", HTTPAdapter())


def main():
    parser = argparse.ArgumentParser(
        description="csv2rdf pipeline command line interface."
    )

    parser.add_argument("cfg_file", help="YAML configuration file.")
    args = parser.parse_args()

    config = read_config(args.cfg_file)
    process = FileExtractor(config)

    process.run()


if __name__ == "__main__":
    main()
