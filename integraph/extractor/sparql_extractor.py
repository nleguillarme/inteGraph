import os
import argparse
import logging
import requests
from urllib.error import URLError
from urllib.parse import urlparse
from .extractor import Extractor
from ..util.config_helper import read_config
from ..util.file_helper import clean_dir, copy_file_to_dir
from ..util.request_manager import requests_retry_session
from pathlib import Path
import pandas as pd
from io import StringIO
from SPARQLWrapper import SPARQLWrapper, JSON
import json
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


class SPARQLExtractor(Extractor):
    def __init__(self, config):
        Extractor.__init__(self, "sparql_extractor")
        self.logger = logging.getLogger(__name__)
        self.cfg = config
        self.cfg.fetched_data_dir = os.path.join(self.cfg.output_dir, "fetched")
        self.cfg.ready_to_process_data_dir = os.path.join(
            self.cfg.output_dir, "ready_to_process"
        )
        self.endpoint = SPARQLWrapper(self.cfg.endpoint)
        self.endpoint.setReturnFormat(JSON)
        self.endpoint.setQuery(self.cfg.query)
        self.logger.info("New Web API Extractor with id {}".format(self.get_id()))

    def run(self):
        self.clean_data_dir()
        self.fetch_dataset()

    def clean_data_dir(self, **kwargs):
        clean_dir(self.cfg.output_dir)
        clean_dir(self.get_fetched_data_dir())
        clean_dir(self.get_ready_to_process_data_dir())

    def fetch_dataset(self, f_out, **kwargs):
        self.logger.debug(f"Fetch dataset at {self.cfg.endpoint}")
        results = self.endpoint.query().convert()
        if results["results"]:
            bindings = results["results"]["bindings"]
            with open(f_out, "w") as f:
                self.logger.debug(f"Write dataset to {f_out}")
                df = pd.read_json(json.dumps(bindings))
                df = df.applymap(lambda x: x["value"])
                df.to_csv(f_out, index=False)

    # def get_paginated_results(self, limit=1000):
    #     pages = []
    #     offset = 0
    #     while True:
    #         paginated_query = self.cfg.url + f"&offset={offset}&limit={limit}"
    #         with self.session.get(paginated_query) as resp:
    #             resp.raise_for_status()
    #             results = pd.read_csv(StringIO(resp.text), sep=self.cfg.delimiter)
    #             pages.append(results)
    #             nb_results = results.shape[0]
    #             # print(offset, nb_results)
    #             offset += nb_results
    #             if nb_results < limit:
    #                 break
    #     return pd.concat(pages, ignore_index=True).to_csv(index=False)

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
        Path(self.cfg.fetched_data_dir).mkdir(parents=True, exist_ok=True)
        return self.cfg.fetched_data_dir

    def get_ready_to_process_data_dir(self):
        Path(self.cfg.ready_to_process_data_dir).mkdir(parents=True, exist_ok=True)
        return self.cfg.ready_to_process_data_dir
