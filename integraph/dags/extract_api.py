import logging
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from pathlib import Path
from ..extractor.api import *
from ..util.staging_helper import StagingHelper, ensure_path
from airflow.utils.edgemodifier import Label


class ExtractAPI:
    def __init__(self, root_dir, config):
        self.tg_id = "extract"
        self.root_dir = ensure_path(root_dir)
        self.staging = StagingHelper(self.root_dir / "staging")
        self.cfg = config

    def extract(self):
        with TaskGroup(group_id=self.tg_id):
            # Fetch data
            self.staging.register("fetched")

            fetch_task = task(fetch)(
                **self.cfg, output_filepath=self.staging["fetched"] / "data.csv"
            )

            return fetch_task
