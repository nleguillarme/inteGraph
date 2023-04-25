import logging
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from ..lib.file import *
from ..util.staging import StagingHelper
from ..util.path import ensure_path
from airflow.utils.edgemodifier import Label


class ExtractFile:
    def __init__(self, root_dir, config):
        self.tg_id = "extract"
        self.root_dir = ensure_path(root_dir)
        self.staging = StagingHelper(self.root_dir / "staging")
        self.cfg = config

    def extract(self):
        # TODO : add clean dir

        with TaskGroup(group_id=self.tg_id):
            # Fetch data
            self.staging.register("fetched")
            url_task = task(get_url)(self.cfg["file_path"], self.root_dir)

            fetch_task = task(fetch)(url_task, self.staging["fetched"])

            cmd = f"patool test {fetch_task} ; echo $?"
            is_archive = BashOperator(task_id="is_archive", bash_command=cmd)
            fetch_task >> is_archive

            # Unpack if archive, else move to "extracted"
            # @task.branch(task_id="is_archive")
            # def branch(filepath):
            #     if is_archive(filepath):
            #         return self.tg_id + ".unpack"
            #     else:
            #         return self.tg_id + ".copy"
            @task.branch(task_id="branch")
            def branch(return_code):
                if return_code == 0:
                    return self.tg_id + ".unpack"
                else:
                    return self.tg_id + ".copy"

            self.staging.register("extracted")
            unpack_task = task(unpack)(
                filepath=fetch_task, output_dir=self.staging["extracted"]
            )
            copy_task = task(copy)(
                filepath=fetch_task, output_dir=self.staging["extracted"]
            )

            # Serve file to next task (transform)
            def serve(filepaths):
                return [f for f in filepaths if f][0]

            serve_task = task(trigger_rule="none_failed_min_one_success")(serve)(
                filepaths=[unpack_task, copy_task]
            )

            (
                branch(is_archive.output)#fetch_task)
                >> [Label("yes") >> unpack_task, Label("no") >> copy_task]
                >> serve_task
            )

            return serve_task
