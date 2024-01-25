import logging
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator

# from airflow.operators.python import ShortCircuitOperator
from ..lib.file import get_url, fetch, unpack, copy, is_archive
from ..util.staging import StagingHelper
from ..util.path import ensure_path


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
            fetch_task = task(fetch)(
                url_task, self.staging["fetched"], filename=self.cfg.get("file_name")
            )

            # cmd = f"patool test {fetch_task}"  # | tail -1"  # echo $?"
            # is_archive = BashOperator(
            #     task_id="is_archive", bash_command=cmd, do_xcom_push=True
            # )
            is_archive_task = task(is_archive)(filepath=fetch_task)
            # fetch_task >> is_archive_task

            # @task.short_circuit()
            # def is_archive(return_code):
            #     return int(return_code) == 0

            # @task.short_circuit()
            # def is_not_archive(return_code):
            #     return int(return_code) == 1

            @task.branch(task_id="branch")
            def branch(return_code):
                if return_code == True:
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

            file = self.cfg.get("file")
            if file:
                file = self.staging["extracted"] / file

            # Serve file to next task (transform)
            def serve(filepaths, file=None):
                if file:
                    if file.exists():
                        return str(file)
                    else:
                        raise FileNotFoundError(file)
                elif len(filepaths) == 1:
                    if type(filepaths[0]) is not list:
                        return str(filepaths[0])
                else:
                    return [str(f) for f in filepaths if f][0]
                raise ValueError(filepaths)

            serve_task = task(trigger_rule="none_failed_min_one_success")(serve)(
                filepaths=[unpack_task, copy_task], file=file
            )

            # is_archive_task = is_archive.override(ignore_downstream_trigger_rules=False)(test_archive.output)
            # is_not_archive_task = is_not_archive.override(ignore_downstream_trigger_rules=False)(test_archive.output)

            # is_archive_task >> unpack_task >> serve_task
            # is_not_archive_task >> copy_task >> serve_task

            (
                # branch(is_archive.output)
                branch(is_archive_task)
                >> [unpack_task, copy_task]
                >> serve_task
                # >> [Label("yes") >> unpack_task, Label("no") >> copy_task]
                # >> serve_task
            )

            return serve_task
