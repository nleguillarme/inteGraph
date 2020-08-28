import logging
import os

from datetime import datetime, timedelta
from airflow import DAG, settings
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowException

from biodivgraph.building.core.dag_template import DAGTemplate
from biodivgraph.building.extractors import FileExtractor


class FileExtractorDAG(DAGTemplate):
    def __init__(self, config, parent_dag_name=None):
        extractor = FileExtractor(config)
        dag_id = (
            parent_dag_name + "." if parent_dag_name else ""
        ) + "file_extractor_{}".format(extractor.get_id())
        DAGTemplate.__init__(self, dag_id)
        self.logger = logging.getLogger(__name__)
        self.extractor = extractor

    def end(self, **kwargs):
        self.logger.info("End execution of pipeline {}".format(self.get_dag_id()))

    def branch(self, unpack_task, end_task, **kwargs):
        if self.extractor.is_txt_file():
            return end_task
        else:
            return unpack_task

    def create_dag(self, schedule_interval, default_args, **kwargs):

        default_args["provide_context"] = True

        dag = DAG(  # Create DAG
            self.get_dag_id(),
            schedule_interval=schedule_interval,
            default_args=default_args,
        )

        clean = PythonOperator(
            task_id=self.get_dag_id() + "." + "clean_output_dir",
            python_callable=self.extractor.clean_output_dir,
            dag=dag,
        )

        fetch = PythonOperator(
            task_id=self.get_dag_id() + "." + "fetch_dataset",
            python_callable=self.extractor.fetch_dataset,
            dag=dag,
        )

        unpack = PythonOperator(
            task_id=self.get_dag_id() + "." + "unpack_archive",
            python_callable=self.extractor.unpack_archive,
            dag=dag,
        )

        end = PythonOperator(
            task_id=self.get_dag_id() + "." + "end", python_callable=self.end, dag=dag
        )

        branch = BranchPythonOperator(
            task_id=self.get_dag_id() + "." + "is_txt_file",
            provide_context=True,
            python_callable=self.branch,
            op_kwargs={"unpack_task": unpack.task_id, "end_task": end.task_id},
            dag=dag,
        )

        clean >> fetch >> branch >> [unpack, end]
        unpack >> end

        return dag
