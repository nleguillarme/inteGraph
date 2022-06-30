import logging
import os

from datetime import datetime, timedelta
from airflow import DAG, settings
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule

from integraph.pipeline.dag_template import DAGTemplate
from integraph.extractor import SPARQLExtractor
from integraph.converter import ConverterFactory


class SPARQLExtractorDAG(DAGTemplate):
    def __init__(self, config, parent_dag_name=None):
        extractor = SPARQLExtractor(config)
        DAGTemplate.__init__(self, extractor.get_id(), parent_dag_name)
        self.logger = logging.getLogger(__name__)
        self.extractor = extractor
        self.converter = ConverterFactory().get_converter(config)

    def create_dag(self, schedule_interval, default_args, **kwargs):

        default_args["provide_context"] = True

        dag = DAG(  # Create DAG
            self.get_dag_id(),
            schedule_interval=schedule_interval,
            default_args=default_args,
        )

        clean = PythonOperator(
            task_id=self.get_dag_id() + "." + "clean_data_dir",
            python_callable=self.extractor.clean_data_dir,
            dag=dag,
        )

        f_out = self.extractor.get_path_to_fetched_data()

        fetch = PythonOperator(
            task_id=self.get_dag_id() + "." + "fetch_dataset",
            python_callable=self.extractor.fetch_dataset,
            op_kwargs={"f_out": f_out},
            dag=dag,
        )

        clean >> fetch

        end = PythonOperator(
            task_id=self.get_dag_id() + "." + "end", python_callable=self.end, dag=dag
        )

        if self.converter:
            f_in = self.extractor.get_path_to_fetched_data()
            out_dir = self.extractor.get_ready_to_process_data_dir()
            convert = PythonOperator(
                task_id=self.get_dag_id() + "." + "convert_to_data_frame",
                python_callable=self.converter,  # .convert,
                op_kwargs={
                    "f_in": f_in,
                    "out_dir": out_dir,
                },
                dag=dag,
            )
            fetch >> convert >> end
        else:
            ready = PythonOperator(
                task_id=self.get_dag_id() + "." + "set_ready",
                python_callable=self.extractor.set_ready,
                dag=dag,
            )
            fetch >> ready >> end

        return dag
