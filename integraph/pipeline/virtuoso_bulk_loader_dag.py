import logging
import os

from datetime import datetime, timedelta
from airflow import DAG, settings
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.exceptions import AirflowException

from integraph.pipeline.dag_template import DAGTemplate
from integraph.loader import VirtuosoBulkLoader


class VirtuosoBulkLoaderDAG(DAGTemplate):
    def __init__(self, config, parent_dag_name=None):
        loader = VirtuosoBulkLoader(config)
        DAGTemplate.__init__(self, loader.get_id(), parent_dag_name)
        self.logger = logging.getLogger(__name__)
        self.loader = loader

    def create_dag(self, schedule_interval, default_args, **kwargs):

        default_args["provide_context"] = True

        dag = DAG(  # Create DAG
            self.get_dag_id(),
            schedule_interval=schedule_interval,
            default_args=default_args,
        )

        sensor = FileSensor(
            task_id=self.get_dag_id() + "." + "data_file_sensor",
            poke_interval=30,
            filepath=self.loader.get_local_data_dir(),
            dag=dag,
        )

        clean = PythonOperator(
            task_id=self.get_dag_id() + "." + "clean_shared_dir",
            python_callable=self.loader.clean_shared_dir,
            dag=dag,
        )

        split = PythonOperator(
            task_id=self.get_dag_id() + "." + "split_data",
            python_callable=self.loader.split_data,
            dag=dag,
        )

        create_script = PythonOperator(
            task_id=self.get_dag_id() + "." + "create_loader_script",
            python_callable=self.loader.create_loader_script,
            dag=dag,
        )

        run_script = PythonOperator(
            task_id=self.get_dag_id() + "." + "run_loader_script",
            python_callable=self.loader.run_loader_script,
            dag=dag,
        )

        end = PythonOperator(
            task_id=self.get_dag_id() + "." + "end", python_callable=self.end, dag=dag
        )

        sensor >> clean >> split >> create_script >> run_script >> end

        return dag
