import logging
import os

from datetime import datetime, timedelta
from airflow import DAG, settings
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.exceptions import AirflowException

from biodivgraph.building.core.dag_template import DAGTemplate
from biodivgraph.building.loaders import RDFoxLoader


class RDFoxLoaderDAG(DAGTemplate):
    def __init__(self, config, parent_dag_name=None):
        loader = RDFoxLoader(config)
        DAGTemplate.__init__(self, loader.get_id(), parent_dag_name)
        self.logger = logging.getLogger(__name__)
        self.loader = loader

    def branch(self, continue_task, end_task, **kwargs):
        if self.loader.test_connection_to_db():
            return continue_task
        else:
            return end_task

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
            filepath=self.loader.get_graph_dir(),
            dag=dag,
        )

        clean = PythonOperator(
            task_id=self.get_dag_id() + "." + "clean_split_dir",
            python_callable=self.loader.clean_split_dir,
            dag=dag,
        )

        split = PythonOperator(
            task_id=self.get_dag_id() + "." + "split_data",
            python_callable=self.loader.split_data,
            dag=dag,
        )

        load = PythonOperator(
            task_id=self.get_dag_id() + "." + "load_data",
            python_callable=self.loader.load_data,
            dag=dag,
        )

        end = PythonOperator(
            task_id=self.get_dag_id() + "." + "end", python_callable=self.end, dag=dag
        )

        branch = BranchPythonOperator(
            task_id=self.get_dag_id() + "." + "test_connection_to_db",
            provide_context=True,
            python_callable=self.branch,
            op_kwargs={"continue_task": clean.task_id, "end_task": end.task_id},
            dag=dag,
        )

        sensor >> branch >> [clean, end]
        # clean >> split >> load >> end
        clean >> load >> end

        return dag
