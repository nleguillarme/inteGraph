import logging
import os

from datetime import datetime, timedelta
from airflow import DAG, settings
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.exceptions import AirflowException

from biodivgraph.building.core.dag_template import DAGTemplate
from biodivgraph.building.transformers import CSV2RDF


class CSV2RDFDAG(DAGTemplate):
    def __init__(self, config, parent_dag_name=None):
        transformer = CSV2RDF(config)
        DAGTemplate.__init__(self, transformer.get_id(), parent_dag_name)
        self.logger = logging.getLogger(__name__)
        self.transformer = transformer

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
            filepath=self.transformer.get_fetched_data_dir(),
            dag=dag,
        )

        clean = PythonOperator(
            task_id=self.get_dag_id() + "." + "clean_output_dir",
            python_callable=self.transformer.clean_output_dir,
            dag=dag,
        )

        split = PythonOperator(
            task_id=self.get_dag_id() + "." + "split_in_chunks",
            python_callable=self.transformer.split_in_chunks,
            dag=dag,
        )

        end = PythonOperator(
            task_id=self.get_dag_id() + "." + "end", python_callable=self.end, dag=dag
        )

        nb_chunks = self.transformer.get_nb_chunks()
        triples_files = [
            self.transformer.get_path_to_triples(chunk_num)
            for chunk_num in range(nb_chunks)
        ]

        f_out = self.transformer.get_path_to_graph()
        merge = PythonOperator(
            task_id=self.get_dag_id() + "." + "merge",
            python_callable=self.transformer.merge,
            provide_context=True,
            op_kwargs={"f_in_list": triples_files, "f_out": f_out},
            dag=dag,
        )

        for chunk_num in range(nb_chunks):

            f_in = self.transformer.get_path_to_chunk(chunk_num)
            f_out = self.transformer.get_path_to_mapped_chunk(chunk_num)
            f_invalid = self.transformer.get_path_to_invalid_data(chunk_num)

            map_taxo = PythonOperator(
                task_id=self.get_dag_id()
                + "."
                + "map_taxonomic_entities_{}".format(chunk_num),
                python_callable=self.transformer.map_taxonomic_entities,
                provide_context=True,
                op_kwargs={"f_in": f_in, "f_out": f_out, "f_invalid": f_invalid},
                dag=dag,
            )

            if self.transformer.with_interactions_mapping():
                map_inter = PythonOperator(
                    task_id=self.get_dag_id()
                    + "."
                    + "map_interactions_{}".format(chunk_num),
                    python_callable=self.transformer.map_interactions,
                    provide_context=True,
                    op_kwargs={"f_in": f_out, "f_out": f_out},
                    dag=dag,
                )

            f_in = f_out
            f_out = self.transformer.get_path_to_triples(chunk_num)
            wdir = self.transformer.get_mapping_working_dir(chunk_num)

            triplify = PythonOperator(
                task_id=self.get_dag_id() + "." + "triplify_{}".format(chunk_num),
                python_callable=self.transformer.triplify,
                provide_context=True,
                op_kwargs={"f_in": f_in, "f_out": f_out, "wdir": wdir},
                dag=dag,
            )

            if self.transformer.with_interactions_mapping():
                split >> map_taxo >> map_inter >> triplify >> merge
            else:
                split >> map_taxo >> triplify >> merge

        sensor >> clean >> split
        split >> end  # In case chunks_count is zero
        merge >> end

        return dag
