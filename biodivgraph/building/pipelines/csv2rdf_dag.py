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
        dag_id = (
            parent_dag_name + "." if parent_dag_name else ""
        ) + "csv2rdf_{}".format(transformer.get_id())
        DAGTemplate.__init__(self, dag_id)
        self.logger = logging.getLogger(__name__)
        self.transformer = transformer

    def end(self, **kwargs):
        self.logger.info("Ending pipeline {}".format(self.get_dag_id()))

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
            filepath=self.transformer.get_extracted_data_dir(),
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
            self.transformer.get_triples_filename(chunk_num)
            for chunk_num in range(nb_chunks)
        ]

        f_output, f_graph = self.transformer.get_output_filenames()
        merge = PythonOperator(
            task_id=self.get_dag_id() + "." + "merge_chunks",
            python_callable=self.transformer.merge_chunks,
            provide_context=True,
            op_kwargs={"f_list_in": triples_files, "f_out": f_output},
            dag=dag,
        )

        for chunk_num in range(nb_chunks):

            f_in = self.transformer.get_chunk_filename(chunk_num)
            f_out = self.transformer.get_mapped_chunk_filename(chunk_num)

            map_taxo = PythonOperator(
                task_id=self.get_dag_id()
                + "."
                + "map_taxonomic_entities_{}".format(chunk_num),
                python_callable=self.transformer.map_taxonomic_entities,
                provide_context=True,
                op_kwargs={"f_in": f_in, "f_out": f_out},
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
            f_out = self.transformer.get_triples_filename(chunk_num)
            wdir = self.transformer.get_onto_mapping_working_dir_template(chunk_num)

            triplify = PythonOperator(
                task_id=self.get_dag_id() + "." + "triplify_chunk_{}".format(chunk_num),
                python_callable=self.transformer.triplify_chunk,
                provide_context=True,
                op_kwargs={"f_in": f_in, "f_out": f_out, "wdir": wdir},
                dag=dag,
            )

            if self.transformer.with_interactions_mapping():
                split >> map_taxo >> map_inter >> triplify >> merge
            else:
                split >> map_taxo >> triplify >> merge

        set_graph_dst = PythonOperator(
            task_id=self.get_dag_id() + "." + "set_graph_dst",
            python_callable=self.transformer.set_graph_dst,
            provide_context=True,
            op_kwargs={"f_name": f_graph},
            dag=dag,
        )

        sensor >> clean >> split
        split >> end  # In case chunks_count is zero
        merge >> set_graph_dst >> end

        return dag
