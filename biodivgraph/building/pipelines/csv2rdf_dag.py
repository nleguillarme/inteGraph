import logging
import os

from datetime import datetime, timedelta
from airflow import DAG, settings
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException

from biodivgraph.building.core.dag_template import DAGTemplate
from biodivgraph.building.process import CSV2RDF


class CSV2RDFDAG(DAGTemplate):
    def __init__(self, service):
        DAGTemplate.__init__(self, "csv2rdf_" + service.get_id())
        self.logger = logging.getLogger(__name__)
        self.service = service

    def end(self, **kwargs):
        self.logger.info("Ending pipeline {}".format(self.get_dag_id()))

    def create_dag(self, schedule_interval, default_args, **kwargs):

        default_args["provide_context"] = True

        dag = DAG(  # Create DAG
            self.get_dag_id(),
            schedule_interval=schedule_interval,
            default_args=default_args,
        )

        clean = PythonOperator(
            task_id=self.get_dag_id() + "." + "clean_output_dir",
            python_callable=self.service.clean_output_dir,
            dag=dag,
        )

        # create_tmp = PythonOperator(
        #     task_id=self.get_dag_id() + "." + "create_tmp_dir",
        #     python_callable=self.service.create_tmp_dir,
        #     dag=dag,
        # )

        split = PythonOperator(
            task_id=self.get_dag_id() + "." + "split_in_chunks",
            python_callable=self.service.split_in_chunks,
            dag=dag,
        )

        end = PythonOperator(
            task_id=self.get_dag_id() + "." + "end", python_callable=self.end, dag=dag
        )

        nb_chunks = self.service.get_nb_chunks()
        triples_files = [
            self.service.get_triples_filename(chunk_num)
            for chunk_num in range(nb_chunks)
        ]

        f_output, f_graph = self.service.get_output_filenames()
        merge = PythonOperator(
            task_id=self.get_dag_id() + "." + "merge_chunks",
            python_callable=self.service.merge_chunks,
            provide_context=True,
            op_kwargs={"f_list_in": triples_files, "f_out": f_output},
            dag=dag,
        )

        for chunk_num in range(nb_chunks):

            f_in = self.service.get_chunk_filename(chunk_num)
            f_out = self.service.get_mapped_chunk_filename(chunk_num)

            map_taxo = PythonOperator(
                task_id=self.get_dag_id()
                + "."
                + "map_taxonomic_entities_{}".format(chunk_num),
                python_callable=self.service.map_taxonomic_entities,
                provide_context=True,
                op_kwargs={"f_in": f_in, "f_out": f_out},
                dag=dag,
            )

            if self.service.with_interactions_mapping():
                map_inter = PythonOperator(
                    task_id=self.get_dag_id()
                    + "."
                    + "map_interactions_{}".format(chunk_num),
                    python_callable=self.service.map_interactions,
                    provide_context=True,
                    op_kwargs={"f_in": f_out, "f_out": f_out},
                    dag=dag,
                )

            f_in = f_out
            f_out = self.service.get_triples_filename(chunk_num)
            wdir = self.service.get_onto_mapping_working_dir_template(chunk_num)

            triplify = PythonOperator(
                task_id=self.get_dag_id() + "." + "triplify_chunk_{}".format(chunk_num),
                python_callable=self.service.triplify_chunk,
                provide_context=True,
                op_kwargs={"f_in": f_in, "f_out": f_out, "wdir": wdir},
                dag=dag,
            )

            if self.service.with_interactions_mapping():
                split >> map_taxo >> map_inter >> triplify >> merge
            else:
                split >> map_taxo >> triplify >> merge

        set_graph_dst = PythonOperator(
            task_id=self.get_dag_id() + "." + "set_graph_dst",
            python_callable=self.service.set_graph_dst,
            provide_context=True,
            op_kwargs={"f_name": f_graph},
            dag=dag,
        )

        clean >> split
        split >> end  # In case chunks_count is zero
        merge >> set_graph_dst >> end

        return dag
