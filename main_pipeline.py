from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
import yaml
import logging
import os
import json
import argparse

from bkgb.modules.datasources.import_job_factory import ImportJobFactory
from bkgb.modules.loaders.bulk import BulkLoader


def setup_logging(
    default_path="./logging.json", default_level=logging.DEBUG, env_key="LOG_CFG"
):
    """ Setup logging configuration
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, "rt") as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def parse_conf_file(path):
    if os.path.exists(path):
        with open(path, "r") as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
            return cfg
    return None


# def make_subdag(jobs, parent_dag_name, child_dag_name, args):
#     dag_subdag = DAG(
#         dag_id="{}.{}".formexplicit_defaults_for_timestamp = 1at(parent_dag_name, child_dag_name), default_args=args
#     )
#     with dag_subdag:
#         for job in jobs:
#             t = PythonOperator(
#                 task_id="{}.{}".format(child_dag_name, job.get_id()),
#                 python_callable=job.run,
#                 default_args=args,
#                 dag=dag_subdag,
#             )
#     return dag_subdag

default_args = {
    "owner": "leguilln",
    "depends_on_past": False,
    "start_date": datetime(2019, 12, 13),
    "email": ["nicolas.leguillarme@univ-grenoble-alpes.fr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

dag = DAG("main_publication_dag", default_args=default_args, schedule_interval="@once")

setup_logging()
logger = logging.getLogger(__name__)

dag_config = "/usr/local/airflow/dags/example/biodiv-graph-sample/scheduler-config.yml"
# dag_config = Variable.get("main_publication_config", default_var=None)

# parser = argparse.ArgumentParser("biodivnet")
# parser.add_argument("config", help="Path to task configuration files.", type=str)
# args = parser.parse_args()

# Read scheduler config file to get properties file and jobs directory
root_dir = os.path.dirname(dag_config)
scheduler_cfg = parse_conf_file(dag_config)

properties = parse_conf_file(os.path.join(root_dir, scheduler_cfg["properties"]))
load_job_cfg = parse_conf_file(os.path.join(root_dir, scheduler_cfg["loadJob"]))
import_jobs_dir = os.path.join(root_dir, scheduler_cfg["importJob"])

# Create reports directory
reports_dir = os.path.join(root_dir, "reports")
if not os.path.exists(reports_dir):
    os.makedirs(reports_dir)

# Create quads directory
quads_dir = os.path.join(root_dir, scheduler_cfg["quadsDirectory"])
if not os.path.exists(quads_dir):
    os.makedirs(quads_dir)
load_job_cfg["quadsDirectory"] = quads_dir

# Create jobs
logging.info("Collect import jobs from {}".format(import_jobs_dir))

import_jobs = []
job_factory = ImportJobFactory()
for job_config_file in os.listdir(import_jobs_dir):
    if job_config_file.endswith(".yml"):
        cfg_file = os.path.join(import_jobs_dir, job_config_file)
        cfg = parse_conf_file(cfg_file)
        cfg["quadsDirectory"] = quads_dir
        cfg["graphURI"] = properties["graphURI"]
        cfg["rootDir"] = import_jobs_dir
        cfg["reportsDir"] = reports_dir
        job = job_factory.get_import_job(cfg)
        import_jobs.append(job)

# publication_tasks = SubDagOperator(
#     task_id="publication_tasks",
#     subdag=load_subdag("main_publication_dag", "publication_tasks", default_args),
#     default_args=default_args,
#     dag=dag,
# )

logging.info(
    "Create export job from configuration file {}".format(
        os.path.join(root_dir, scheduler_cfg["loadJob"])
    )
)

load_job = BulkLoader(load_job_cfg)

import_tasks = [
    PythonOperator(
        task_id=job.get_id(),
        python_callable=job.run,
        default_args=default_args,
        dag=dag,
    )
    for job in import_jobs
]

load_task = PythonOperator(
    task_id=load_job.get_id(),
    python_callable=load_job.run,
    default_args=default_args,
    dag=dag,
)

import_tasks >> load_task
