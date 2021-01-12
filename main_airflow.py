from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
import logging
import os
import json
import glob
from datetime import date, datetime, timedelta

from biodivgraph.utils.config_helper import read_config
from biodivgraph.building.pipelines import WorkflowFactory


def setup_logging(
    default_path="./logging.json", default_level=logging.INFO, env_key="LOG_CFG"
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


def register_dag(dag):
    logging.info("Register dag {}".format(dag.dag_id))
    globals()[dag.dag_id] = dag


def create_data_pipeline(config, default_args=None):

    today = date.today()
    dag_name = config.internal_id + "_" + today.strftime("%d%m%Y")

    extractor_id, extractor = factory.get_extractor_dag(config, default_args, dag_name)
    transformer_id, transformer = factory.get_transformer_dag(
        config, default_args, dag_name
    )
    loader_id, loader = factory.get_loader_dag(config, default_args, dag_name)

    schedule_interval = (
        config.scheduleInterval
        if "scheduleInterval" in config
        else default_args["schedule_interval"]
    )
    dag = DAG(
        dag_id=dag_name, default_args=default_args, schedule_interval=schedule_interval
    )

    start = DummyOperator(task_id="start", dag=dag)

    extract = SubDagOperator(
        task_id=extractor_id.split(".")[-1], subdag=extractor, dag=dag
    )

    transform = SubDagOperator(
        task_id=transformer_id.split(".")[-1], subdag=transformer, dag=dag
    )

    load = SubDagOperator(task_id=loader_id.split(".")[-1], subdag=loader, dag=dag)

    end = DummyOperator(task_id="end", dag=dag)

    start >> extract >> transform >> load >> end

    return dag


default_args = {
    "owner": "leguilln",
    "depends_on_past": False,
    "email": ["nicolas.leguillarme@univ-grenoble-alpes.fr"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
    "start_date": datetime(2020, 1, 1),
    "schedule_interval": timedelta(1),
}

setup_logging()
logger = logging.getLogger(__name__)

# Read scheduler config file to get properties file and jobs directory
scheduler_cfg_file = "/usr/local/airflow/dags/dags_test_data/scheduler-config.yml"
scheduler_cfg = read_config(scheduler_cfg_file)
root_dir = os.path.dirname(scheduler_cfg_file)
sources_dir = os.path.join(root_dir, scheduler_cfg.sources)

# Read loader config file
loader_cfg_file = os.path.join(root_dir, scheduler_cfg.loader)
loader_cfg = read_config(loader_cfg_file)

# Create RDFization jobs
logging.info("Collect integration jobs from {}".format(sources_dir))

jobs = []
factory = WorkflowFactory()
sources = [
    source_dir
    for source_dir in os.listdir(sources_dir)
    if os.path.isdir(os.path.join(sources_dir, source_dir))
]

for source in sources:
    source_dir = os.path.join(sources_dir, source)
    config = read_config(os.path.join(source_dir, "config", "config.yml"))
    config.root_dir = root_dir
    config.source_root_dir = source_dir
    config.run_on_localhost = False
    config.jars_location = "/usr/local/airflow/jars"
    config.output_dir = os.path.join(config.source_root_dir, "output")
    if "internal_id" not in config:
        config.internal_id = source
    logging.info(config)

    # register_dag(factory.get_extractor_dag(config, default_args))
    # register_dag(factory.get_transformer_dag(config, default_args))
    # register_dag(factory.get_loader_dag(config + loader_cfg, default_args))
    register_dag(create_data_pipeline(config + loader_cfg, default_args))
