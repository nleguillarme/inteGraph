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
    """Setup logging configuration"""
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


def create_ETL_dag(cfg, default_args=None, test_mode=False):

    today = date.today()
    dag_name = cfg.internal_id + "_" + today.strftime("%d%m%Y")

    extractor_id, extractor = factory.get_extractor_dag(cfg, default_args, dag_name)
    transformer_id, transformer = factory.get_transformer_dag(
        cfg, default_args, dag_name
    )
    loader_id, loader = factory.get_loader_dag(cfg, default_args, dag_name)

    schedule_interval = (
        cfg.scheduleInterval
        if "scheduleInterval" in cfg
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

    end = DummyOperator(task_id="end", dag=dag)

    if not test_mode:
        load = SubDagOperator(task_id=loader_id.split(".")[-1], subdag=loader, dag=dag)
        start >> extract >> transform >> load >> end
    else:
        start >> extract >> transform >> end

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

test_mode = os.getenv("BDG_MODE", default=None) == "test"

# Read scheduler config file to get properties file and jobs directory
root_dir = os.getenv("CONFIG_DIR")

scheduler_cfg_file = os.path.join(root_dir, "scheduler-config.yml")
scheduler_cfg = read_config(scheduler_cfg_file)

sources_dir = (
    scheduler_cfg.sources
    if os.path.isabs(scheduler_cfg.sources)
    else os.path.join(root_dir, scheduler_cfg.sources)
)

# Read loader config file
loader_cfg_file = (
    scheduler_cfg.loader
    if os.path.isabs(scheduler_cfg.loader)
    else os.path.join(root_dir, scheduler_cfg.loader)
)
loader_cfg = read_config(loader_cfg_file)

# Create RDFization jobs
logging.info(f"Collect integration jobs from {sources_dir}")

jobs = []
factory = WorkflowFactory()
sources = [
    src_dir
    for src_dir in os.listdir(sources_dir)
    if os.path.isdir(os.path.join(sources_dir, src_dir))
]

with open(os.path.join(root_dir, "sources.ignore"), "r") as f:
    ignore_sources = f.readlines()
    ignore_sources = [src.strip() for src in ignore_sources]
sources = [src for src in sources if src not in ignore_sources]
logging.info(f"Found {len(sources)} integration jobs: {sources}")

for source in sources:
    src_dir = os.path.join(sources_dir, source)
    src_cfg = read_config(os.path.join(src_dir, "config", "config.yml"))
    src_cfg.root_dir = root_dir
    src_cfg.source_root_dir = src_dir
    src_cfg.run_on_localhost = False
    src_cfg.output_dir = os.path.join(src_dir, "output")
    if "internal_id" not in src_cfg:
        src_cfg.internal_id = source
    logging.info(f"Create new ETL pipeline : {src_cfg.internal_id} : {src_cfg}")
    register_dag(
        create_ETL_dag(src_cfg + loader_cfg, default_args, test_mode=test_mode)
    )
