from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
import logging
import os
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from os.path import join, exists, isabs, isdir
from integraph.util.config_helper import read_config
from integraph.pipeline import PipelineFactory


def setup_logging(
    default_path="./logging.json", default_level=logging.INFO, env_key="LOG_CFG"
):
    """Setup logging configuration"""
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if exists(path):
        with open(path, "rt") as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def register_dag(dag):
    logging.debug("Register dag {}".format(dag.dag_id))
    globals()[dag.dag_id] = dag


def create_ETL_dag(
    cfg, factory=PipelineFactory(), default_args=None, run_in_test_mode=False
):

    today = date.today()
    dag_name = cfg.internal_id + "_" + today.strftime("%d%m%Y")

    ext_id, ext_dag = factory.get_extractor_dag(cfg, default_args, dag_name)
    tra_id, tra_dag = factory.get_transformer_dag(cfg, default_args, dag_name)

    schedule_interval = (
        cfg.scheduleInterval
        if "scheduleInterval" in cfg
        else default_args["schedule_interval"]
    )

    dag = DAG(
        dag_id=dag_name, default_args=default_args, schedule_interval=schedule_interval
    )

    # Create tasks
    start = DummyOperator(task_id="start", dag=dag)
    extract = SubDagOperator(task_id=ext_id.split(".")[-1], subdag=ext_dag, dag=dag)
    transform = SubDagOperator(task_id=tra_id.split(".")[-1], subdag=tra_dag, dag=dag)
    end = DummyOperator(task_id="end", dag=dag)

    if run_in_test_mode:
        start >> extract >> transform >> end
    else:
        loa_id, loa_dag = factory.get_loader_dag(cfg, default_args, dag_name)
        load = SubDagOperator(task_id=loa_id.split(".")[-1], subdag=loa_dag, dag=dag)
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

run_in_test_mode = os.getenv("INTEGRAPH__EXEC__TEST_MODE", default="False") == "True"
if run_in_test_mode == True:
    logger.info("Run in test mode")

# Read scheduler config file to get properties file and jobs directory
root_dir = os.getenv(
    "INTEGRAPH__CONFIG__ROOT_CONFIG_DIR", default="/opt/airflow/config"
)  # "/opt/airflow/config"
scheduler_cfg = read_config(join(root_dir, "scheduler-config.yml"))

# Read properties file
properties = read_config(join(root_dir, scheduler_cfg.properties))
# print(properties)

# Read loader config file
loader_cfg_file = (
    scheduler_cfg.loader
    if isabs(scheduler_cfg.loader)
    else join(root_dir, scheduler_cfg.loader)
)
loader_cfg = read_config(loader_cfg_file)

# Create integration jobs
sources_dir = (
    scheduler_cfg.sources
    if isabs(scheduler_cfg.sources)
    else join(root_dir, scheduler_cfg.sources)
)
logging.info(f"Collect data sources from {sources_dir}")

ignore_sources = []
if exists(join(root_dir, "sources.ignore")):
    with open(join(root_dir, "sources.ignore"), "r") as f:
        ignore_sources = f.readlines()
        ignore_sources = [join(sources_dir, src.strip()) for src in ignore_sources]

sources = [join(*path.parts[:-2]) for path in Path(sources_dir).glob("**/config.yml")]
sources = [src for src in sources if isdir(src) and src not in ignore_sources]

logging.info(f"Found {len(sources)} data sources: {sources}")

jobs = []
for src in sources:
    src_dir = src  # os.path.join(sources_dir, src)
    src_cfg = read_config(join(src_dir, "config", "config.yml"))
    src_cfg.ontologies = (
        properties.ontologies
    )  # os.path.join(root_dir, scheduler_cfg.ontologies)
    src_cfg.root_dir = root_dir
    src_cfg.source_root_dir = src_dir
    src_cfg.run_on_localhost = False
    src_cfg.output_dir = join(src_dir, "output")
    if "internal_id" not in src_cfg:
        src_cfg.internal_id = src

    logging.info(
        f"Create new ETL pipeline {src_cfg.internal_id}"  # for data source {src_cfg.internal_id}"
    )
    try:
        dag = create_ETL_dag(
            cfg=src_cfg + loader_cfg + properties,
            default_args=default_args,
            run_in_test_mode=run_in_test_mode,
        )
    except Exception as err:
        logging.error(
            f"Could not create ETL pipeline {src_cfg.internal_id} : {err}. Skip."
        )
    else:
        register_dag(dag)
