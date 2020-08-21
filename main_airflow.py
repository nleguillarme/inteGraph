from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
import logging
import os
import json
import glob

from biodivgraph.utils.config_helper import read_config
from biodivgraph.building.pipelines import WorkflowFactory


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

# ROOT_DIR = "/usr/local/airflow/config/test"
# SERVICE_DIR = "/usr/local/airflow/config/test/services"

# Read scheduler config file to get properties file and jobs directory
scheduler_cfg_file = "/usr/local/airflow/dags/dags_test_data/scheduler-config.yml"
scheduler_cfg = read_config(scheduler_cfg_file)
root_dir = os.path.dirname(scheduler_cfg_file)
sources_dir = os.path.join(root_dir, scheduler_cfg.sources)


# Create dump directory
# dump_dir = os.path.join(root_dir, scheduler_cfg["dumpDirectory"])
# if not os.path.exists(dump_dir):
#     os.makedirs(dump_dir)

# load_job_cfg = read_config(os.path.join(root_dir, scheduler_cfg["publishingJob"]))
# load_job_cfg["dumpDirectory"] = dump_dir
# load_job_cfg["rootDir"] = root_dir

# Create reports directory
# reports_dir = os.path.join(root_dir, "reports")
# if not os.path.exists(reports_dir):
#     os.makedirs(reports_dir)

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
    config = read_config(os.path.join(source_dir, "config", "job.conf"))
    config.source_root_dir = source_dir
    config.run_on_localhost = False
    config.jars_location = "/usr/local/airflow/jars"
    if "internal_id" not in config:
        config.internal_id = source
    logging.info(config)

    # for cfg_file in glob.glob(os.path.join(service_dir, "*.yml")):
    #     cfg = read_config(cfg_file)
    #     logging.info(cfg)
    #     cfg["rootDir"] = root_dir
    #     cfg["serviceDir"] = service_dir
    #     cfg["dumpDirectory"] = dump_dir
    #     # cfg["graphURI"] = properties["graphURI"]
    #     # cfg["reportsDir"] = reports_dir

    workflow = factory.get_worflow(cfg=config, default_args=default_args)
    logging.info("Got {} DAGs".format(len(workflow)))
    for dag_id in workflow:
        logging.info("Register dag {}".format(dag_id))
        globals()[dag_id] = workflow[dag_id]

# Create publishing job
# workflow = factory.get_worflow(cfg=load_job_cfg, default_args=default_args)
# logging.info("Got {} DAGs".format(len(workflow)))
# for dag_id in workflow:
#     logging.info("Register dag {}".format(dag_id))
#     globals()[dag_id] = workflow[dag_id]
