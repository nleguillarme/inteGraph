from integraph.etl.factory import create_etl_dag
from integraph.util.config import registry, read_config
from integraph.util.path import ensure_path
from integraph.util.connections import register_connections

import pendulum
import logging
import os
import json

logger = logging.getLogger(__name__)

run_in_test_mode = os.getenv("INTEGRAPH__EXEC__TEST_MODE", default="False") == "True"
if run_in_test_mode == True:
    logger.info("Run in test mode")

### Read graph config
root_dir = ensure_path(
    os.getenv("INTEGRAPH__CONFIG__ROOT_CONFIG_DIR", default="/opt/airflow/config")
)
graph_cfg = read_config(root_dir / "graph.cfg")
logging.info(f"Found graph config at {graph_cfg}")

### Collect data sources
sources_dir = ensure_path(graph_cfg["sources"]["dir"])
sources_dir = sources_dir if sources_dir.is_absolute() else root_dir / sources_dir
logging.info(f"Collect data sources from {sources_dir}")

sources = [
    src.parent.relative_to(sources_dir)
    for src in sources_dir.rglob("*.cfg")
    if src.parent.is_dir()
]
ignored = []
if (root_dir / "sources.ignore").exists():
    with open(root_dir / "sources.ignore", "r") as f:
        ignored = f.readlines()
        ignored = [sources_dir / src.strip() for src in ignored]
sources = [src for src in sources if src not in ignored]

logging.info(f"Found {len(sources)} data sources: {sources}")

### Register airflow connections
connections = root_dir / "connections.json"
if connections.exists():
    config = json.load(connections.open())
    register_connections(config)


### Register ontologies
registry.ontologies.update(graph_cfg["ontologies"])


default_args = {
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": pendulum.duration(minutes=5),
}

dag_args = {
    "start_date": pendulum.today(),
    "schedule": "@once",
    "catchup": False,
}

for src in sources:
    src_dir = sources_dir / src
    morph_config_filepath = root_dir / graph_cfg["morph"]["config"]
    src_cfg = read_config(list(src_dir.glob("*.cfg"))[-1])
    create_etl_dag(
        graph_base_iri=graph_cfg["core"]["base_iri"],
        morph_config_filepath=morph_config_filepath,
        src_id=src_cfg["core"]["source_id"],
        src_dir=src_dir,
        extract_cfg=src_cfg["extract"],
        transform_cfg=src_cfg["transform"],
        load_cfg=graph_cfg["load"],
        dag_args=dag_args,
        default_args=default_args,
        run_in_test_mode=run_in_test_mode,
    )
