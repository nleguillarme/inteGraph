from integraph.etl.factory import create_etl_dag
from integraph.util.config import (
    read_config,
    validate_config,
    SCHEMA_SOURCE,
    SCHEMA_GRAPH,
)
from integraph.util.path import ensure_path
from integraph.util.connections import register_connections
from distutils.util import strtobool
import pendulum
import logging
import os
import json
import text2term


logger = logging.getLogger(__name__)


run_in_test_mode = strtobool(os.getenv("INTEGRAPH__EXEC__TEST_MODE", default="false"))
if run_in_test_mode == True:
    logger.info("Run in dev mode. The load module is skipped in dev mode.")


### Read graph config
root_dir = ensure_path(
    os.getenv("INTEGRAPH__CONFIG__ROOT_CONFIG_DIR", default="/opt/airflow/config")
)
graph_path = root_dir / "graph.cfg"
graph_cfg = read_config(root_dir / "graph.cfg")
validate_config(graph_cfg, SCHEMA_GRAPH)
logging.info(f"Found graph config at {graph_path}")

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
        ignored = [src.strip() for src in ignored]
    logger.info(
        f"Found sources.ignore. The following sources will be ignored: {ignored}"
    )
sources = [src for src in sources if str(src) not in ignored]
logging.info(f"Found {len(sources)} data sources: {sources}")

### Register airflow connections
connections = root_dir / "connections.json"
if connections.exists():
    config = json.load(connections.open())
    logger.info(f"Register airflow connections: {list(config.keys())}")
    register_connections(config)

### Register ontologies
ontologies = graph_cfg.get("ontologies")
if ontologies:
    logger.info(f"Register ontologies: {list(ontologies.keys())}")
    for onto in ontologies:
        if not text2term.cache_exists(onto):
            text2term.cache_ontology(ontologies[onto], onto)

default_args = {
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": pendulum.duration(minutes=5),
}

dag_args = {
    "start_date": pendulum.today(),
    "schedule ": "@once",
    "catchup": False,
}

for src in sources:
    src_dir = sources_dir / src
    src_cfg = read_config(list(src_dir.glob("*.cfg"))[-1])
    if validate_config(src_cfg, SCHEMA_SOURCE):
        src_id = src_cfg["source"]["id"]
        logger.info(f"Create DAG for source: {src_id}")
        create_etl_dag(
            base_iri=graph_cfg["graph"]["id"],
            src_dir=src_dir,
            src_config=src_cfg,
            load_config=graph_cfg["load"],
            dag_args=dag_args,
            default_args=default_args,
            run_in_test_mode=run_in_test_mode,
        )
