import logging
import logging.config
import json
import yaml
import os
import argparse

from bkgb.modules.datasources.import_job_factory import ImportJobFactory
from bkgb.modules.loaders.bulk import BulkLoader


def setup_logging(
    default_path="logging.json", default_level=logging.INFO, env_key="LOG_CFG"
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


if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser("kg-builder")
    parser.add_argument("path", help="Path to task configuration files.", type=str)
    args = parser.parse_args()

    # Read scheduler config file to get properties file and jobs directory
    root_dir = os.path.dirname(args.path)
    scheduler_cfg = parse_conf_file(args.path)
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

    job_list = []
    job_factory = ImportJobFactory()
    for job_config_file in os.listdir(import_jobs_dir):
        if job_config_file.endswith(".yml"):
            with open(os.path.join(import_jobs_dir, job_config_file), "r") as ymlfile:
                cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
                cfg["quadsDirectory"] = quads_dir
                cfg["graphURI"] = properties["graphURI"]
                cfg["rootDir"] = import_jobs_dir
                cfg["reportsDir"] = reports_dir
                job = job_factory.get_import_job(cfg)
                job_list.append(job)

    logging.info(
        "Create export job from configuration file {}".format(
            os.path.join(root_dir, scheduler_cfg["loadJob"])
        )
    )
    job_list.append(BulkLoader(load_job_cfg))

    # Run jobs
    for job in job_list:
        job.run()
