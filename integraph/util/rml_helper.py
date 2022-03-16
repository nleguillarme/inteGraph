import os
import docker
import logging
from docker.errors import NotFound, APIError

logging.basicConfig(level=logging.DEBUG)


def run_yarrrml_parser(yarrrml_filepath, rml_filepath):
    logger = logging.getLogger("run_yarrrml_parser")
    client = docker.from_env()

    local_dir = os.path.dirname(yarrrml_filepath)
    remote_yarrrml = os.path.join("/data", os.path.basename(yarrrml_filepath))
    remote_rml = os.path.join("/data", os.path.basename(rml_filepath))

    volume = {local_dir: {"bind": "/data", "mode": "rw"}}
    logger.debug(f"run_yarrrml_parser_container : mount volume {volume}")

    parser_command = f"-i {remote_yarrrml} -o {remote_rml}"
    logger.debug(f"run_yarrrml_parser_container : command {parser_command}")

    return run_container(client, "yarrrml-parser:latest", parser_command, volume)


def run_rml_mapper(rml_filepath, rdf_filepath):
    logger = logging.getLogger("run_rml_mapper")
    client = docker.from_env()

    remote_rml = os.path.join("/data", os.path.basename(rml_filepath))
    remote_rdf = os.path.join("/data", os.path.basename(rdf_filepath))

    local_dir = os.path.dirname(rml_filepath)

    volume = {local_dir: {"bind": "/data", "mode": "rw"}}
    logger.debug(f"run_rmlmapper_container : mount volume {volume}")

    mapper_command = f"-m {remote_rml} -o {remote_rdf} -d -s nquads"
    logger.debug(f"run_rmlmapper_container : command {mapper_command}")

    return run_container(client, "rmlmapper:latest", mapper_command, volume)


def run_mapeathor(spreadsheet_filepath, rml_filepath):
    logger = logging.getLogger("run_mapeathor")
    client = docker.from_env()

    remote_spreadsheet = os.path.join(
        "/mapeathor/data", os.path.basename(spreadsheet_filepath)
    )
    remote_rml = os.path.join("/mapeathor/result", os.path.basename(rml_filepath))

    local_dir = os.path.dirname(spreadsheet_filepath)

    volumes = [
        f"{os.path.dirname(spreadsheet_filepath)}:/mapeathor/data",
        f"{os.path.dirname(rml_filepath)}:/mapeathor/result",
    ]
    logger.debug(f"run_mapeathor : mount volume {volumes}")

    parser_command = f"{remote_spreadsheet} RML {remote_rml}"
    logger.debug(f"run_mapeathor : command ./run.sh {parser_command}")

    return run_container(
        client,
        "oegdataintegration/mapeathor:v1.5.3",
        parser_command,
        volumes,
        entrypoint="./run.sh",
    )


def run_morph_kgc(config_filepath):
    # docker run -v $(pwd):/data --rm morph-kgc:latest /data/config-morph.ini
    logger = logging.getLogger("run_morph_kgc")
    client = docker.from_env()

    remote_config = os.path.join("/data", os.path.basename(config_filepath))
    # remote_rml = os.path.join("/mapeathor/data", os.path.basename(rml_filepath))

    local_dir = os.path.dirname(config_filepath)

    volume = {local_dir: {"bind": "/data", "mode": "rw"}}
    logger.debug(f"run_morph_kgc : mount volume {volume}")

    mapper_command = f"{remote_config}"
    logger.debug(f"run_morph_kgc : command ./run.sh {mapper_command}")

    return run_container(client, "morph-kgc:latest", mapper_command, volume)


def run_sdm_rdfizer(config_filepath):
    # docker run -it --rm -v $(pwd)/example:/data rdfizer:latest -c config.ini
    logger = logging.getLogger("run_sdm_rdfizer")
    client = docker.from_env()

    remote_config = os.path.join("/data", os.path.basename(config_filepath))
    # remote_rml = os.path.join("/mapeathor/data", os.path.basename(rml_filepath))

    local_dir = os.path.dirname(config_filepath)

    volume = {local_dir: {"bind": "/data", "mode": "rw"}}
    logger.debug(f"run_sdm_rdfizer : mount volume {volume}")

    mapper_command = f"-c {remote_config}"
    logger.debug(f"run_sdm_rdfizer : command {mapper_command}")

    return run_container(client, "rdfizer:latest", mapper_command, volume)


def run_container(client, image, command, volumes, entrypoint=None):
    container = client.containers.run(
        client.images.get(image),
        command=command,
        volumes=volumes,
        entrypoint=entrypoint,
        detach=True,
    )
    result = container.wait()
    result["logs"] = container.logs().decode("utf-8")
    container.remove()
    return result
