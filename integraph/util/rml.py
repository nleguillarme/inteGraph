import os
import docker
import logging
from .path import ensure_path
from docker.errors import NotFound, APIError

logging.basicConfig(level=logging.DEBUG)


# def run_yarrrml_parser(yarrrml_filepath, rml_filepath):
#     logger = logging.getLogger("run_yarrrml_parser")
#     client = docker.from_env()

#     local_dir = os.path.dirname(yarrrml_filepath)
#     remote_yarrrml = os.path.join("/data", os.path.basename(yarrrml_filepath))
#     remote_rml = os.path.join("/data", os.path.basename(rml_filepath))

#     volume = {local_dir: {"bind": "/data", "mode": "rw"}}
#     logger.debug(f"run_yarrrml_parser_container : mount volume {volume}")

#     parser_command = f"-i {remote_yarrrml} -o {remote_rml}"
#     logger.debug(f"run_yarrrml_parser_container : command {parser_command}")

#     return run_container(client, "yarrrml-parser:latest", parser_command, volume)


# def run_rml_mapper(rml_filepath, rdf_filepath):
#     logger = logging.getLogger("run_rml_mapper")
#     client = docker.from_env()

#     remote_rml = os.path.join("/data", os.path.basename(rml_filepath))
#     remote_rdf = os.path.join("/data", os.path.basename(rdf_filepath))

#     local_dir = os.path.dirname(rml_filepath)

#     volume = {local_dir: {"bind": "/data", "mode": "rw"}}
#     logger.debug(f"run_rmlmapper_container : mount volume {volume}")

#     mapper_command = f"-m {remote_rml} -o {remote_rdf} -d -s nquads"
#     logger.debug(f"run_rmlmapper_container : command {mapper_command}")

#     return run_container(client, "rmlmapper:latest", mapper_command, volume)


def run_mapeathor(spreadsheet_filepath, rml_filepath):
    logger = logging.getLogger("run_mapeathor")
    client = docker.from_env()

    import shutil

    shutil.copyfile(spreadsheet_filepath, "mapeathor")

    # spreadsheet_filepath = (
    #     "/home/leguilln/workspace/data_integration/gratin-3/sources/funfun/mapping.xlsx"
    # )
    spreadsheet_filepath = ensure_path(spreadsheet_filepath)
    rml_filepath = ensure_path(rml_filepath)

    remote_spreadsheet = ensure_path("/mapeathor/data") / spreadsheet_filepath.name
    remote_rml = ensure_path("/mapeathor/result") / rml_filepath.name

    # volumes = {
    #     str(spreadsheet_filepath): {"bind": str(remote_spreadsheet), "mode": "ro"},
    #     str(rml_filepath.parent): {"bind": "/mapeathor/result", "mode": "rw"},
    # }
    volumes = {
        "mapeathor/mapping.xlsx": {"bind": str(remote_spreadsheet), "mode": "ro"},
        "mapeathor": {"bind": "/mapeathor/result", "mode": "rw"},
    }

    # volumes = [
    #     f"{spreadsheet_filepath}:{remote_spreadsheet}",
    #     f"{rml_filepath.parent}:/mapeathor/result",
    # ]

    parser_command = f"-i {remote_spreadsheet} -l RML -o {remote_rml}"
    print(parser_command, volumes)

    return run_container(
        client,
        "mapeathor:latest",
        parser_command,
        volumes,
    )


def run_morph_kgc(config_filepath):
    logger = logging.getLogger("run_morph_kgc")
    client = docker.from_env()
    config_filepath = ensure_path(config_filepath)
    remote_config = ensure_path("/morph-morphkgc/data/") / config_filepath.name
    local_dir = config_filepath.parent
    volume = {local_dir: {"bind": "/morphkgc/data", "mode": "rw"}}
    mapper_command = f"{remote_config}"
    print(mapper_command, volume)
    return run_container(client, "morph-kgc:latest", mapper_command, volume)


# def run_sdm_rdfizer(config_filepath):
#     logger = logging.getLogger("run_sdm_rdfizer")
#     client = docker.from_env()

#     remote_config = os.path.join("/data", os.path.basename(config_filepath))

#     local_dir = os.path.dirname(config_filepath)

#     volume = {local_dir: {"bind": "/data", "mode": "rw"}}
#     logger.debug(f"run_sdm_rdfizer : mount volume {volume}")

#     mapper_command = f"-c {remote_config}"
#     logger.debug(f"run_sdm_rdfizer : command {mapper_command}")

#     return run_container(client, "rdfizer:latest", mapper_command, volume)


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
