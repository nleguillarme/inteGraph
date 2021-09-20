import docker
import os
import logging

logger = logging.getLogger(__name__)


def split_rdf_files(rdf_dir, rdf_filename, output_dir="split"):
    client = docker.from_env()
    rdfsplit_image = client.images.get("rdfsplit:latest")
    volume = {os.path.abspath(rdf_dir): {"bind": "/data", "mode": "rw"}}
    logger.debug(f"split_rdf_files : mount volume {volume}")

    split_command = f"-v -f --output {output_dir} {rdf_filename}"
    logger.debug(f"split_rdf_files : command {split_command}")

    response = client.containers.run(
        rdfsplit_image,
        split_command,
        volumes=volume,
        remove=True,
        user=int(os.getenv("USER_UID")),
    )

    logger.debug(f"split_rdf_files response : {response}")

    if response:
        return_msg = response.decode("utf-8").strip("\n")
        return_msg_split = return_msg.split("\n")
        if len(return_msg_split) > 0 and return_msg_split[-1] == "Done.":
            return True
    return False
