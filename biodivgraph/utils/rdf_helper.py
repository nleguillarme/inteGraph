import docker
import os


def split_rdf_files(rdf_dir, rdf_filename):
    client = docker.from_env()
    rdfsplit_image = client.images.get("rdfsplit:latest")
    volume = {os.path.abspath(rdf_dir): {"bind": "/data", "mode": "rw"}}
    split_command = f"-v -f --output split {rdf_filename}"
    response = client.containers.run(
        rdfsplit_image, split_command, volumes=volume, remove=True
    )
    print(response)
    if response:
        return_msg = response.decode("utf-8").strip("\n")
        return_msg_split = return_msg.split("\n")
        if len(return_msg_split) > 0 and return_msg_split[-1] == "Done.":
            return True
    return False
