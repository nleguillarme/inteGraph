import logging

import os
import docker
from docker.errors import NotFound, APIError


class RobotHelper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.cfg = config
        self.user_uid = os.getuid()
        self.client = docker.from_env()
        self.robot_image = self.client.images.get("robot:latest")

    def extract(self, f_in, f_terms, f_out):
        with open(f_out, "w"):
            remote_input = os.path.join("/data", os.path.basename(f_in))
            remote_output = os.path.join("/data", os.path.basename(f_out))
            remote_terms = os.path.join("/data", os.path.basename(f_terms))

            volume = {
                # os.path.dirname(f_in): {"bind": "/data"},
                f_in: {"bind": remote_input},
                f_out: {"bind": remote_output},
                f_terms: {"bind": remote_terms},
            }
            self.logger.debug(f"robot_helper.extract : mount volume {volume}")

            robot_command = (
                "extract --method MIREOT"
                f" --input {remote_input}"
                f" --term-file {remote_terms}"
                f" -L {remote_terms}"
                f" --output {remote_output}"
            )
            self.logger.debug(f"robot_helper.extract : command {robot_command}")

            return self.client.containers.run(
                self.robot_image,
                robot_command,
                volumes=volume,
                remove=True,
                environment=["ROBOT_JAVA_ARGS=-Xmx14G"],
                user=self.user_uid,
            )
