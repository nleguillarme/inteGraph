import pytest
import os
import docker
import subprocess
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DAG_PATH = ROOT_DIR
TEST_PATH = os.path.join(ROOT_DIR, "tests")


@pytest.fixture(scope="session", autouse=True)
def running_container():
    """
    This function runs the build for a container with airflow processes locally. Turns on webserver and scheduler.
    :param dag_path: (string) the filepath where dags live locally.
    :param test_path: (string) the filepath where the pytest script lives locally.
    :return: returns the docker container object.
    """
    uid = subprocess.check_output(["bash", "-c", "id -u"])

    client = docker.from_env()
    # image = client.images.build(
    #     path=ROOT_DIR,
    #     buildargs={"DOCKER_UID": "1001"},
    #     rm=True,
    #     tag="custom-airflow-for-test",
    # )

    running_container = client.containers.run(
        "custom-airflow",
        detach=True,
        ports={"8081/tcp": 8081},  # expose local port 8080 to container
        volumes={
            DAG_PATH: {"bind": "/usr/local/airflow/dags/", "mode": "rw"},
            TEST_PATH: {"bind": "/usr/local/airflow/test/", "mode": "rw"},
        },
    )
    running_container.exec_run(
        "airflow initdb", detach=True
    )  # docker execute command to initialize the airflow db
    running_container.exec_run(
        "airflow scheduler", detach=True
    )  # docker execute command to start airflow scheduler

    yield running_container


def test_dags(running_container):
    """
    This function runs a docker exec command on the container and will run our airflow DAG testing script. Once the test is complete return the exit status and output messsage from the docker execute command. Then the function stops the container.
    :return: the exit status and output message from the docker exec command of test script.
    """

    dag_test_output = running_container.exec_run(
        "pytest /usr/local/airflow/test/test_dags_script.py"
    )
    print(dag_test_output[1].decode("utf8"))
    ex_code = dag_test_output[0]

    running_container.stop()

    assert ex_code == 0
