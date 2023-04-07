from airflow import settings
from ..util.connections import create_connection


def connect(config):
    create_connection(config.get("id"), config)
    return config.get("id")


def graphdb(filepath, conn_id, config):
    from airflow.providers.http.hooks.http import HttpHook

    repository = config["repository"]
    http = HttpHook(http_conn_id=conn_id)
    response = http.run(
        headers={"Content-Type": "application/n-quads"},
        data=open(filepath, "rb"),
        endpoint=f"/repositories/{repository}/statements",
    )
    return str(response.text)
