from airflow.models.dagbag import DagBag


def test_import_dags():
    dags = DagBag()
    print("DAG import failures. Errors: {}".format(dags.import_errors))
    assert len(dags.import_errors) == 0
