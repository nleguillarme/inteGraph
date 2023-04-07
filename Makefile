export INTEGRAPH__INSTALL__INSTALL_DIR=${PWD}/install
export INTEGRAPH__CONFIG__NOMER_CACHE_DIR=${HOME}/.integraph/.nomer
export INTEGRAPH__CONFIG__AIRFLOW_LOGS_DIR=${HOME}/.integraph/logs
export INTEGRAPH__CONFIG__HOST_CONFIG_DIR=/home/leguilln/workspace/data_integration/gratin-3
export INTEGRAPH__EXEC__TEST_MODE=false

init:
	docker compose up airflow-init

build:
	docker compose build

up:
	mkdir -p "${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}"
	mkdir -p "${INTEGRAPH__CONFIG__AIRFLOW_LOGS_DIR}"
	docker compose up gnparser -d
	docker compose up airflow-webserver airflow-scheduler

down:
	docker compose down