export INTEGRAPH__INSTALL__INSTALL_DIR=${PWD}/install
export INTEGRAPH__CONFIG__CACHE_DIR=${HOME}/.integraph
export INTEGRAPH__CONFIG__NOMER_CACHE_DIR=${INTEGRAPH__CONFIG__CACHE_DIR}/.nomer
export INTEGRAPH__CONFIG__AIRFLOW_LOGS_DIR=${INTEGRAPH__CONFIG__CACHE_DIR}/logs

cli:
	docker compose up airflow-cli

init:
	docker compose up airflow-init

reset:
	docker compose up airflow-reset

build:
	docker compose build

nomer_init:
	sudo rm -rf ${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}
	mkdir -p ${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}
	echo "nomer.cache.dir=${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}" > ${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}/nomer.prop
	echo -e "\tHomo sapiens" | nomer append ncbi -p ${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}/nomer.prop
	echo -e "\tHomo sapiens" | nomer append gbif -p ${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}/nomer.prop
	echo -e "\tHomo sapiens" | nomer append indexfungorum -p ${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}/nomer.prop
	echo -e "\tHomo sapiens" | nomer append ott -p ${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}/nomer.prop

up: export INTEGRAPH__EXEC__TEST_MODE := False
up:
	sudo chown $(shell id -u) /var/run/docker.sock
	mkdir -p "${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}"
	mkdir -p "${INTEGRAPH__CONFIG__AIRFLOW_LOGS_DIR}"
	docker compose up gnparser -d
	docker compose up airflow-webserver airflow-scheduler

dev: export INTEGRAPH__EXEC__TEST_MODE := True
dev:
	sudo chown $(shell id -u) /var/run/docker.sock
	mkdir -p "${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}"
	mkdir -p "${INTEGRAPH__CONFIG__AIRFLOW_LOGS_DIR}"
	docker compose up gnparser -d
	docker compose up airflow-webserver airflow-scheduler

down:
	docker compose down

clean:
	sudo rm -r "${INTEGRAPH__CONFIG__CACHE_DIR}"

upgrade: down build reset init
