export INTEGRAPH_ROOT_DIR=${PWD}
export INTEGRAPH_DOCKER_DIR=${INTEGRAPH_ROOT_DIR}/docker
export INTEGRAPH_PROJ_DIR=${HOME}/.integraph
export INTEGRAPH_NOMER_CACHE_DIR=${INTEGRAPH_PROJ_DIR}/.nomer
export INTEGRAPH_AIRFLOW_LOGS_DIR=${INTEGRAPH_PROJ_DIR}/logs

init:
	docker compose up airflow-init

reset:
	docker compose up airflow-reset

build:
	docker compose build

nomer_init:
	sudo rm -rf ${INTEGRAPH_NOMER_CACHE_DIR}
	mkdir -p ${INTEGRAPH_NOMER_CACHE_DIR}
	echo "nomer.cache.dir=${INTEGRAPH_NOMER_CACHE_DIR}" > ${INTEGRAPH_NOMER_CACHE_DIR}/nomer.prop
	echo -e "\tHomo sapiens" | nomer append ncbi -p ${INTEGRAPH_NOMER_CACHE_DIR}/nomer.prop
	echo -e "\tHomo sapiens" | nomer append gbif -p ${INTEGRAPH_NOMER_CACHE_DIR}/nomer.prop
	echo -e "\tHomo sapiens" | nomer append indexfungorum -p ${INTEGRAPH_NOMER_CACHE_DIR}/nomer.prop
	echo -e "\tHomo sapiens" | nomer append ott -p ${INTEGRAPH_NOMER_CACHE_DIR}/nomer.prop

up: export INTEGRAPH_TEST_MODE := False
up:
	sudo chown $(shell id -u) /var/run/docker.sock
	mkdir -p "${INTEGRAPH_NOMER_CACHE_DIR}"
	mkdir -p "${INTEGRAPH_AIRFLOW_LOGS_DIR}"
	docker compose up gnparser -d
	docker compose up airflow-webserver airflow-scheduler

dev: export INTEGRAPH_TEST_MODE := True
dev:
	sudo chown $(shell id -u) /var/run/docker.sock
	mkdir -p "${INTEGRAPH_NOMER_CACHE_DIR}"
	mkdir -p "${INTEGRAPH_AIRFLOW_LOGS_DIR}"
	docker compose up gnparser -d
	docker compose up airflow-webserver airflow-scheduler

down:
	docker compose down

clean:
	sudo rm -r "${INTEGRAPH_PROJ_DIR}"

upgrade: down reset install

install: build init nomer_init