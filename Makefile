export INTEGRAPH__CONFIG__HOST_CONFIG_DIR=$(shell pwd)/test-config
export INTEGRAPH__CONFIG__USER_ID=$(shell id -u)
export INTEGRAPH__CONFIG__GROUP_ID=$(shell id -g)
export INTEGRAPH__CONFIG__NOMER_CACHE_DIR=${HOME}/.integraph/.nomer
export INTEGRAPH__CONFIG__AIRFLOW_LOGS_DIR=${HOME}/.integraph/logs
export INTEGRAPH__INSTALL__INSTALL_DIR=${PWD}/install_files
export INTEGRAPH__CONFIG__DOCKER_GROUP_ID=$(shell getent group docker | cut -d: -f3)

################
# Initialization
################

nomer-cache:
		mkdir -p "${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}"

clean-nomer-cache:
		sudo rm -r "${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}"

################
# Development
################

test_dags:
		python -m pytest tests/test_dags.py

test:
		mkdir -p "${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}" ;\
		export INTEGRAPH__EXEC__TEST_MODE="True" ;\
		docker-compose -f docker-compose-LocalExecutor.yml up webserver

demo-test: nomer-cache
		export INTEGRAPH__EXEC__TEST_MODE="True" ;\
		export INTEGRAPH__CONFIG__HOST_CONFIG_DIR="/home/leguilln/workspace/KNOWLEDGE_INTEGRATION/gratin/gratin-demo" ;\
		docker-compose -f docker-compose-LocalExecutor.yml up webserver

demo: nomer-cache
		export INTEGRAPH__CONFIG__HOST_CONFIG_DIR="/home/leguilln/workspace/KNOWLEDGE_INTEGRATION/gratin/gratin-demo" ;\
		docker-compose -f docker-compose-LocalExecutor.yml up webserver

test_main:
		export INTEGRAPH__EXEC__TEST_MODE="True" ;\
	  export INTEGRAPH__CONFIG__ROOT_CONFIG_DIR="/home/leguilln/workspace/KNOWLEDGE_INTEGRATION/gratin/gratin-config" ;\
		python main_airflow.py

test_rml_mapper:
	export INTEGRAPH__CONFIG__ROOT_CONFIG_DIR="/home/leguilln/workspace/KNOWLEDGE_INTEGRATION/gratin/gratin-config" ;\
	python test_rml_mapper.py

################
# Production
################

build: build_yarrrml_parser build_rmlmapper build_nomer build_webserver

build_rdfizer:
		docker-compose -f docker-compose-LocalExecutor.yml build --no-cache sdm-rdfizer

build_morph_kgc:
		docker-compose -f docker-compose-LocalExecutor.yml build --no-cache morph-kgc

build_gnparser:
		docker-compose -f docker-compose-LocalExecutor.yml build --no-cache gnparser

build_yarrrml_parser:
		docker-compose -f docker-compose-LocalExecutor.yml build --no-cache yarrrml-parser

build_rmlmapper:
		docker-compose -f docker-compose-LocalExecutor.yml build --no-cache rmlmapper

build_nomer:
		docker-compose -f docker-compose-LocalExecutor.yml build --no-cache nomer

build_mapeathor:
		docker-compose -f docker-compose-LocalExecutor.yml build --no-cache mapeathor

build_webserver:
		docker-compose -f docker-compose-LocalExecutor.yml build --no-cache webserver

up:
		#@FERNET_KEY="$(shell docker run custom-airflow python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")"\
		mkdir -p "${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}"
		mkdir -p "${INTEGRAPH__CONFIG__AIRFLOW_LOGS_DIR}"
		docker-compose -f docker-compose-LocalExecutor.yml up webserver

down:
		docker-compose -f docker-compose-LocalExecutor.yml down

reset_db:
		docker exec integraph_webserver_1 airflow resetdb

#####################
# Triplestore init
#####################

#rdfox_server_directory:
#		docker run --rm -v${PWD}/RDFox-data/RDFox.lic:/opt/RDFox/RDFox.lic -v${PWD}/rdfox-server-directory:/home/rdfox/.RDFox -v${PWD}/RDFox-data/:/data -e RDFOX_ROLE=admin -e RDFOX_PASSWORD=leca2019 oxfordsemantic/rdfox-init

#graphdb_repository:
#		docker run --env GDB_JAVA_OPTS="-Xmx12g -Xms12g" -v ${PWD}/graphdb-data:/opt/graphdb/home --entrypoint sh /opt/graphdb/home/config/load_ncbi.sh -t ontotext/graphdb:9.8.0-free
		#sh graphdb-config/create_repo.sh

#clean-test-datastore:
#	curl -i -X PATCH admin:leca2019@localhost:12110/datastores/integraph-test?command=clear

.PHONY: test_dags test build up down reset_db
