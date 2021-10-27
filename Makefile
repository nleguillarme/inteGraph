#export INTEGRAPH__CONFIG__HOST_CONFIG_DIR=$(shell pwd)/test-config
export INTEGRAPH__CONFIG__HOST_CONFIG_DIR=/home/leguilln/workspace/KNOWLEDGE_INTEGRATION/gratin/integraph-config
export INTEGRAPH__CONFIG__USER_ID = $(shell id -u)
export INTEGRAPH__CONFIG__GROUP_ID = $(shell id -g)
export INTEGRAPH__CONFIG__DOCKER_GROUP_ID = $(shell awk -F\: '/docker/ {print $3}' /etc/group)
export INTEGRAPH__CONFIG__NOMER_CACHE_DIR = ${HOME}/.integraph/.nomer
export INTEGRAPH__INSTALL__INSTALL_DIR=${PWD}/install_files

################
# Development
################

test_dags:
		python -m pytest tests/test_dags.py

test:
		mkdir -p "${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}"
		INTEGRAPH__EXEC__TEST_MODE="True"\
		#INTEGRAPH__CONFIG__HOST_CONFIG_DIR="$(shell pwd)/test-config"\
		docker-compose -f docker-compose-LocalExecutor.yml up webserver

test_main:
		INTEGRAPH__EXEC__TEST_MODE="True"\
	  INTEGRAPH__CONFIG__ROOT_CONFIG_DIR="/home/leguilln/workspace/KNOWLEDGE_INTEGRATION/gratin/integraph-config"\
		python main_airflow.py

################
# Production
################

build:
		# docker build https://github.com/stain/rdfsplit.git -t rdfsplit:latest
		# Fork rdfsplit and change Dockerfile, build from our fork
		docker-compose -f docker-compose-LocalExecutor.yml build --no-cache webserver

up:
		#@FERNET_KEY="$(shell docker run custom-airflow python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")"\
		mkdir -p "${INTEGRAPH__CONFIG__NOMER_CACHE_DIR}"
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
