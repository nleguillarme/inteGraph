export USER_ID = $(shell id -u)
export GROUP_ID = $(shell id -g)
export NOMER_DIR = ${HOME}/.integraph/.nomer

init:
		pip install -r requirements.txt
		python setup.py install

test_dags:
		python -m pytest tests/test_dags.py

build:
		#docker build https://github.com/RMLio/yarrrml-parser.git -t yarrrml-parser:latest
		#docker build https://github.com/RMLio/rmlmapper-java.git -t rmlmapper:latest
		#docker build https://github.com/ontodev/robot.git -t robot:latest
		#docker build -f ./install_files/Dockerfile.nomer -t nomer:latest ./install_files
		# docker build https://github.com/stain/rdfsplit.git -t rdfsplit:latest
		# docker build nomer
		#Fork rdfsplit and change Dockerfile, build from our fork
		#TODO : add RDFox server directory initialization
		docker-compose -f docker-compose-LocalExecutor.yml build --no-cache webserver

run_nomer:
		mkdir -p "${NOMER_DIR}"
		USER_ID="${USER_ID}" GROUP_ID="${GROUP_ID}" NOMER_DIR="${NOMER_DIR}" docker-compose -f docker-compose-LocalExecutor.yml run nomer

rdfox_server_directory:
		docker run --rm -v${PWD}/RDFox-data/RDFox.lic:/opt/RDFox/RDFox.lic -v${PWD}/rdfox-server-directory:/home/rdfox/.RDFox -v${PWD}/RDFox-data/:/data -e RDFOX_ROLE=admin -e RDFOX_PASSWORD=leca2019 oxfordsemantic/rdfox-init

graphdb_repository:
		docker run --env GDB_JAVA_OPTS="-Xmx12g -Xms12g" -v ${PWD}/graphdb-data:/opt/graphdb/home --entrypoint sh /opt/graphdb/home/config/load_ncbi.sh -t ontotext/graphdb:9.8.0-free

		#sh graphdb-config/create_repo.sh

up:
		#@FERNET_KEY="$(shell docker run custom-airflow python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")"\
		mkdir -p "${NOMER_DIR}"
		INTEGRAPH__CONFIGURATION__CONFIG_DIR="$(shell pwd)/pipe-config" USER_ID="${USER_ID}" GROUP_ID="${GROUP_ID}" NOMER_DIR="${NOMER_DIR}" docker-compose -f docker-compose-LocalExecutor.yml up webserver

down:
		docker-compose -f docker-compose-LocalExecutor.yml down

test:
		mkdir -p "${NOMER_DIR}"
		INTEGRAPH__EXECUTION__TEST_MODE="True" INTEGRAPH__CONFIGURATION__CONFIG_DIR="$(shell pwd)/test-config" USER_ID="${USER_ID}" GROUP_ID="${GROUP_ID}" NOMER_DIR="${NOMER_DIR}" docker-compose -f docker-compose-LocalExecutor.yml up webserver

reset_db:
		docker exec integraph_webserver_1 airflow resetdb

clean-test-datastore:
	curl -i -X PATCH admin:leca2019@localhost:12110/datastores/integraph-test?command=clear

.PHONY: init test services up down
