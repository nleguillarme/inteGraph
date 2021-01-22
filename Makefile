init:
		pip install -r requirements.txt
		python setup.py install

test:
		python -m pytest tests

sequential:
		#FERNET_KEY := $(shell docker run puckel/docker-airflow python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")
		docker run -d -p 8081:8080 -e FERNET_KEY="$(FERNET_KEY)" -v $(shell pwd):/usr/local/airflow/dags puckel/docker-airflow webserver
		#docker run -d -e FERNET_KEY="$(FERNET_KEY)" -v $(shell pwd):/usr/local/airflow/dags puckel/docker-airflow

build:
		# docker build https://github.com/RMLio/yarrrml-parser.git -t yarrrml-parser:latest
		# docker build https://github.com/RMLio/rmlmapper-java.git -t rmlmapper:latest
		#docker build https://github.com/stain/rdfsplit.git -t rdfsplit:latest
		#Fork rdfsplit and change Dockerfile, build from our fork
		#TODO : add RDFox server directory initialization
		docker-compose -f docker-compose-LocalExecutor.yml build webserver

up:
		#@FERNET_KEY="$(shell docker run custom-airflow python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")"\
		# export BDG_SOURCES_DIR="$(shell pwd)/dags_test_data" \
		CONFIG_DIR="$(shell pwd)/pipe-config" docker-compose -f docker-compose-LocalExecutor.yml up webserver

down:
		docker-compose -f docker-compose-LocalExecutor.yml down

clean-graph:
		curl -i -X PATCH -H "Content-Type: application/x.datalog" "admin:leca2019@localhost:12110/datastores/biodivgraph-test/content?mode=delete" --data-raw ". [?s, ?p, ?o] ."

.PHONY: init test services up down
