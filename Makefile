init:
		pip install -r requirements.txt
		python setup.py install

test:
		python -m pytest tests

airflow:
		# initialize the database
		airflow initdb
		# start the web server, default port is 8080
		airflow webserver -p 8080
		# start the scheduler
		airflow scheduler

main_dag:
		airflow trigger_dag main_publication_dag

# FERNET_KEY := $(shell docker run puckel/docker-airflow python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")

docker_airflow:
		#FERNET_KEY := $(shell docker run puckel/docker-airflow python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")
		docker run -d -p 8081:8080 -e FERNET_KEY="$(FERNET_KEY)" -v $(shell pwd):/usr/local/airflow/dags puckel/docker-airflow webserver
		#docker run -d -e FERNET_KEY="$(FERNET_KEY)" -v $(shell pwd):/usr/local/airflow/dags puckel/docker-airflow

.PHONY: init test airflow main_dag docker_airflow
