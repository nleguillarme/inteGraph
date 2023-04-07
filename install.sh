echo "AIRFLOW_UID=$(id -u)" > .env
echo "DOCKER_GID=$(getent group docker | cut -d: -f3)" >> .env
echo "AIRFLOW_PROJ_DIR=${HOME}/.integraph"  >> .env

# docker compose up airflow-init
