echo "AIRFLOW_UID=$(id -u)" > .env
echo "DOCKER_GID=$(getent group docker | cut -d: -f3)" >> .env
echo "AIRFLOW_PROJ_DIR=${HOME}/.integraph" >> .env
sudo chown $(id -u) /var/run/docker.sock
make install
