VERSION=9.8.0
GRAPHDBFREE=/home/leguilln/Téléchargements/graphdb-free-${VERSION}-dist.zip
ROOTDIR=$(shell pwd)

all: build preload start

clean:
	chmod -R +w "./graphdb-docker/.git"
	rm -r ./graphdb-docker

build:
	git clone https://github.com/Ontotext-AD/graphdb-docker.git
	cp ${GRAPHDBFREE} ./graphdb-docker/free-edition
	make -C graphdb-docker free VERSION=${VERSION}

preload:
	cp preload.params ./graphdb-docker/preload/.env
	docker-compose -f ./graphdb-docker/preload/docker-compose.yml build
	docker-compose -f ./graphdb-docker/preload/docker-compose.yml up -d

start:
	cp docker-compose.yml ./graphdb-docker
	cp start.params ./graphdb-docker/.env
	docker-compose -f ./graphdb-docker/docker-compose.yml up -d

stop:
	docker-compose -f ./graphdb-docker/docker-compose.yml down

.PHONY: all build preload start stop
