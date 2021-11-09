VERSION=9.8.0
GRAPHDBFREE=/home/leguilln/Téléchargements/graphdb-free-${VERSION}-dist.zip
ROOTDIR=$(shell pwd)

all: build preload reinfer start

init: clean preload reinfer start

clear:
	chmod -R +w "./graphdb-docker/.git"
	rm -r ./graphdb-docker
	
clean:
	rm -rf ./graphdb-data

build:
	git clone https://github.com/Ontotext-AD/graphdb-docker.git
	cp ${GRAPHDBFREE} ./graphdb-docker/free-edition
	make -C graphdb-docker free VERSION=${VERSION}

preload:
	cp preload.params ./graphdb-docker/preload/.env
	docker-compose -f ./graphdb-docker/preload/docker-compose.yml build
	docker-compose -f ./graphdb-docker/preload/docker-compose.yml up
	
	
post:
	curl -X POST --data-binary @stwo.owl -H "Accept: application/rdf+xml" -H "Content-Type: application/rdf+xml;charset=UTF-8" http://localhost:7200/repositories/gratin/statements 
	
loadonto: start post stop

loadrdf:
	cp loadrdf.params ./graphdb-docker/loadrdf/.env
	docker-compose -f ./graphdb-docker/loadrdf/docker-compose.yml build
	docker-compose -f ./graphdb-docker/loadrdf/docker-compose.yml up
	#curl -X POST --data-binary @stwo.owl -H "Accept: application/rdf+xml" -H "Content-Type: application/rdf+xml;charset=UTF-8" http://localhost:7200/repositories/gratin/statements

start:
	cp docker-compose.yml ./graphdb-docker
	cp start.params ./graphdb-docker/.env
	docker-compose -f ./graphdb-docker/docker-compose.yml up -d
	
reinfer:
	curl --data-urlencode "update@reinfer.rq" http://localhost:7200/repositories/gratin/statements

stop:
	docker-compose -f ./graphdb-docker/docker-compose.yml down

.PHONY: all build preload start stop reinfer
