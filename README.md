![](https://i.ibb.co/C0jzrCk/integraph-logo.png)

`inteGraph` is a toolkit for ontology-based data integration in the biodiversity domain that allows generating data integration pipelines dynamically from configuration files, and scheduling and monitoring the execution of these pipelines.

`inteGraph` is powered by [Apache Airflow](https://airflow.apache.org/).

## Installation

1. Clone the project repository
``` console
git clone https://github.com/nleguillarme/inteGraph.git
```
2. Run install.sh
``` console
cd inteGraph ; sh install.sh
```

## Running inteGraph

To run inteGraph in Docker you just need to execute the following:
``` console
make up
```
This will create and start a set of containers that you can list with the `docker ps` command:
``` console
$ docker ps
CONTAINER ID   IMAGE                         COMMAND                  CREATED         STATUS                   PORTS                                       NAMES
21f4c8d28833   integraph-airflow-webserver   "/usr/bin/dumb-init …"   4 minutes ago   Up 3 minutes (healthy)   0.0.0.0:8080->8080/tcp, :::8080->8080/tcp   integraph-airflow-webserver-1
ab066b04f9b8   integraph-airflow-scheduler   "/usr/bin/dumb-init …"   4 minutes ago   Up 3 minutes (healthy)   8080/tcp                                    integraph-airflow-scheduler-1
3a5a84452e98   gnames/gognparser:latest      "gnparser -p 8778"       4 minutes ago   Up 4 minutes             0.0.0.0:8778->8778/tcp, :::8778->8778/tcp   integraph-gnparser-1
70b2ec620cda   postgres:13                   "docker-entrypoint.s…"   8 minutes ago   Up 8 minutes (healthy)   5432/tcp                                    integraph-postgres-1
```
In particular, this starts an instance of the Airflow scheduler and webserver. The webserver is available at http://localhost:8080.

To exit inteGraph and properly close all containers, run the following command:
``` console
make down
```

## Building a biodiversity knowledge graph with inteGraph

*Coming soon...*

## Cite inteGraph

Le Guillarme, N., & Thuiller, W. (2023). A Practical Approach to Constructing a Knowledge Graph for Soil Ecological Research. bioRxiv, 2023-03. https://doi.org/10.1101/2023.03.02.530763 

## Acknowledgments

`inteGraph` is running thanks to the following open-source softwares: [Mapeathor](https://github.com/oeg-upm/mapeathor), [Morph-KGC](https://morph-kgc.readthedocs.io/en/latest/), [nomer](https://github.com/globalbioticinteractions/nomer), [GNparser](https://github.com/gnames/gnparser), [confection](https://github.com/explosion/confection).

## License

License: Apache License 2.0

## Authors

inteGraph was written by [nleguillarme](https://github.com/nleguillarme/).
