![](https://i.ibb.co/C0jzrCk/integraph-logo.png)

inteGraph is a library of ETL (Extract-Transform-Load) components for ontology-based biodiversity data integration, powered by Apache Airflow.

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

## License

License: Apache License 2.0

## Authors

inteGraph was written by [nleguillarme](https://github.com/nleguillarme/).
