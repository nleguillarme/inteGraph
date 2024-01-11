# User manual

[<<](index.md) Back to homepage

1. [Installation](#installation)
1. [Running inteGraph](#running-integraph)
1. [Create a new project](#create-a-new-project)
1. [Pipeline execution and monitoring](#pipeline-execution-and-monitoring)

## Installation

Clone the project repository
``` console
$ git clone https://github.com/nleguillarme/inteGraph.git
```
Run install.sh
``` console
$ cd inteGraph ; sh install.sh
```

## Running inteGraph

To run **inteGraph** in Docker you just need to execute the following:
``` console
$ make up
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
In particular, this starts an instance of the Airflow scheduler and webserver. The webserver is available at [http://localhost:8080](http://localhost:8080).

To exit **inteGraph** and properly close all containers, run the following command:
``` console
$ make down
```

## Create a new project

The structure of a typical **inteGraph** project looks like the following:

``` bash
my-project/
|-- graph.cfg
|-- connections.json
|-- sources.ignore
|
|-- sources/
|   |-- source_1/
|   |   |-- source.cfg
|   |   |-- mapping.xlsx
|   |   
|   |-- source_2/
|   |   |-- source.cfg
|   |   |-- mapping.xlsx
```

### Graph configuration: `graph.cfg`

*inteGraph* uses an INI-like file to configure the knowledge graph creation process. This configuration file can contain the following sections:

#### [core]

| Property | Description | Values
| --- | --- | --- |
| `id` | The base IRI of the knowledge graph.<br />It will be used to generate a graph label IRI for each data source. | Example: `http://leca.osug.fr/my_kg` |

#### [sources]

This section can be empty, in which case **inteGraph** will use the default property values.

| Property | Description | Values
| --- | --- | --- |
| `dir` | The path to the directory containing the configuration of the data sources.<br /> It can be absolute or relative to the directory containing `graph.cfg`. | Default: `sources` |

#### [load]

This section contains configuration properties for connecting to the triplestore. The properties in this section vary depending on the triplestore implementation.

| Property | Description | Values
| --- | --- | --- |
| `id`         | The identifier of the triplestore implementation. | Valid: `graphdb` |
| `conn_type`  | The type of connection. | Valid: `http` |
| `host`       | The URL or IP address of the host. | Example: `0.0.0.0` |
| `port`       | The port number. | Example: `7200` |
| `user`       | The user login. <br /> The user should have read/write permissions to the repository. | Example: `integraph` |
| `password`   | The user password.   | Example: `p@ssw0rd` |
| `repository` | The identifier of the target repository. | Example: `my-kg` |

#### [ontologies]

This section is optional and is used to declare the ontologies that will be searched during semantic annotation of your data. Each line is a key-value pair `shortname=iri` where `shortname` will be used as the ontology's internal identifier and `iri` is the ontology's IRI or a valid path to the ontology.

Below is an example graph configuration file:
``` ini
[graph]
id=http://leca.osug.fr/my_kg

[sources]
dir=sources

[load]
id=graphdb
conn_type=http
host=129.88.204.79
port=7200
repository=my-kg

[ontologies]
sfwo="http://purl.org/sfwo/sfwo.owl"
```

### Data source configuration

*Coming soon.*

## Pipeline execution and monitoring

*Coming soon.*
