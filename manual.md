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

#### [graph]

| Property | Description | Values
| --- | --- | --- |
| `id` | The base IRI of the knowledge graph.<br />It will be used to generate a graph label IRI for each data source. | iri, example `http://leca.osug.fr/my_kg` |

#### [sources]

This section can be empty, in which case **inteGraph** will use the default property values.

| Property | Description | Values
| --- | --- | --- |
| `dir` | The path to the directory containing the configuration of the data sources.<br /> It can be absolute or relative to the directory containing `graph.cfg`. | path, default `sources` |

#### [load]

This section contains configuration properties for connecting to the triplestore. The properties in this section vary depending on the triplestore implementation.

| Property | Description | Values
| --- | --- | --- |
| `id`         | The identifier of the triplestore implementation. | {`graphdb`, `rdfox`} |
| `conn_type`  | The type of connection. | {`http`} |
| `host`       | The URL or IP address of the host. | url or ip, example `0.0.0.0` |
| `port`       | The port number. | int, example `7200` |
| `user`       | The user login. <br /> The user should have read/write permissions to the repository. | str, optional, example `integraph` |
| `password`   | The user password.   | str, optional, example `p@ssw0rd` |
| `repository` | The identifier of the target repository. | str, example `my-kg` |

#### [ontologies]

This section is optional and is used to declare the ontologies that will be searched during the semantic annotation of your data. Each line is a key-value pair `shortname=iri` where `shortname` will be used as the ontology's internal identifier and `iri` is the ontology's IRI or a valid path to the ontology.

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

Each data source to be integrated into the knowledge graph must have its own INI-like configuration file. To add a new data source to your **inteGraph** project, create a new directory in your sources directory, and create a file `source.cfg` in this directory. This configuration file can contain the following sections:

#### [source]

| Property | Description | Values
| --- | --- | --- |
| `id` | The internal unique identifier of the data source. | str, example `source_1` |

#### [source.metadata]

This subsection can be empty. You can use it to specify metadata about the data source using any of the fifteen terms in the [Dublin Core™ Metadata Element Set](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/) (also known as "the Dublin Core").

Below is an example `[source.metadata]` section specifying metadata for [the BETSI database](https://portail.betsi.cnrs.fr/):

``` ini
[source.metadata]
title=A database for soil invertebrate biological and ecological traits
creator=Hedde et al.
subject=araneae, carabidae, chilopoda, diplopoda, earthworms, isopoda
description=The Biological and Ecological Traits of Soil Invertebrates database (BETSI, https://portail.betsi.cnrs.fr/) is a European database dedicated specifically to soil organisms’ traits.
date=2021
format=csv
identifier=hal-03581637
language=en
```

#### [annotators]

This section allows you to define any number of semantic annotators. The role of a semantic annotator is to match a piece of data with the concept in the target ontology/taxonomy that best captures its meaning.

To create a new annotator, add a new subsection `[annotators.MyNewAnnotator]` where `MyNewAnnotator` should be a unique identifier for the annotator. This subsection can contain the following properties:

| Property | Description | Values
| --- | --- | --- |
| `type` | The type of the semantic annotatior. | {`taxonomy`, `ontology`, `map`} |
| `source` | If `type=taxonomy`, the identifier of the taxonomy used by the data source. | {`NCBI`, `GBIF`,`IF`}, optional |
| `targets` | If `type=taxonomy`, an ordered list containing the identifiers of the target taxonomies. | list, optional, default `["NCBI", "GBIF", "IF", "EOL", "OTT"]` |
| `filter_on_ranks` | If `type=taxonomy`, a list of taxa which will be used to restrict the search when trying to match taxa on the basis of their scientific names. | list, optional, example: `["Eukaryota", "Protista", "Protozoa"]`  |
| `multiple_match` | If `type=taxonomy`, how multiple matches are handled.  | {`strict`, `warning`, `ignore`}, optional, default `warning` |
| `shortname` | If `type=ontology`, the short name of the target ontology (see [Graph configuration](#ontologies)). | string, example `sfwo` |
| `mapping_file` | If `type=map`, the name of a YAML file containing label-IRI mappings. | path, example `mapping.yml` |

#### [extract]

#### [transform]

*Under construction.*

## Pipeline execution and monitoring

*Coming soon.*
