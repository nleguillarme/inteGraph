# Tutorial

[<<](index.md) Back to homepage

*Under construction.*

## Building a knowledge graph on the trophic ecology of soil organisms using **inteGraph**

In this tutorial, we will provide a step-by-step guide on how to harness the capabilities of **inteGraph** to create a knowledge graph on the trophic ecology of carabid beetles (Carabidae) from multiple data sources. This knowledge graph will integrate data from two sources:
- The Biological and Ecological Traits of Soil Invertebrates database (BETSI, https://portail.betsi.cnrs.fr/)
- The Global Biotic Interactions database (GloBI, https://www.globalbioticinteractions.org/)

### 0. Set up a triplestore instance and create a repository (optional) 

Storing RDF knowledge graphs in simple files using one of the many available RDF serialisation formats (e.g. Turtle, TriG, N-Triples, N-Quads, JSON-LD) can be practical for smaller projects or scenarios where simplicity and portability are prioritised. However, it may not be suitable for large or highly interconnected knowledge graphs that require efficient querying and traversal. In such cases, the use of specialised RDF graph databases, also called triplestores, may be more appropriate.

While **inteGraph** is not tied to a specific triplestore solution, it does provide connectors to help you load your RDF data into [GraphDB](https://graphdb.ontotext.com/documentation/10.6/) and [RDFox](https://docs.oxfordsemantic.tech/) repositories.

In this section, we will show you how to set up an instance of GraphDB on your machine, create a repository for your trophic knowledge graph, and load the knowledge graph ontology into the repository.

#### Set up a GraphDB instance

#### Create a new repository

#### Load the knowledge graph ontology

### 1. Create the graph configuration file

The graph configuration file is **inteGraph**'s entry point for creating semantic data integration pipelines. The construction of our trophic knowledge graph begins with the creation of the graph configuration file.

```bash
# Create the project's root directory
mkdir trophic-kg

# Create a new graph configuration file in the root directory
cd trophic-kg
touch graph.cfg
```

The graph configuration file contains [up to four sections](https://nleguillarme.github.io/inteGraph/manual.html#create-a-new-project). Copy the following lines into the `graph.cfg` file we have just created:

```ini
[graph]
id=https://nleguillarme.github.io/inteGraph/tutorial/trophic-kg

[sources]
dir=sources

[load]
id=graphdb
conn_type=http
host=127.0.0.1
port=7200
repository=trophic-kg

[ontologies]
sfwo="http://purl.org/sfwo/sfwo.owl"
```

### 2. Set up connections to remote databases (optional)

**inteGraph** allows you to extract data from online data sources via their APIs. These APIs may require you to provide connection information, including authentication credentials (such as API keys, tokens, or OAuth credentials) and endpoint URLs. To simplify connection management, **inteGraph** allows you to store connection information in a single JSON file `connections.json`.

```bash
# Create a new file connections.json in the root directory
touch connections.json
```

In this tutorial we want to integrate data on trophic interactions of carabid beetles into our trophic knowledge graph. This interaction data will be extracted from GloBI using the [GloBI Web API](https://github.com/globalbioticinteractions/globalbioticinteractions/wiki/API). Copy the following lines into the `connections.json` file we have just created:

```json
{
  "globi": {
    "conn_type": "http",
    "description": null,
    "host": "https://api.globalbioticinteractions.org",
    "port": null,
    "schema": "https",
    "extra": null
  }
}
```

At runtime, **inteGraph** parses the `connections.json` file and creates as many [Airflow's Connection objects](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) as needed.

### 3. Create the source configuration file for BETSI

We will now create a configuration file for the first data source, the BETSI database.

```bash
# Create a new directory for the first data source in the sources directory
mkdir -p sources/betsi

# Create the source configuration file
cd sources/betsi
touch source.cfg
```

BETSI does not provide programmatic access to its data. Carabid diet data can be downloaded manually from the BETSI web portal. For the purposes of this tutorial, we also provide a download link (with the permission of the data provider).

```bash
# Create a new directory for storing the data file
mkdir data

# Download the data file
wget <link> -P ./data
```

#### 3.1 Specify metadata for the source

The first section of the source configuration file contains the data source identifier and source metadata. Although the latter is optional, it is recommended that you add source metadata to your knowledge graph to keep track of the provenance of information. Copy the following lines into the `source.cfg` file we have just created:

```ini
[source]
id=betsi

[source.metadata]
title=A database for soil invertebrate biological and ecological traits
creator=Hedde et al
subject=araneae, carabidae, chilopoda, diplopoda, earthworms, isopoda
description=The Biological and Ecological Traits of Soil Invertebrates database (BETSI, https://portail.betsi.cnrs.fr/) is a European database dedicated specifically to soil organisms’ traits.
date=2021
format=csv
identifier=hal-03581637
language=en
```

**inteGraph** will append the data source identifier to the graph base IRI specified in the graph configuration file to create the full IRI of the data source, here `https://nleguillarme.github.io/inteGraph/tutorial/trophic-kg/betsi`.

### 4. Create the mapping spreadsheet for BETSI

### 5. Create the source configuration file for GloBi

### 6. Create the mapping spreadsheet for GloBi

### Execute the data integration pipelines

### Retrieve information from the knowledge graph
