# Tutorial

[<<](index.md) Back to homepage

*Under construction.*

## Building a knowledge graph on the trophic ecology of soil organisms using **inteGraph**

In this tutorial, we will provide a step-by-step guide on how to harness the capabilities of **inteGraph** to create a knowledge graph on the trophic ecology of soil organisms from multiple data sources.

### 0. Set up a triplestore instance and create a repository (optional) 

Storing RDF knowledge graphs in simple files using one of the many available RDF serialisation formats (e.g. Turtle, TriG, N-Triples, N-Quads, JSON-LD) can be practical for smaller projects or scenarios where simplicity and portability are prioritised. However, it may not be suitable for large or highly interconnected knowledge graphs that require efficient querying and traversal. In such cases, the use of specialised RDF graph databases, also called triplestores, may be more appropriate.

While **inteGraph** is not tied to a specific triplestore solution, it does provide connectors to help you load your RDF data into [GraphDB](https://graphdb.ontotext.com/documentation/10.6/) and [RDFox](https://docs.oxfordsemantic.tech/) repositories.

In this section, we will show you how to set up an instance of GraphDB on your machine, create a repository for your trophic knowledge graph, and load the knowledge graph ontology into the repository.

#### Set up a GraphDB instance

#### Create a new repository

#### Load the knowledge graph ontology

### 1. Create the graph configuration file

The graph configuration file is **inteGraph**'s entry point for creating semantic data integration pipelines. The construction of our trophic knowledge graph begins with the creation of the graph configuration file.

```
# Create a new project directory
mkdir trophic-kg

# Create a new graph configuration file in the project's directory
cd trophic-kg
touch graph.cfg
```

The graph configuration file contains [up to four sections](https://nleguillarme.github.io/inteGraph/manual.html#create-a-new-project). Copy the following lines into the `graph.cfg` file we have just created:

```
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

### Set up connections to remote databases

### Create the graph configuration file

### Create the source configuration file for FungalTraits

### Create the mapping spreadsheet for FungalTraits

### Create the source configuration file for GloBi

### Create the mapping spreadsheet for GloBi

### Execute the data integration pipelines

### Retrieve information from the knowledge graph
