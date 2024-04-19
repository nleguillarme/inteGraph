# Tutorial

[<<](index.md) Back to homepage

*Under construction.*

## Building a knowledge graph on the trophic ecology of soil organisms using **inteGraph**

In this tutorial, we will provide a step-by-step guide on how to harness the capabilities of **inteGraph** to create a knowledge graph on the trophic ecology of carabid beetles (Carabidae) from multiple data sources. This knowledge graph will integrate data from two sources:
- The Biological and Ecological Traits of Soil Invertebrates database (BETSI, [https://portail.betsi.cnrs.fr/](https://portail.betsi.cnrs.fr/))
- The Global Biotic Interactions database (GloBI, [https://www.globalbioticinteractions.org/](https://www.globalbioticinteractions.org/))

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

Let's take a closer look at the contents of the BETSI database:

| taxon_name                                                 | trait_name | attribute_trait | source_fauna      |
|------------------------------------------------------------|------------|-----------------|-------------------|
| Abacetus (Astigis)                                         | Diet       | Zoophage        | Larochelle (1990) |
| Abax (Abax) ovalis (Duftschmid, 1812)                      | Diet       | Zoophage        | Larochelle (1990) |
| Abax (Abax) ovalis (Duftschmid, 1812)                      | Diet       | Necrophagous    | Larochelle (1990) |
| Abax (Abax) parallelepipedus (Piller & Mitterpacher, 1783) | Diet       | Zoophage        | Kromp, B. (1999)  |

#### 3.1 Specify metadata for the source

The first section of the source configuration file contains the data source identifier and source metadata. Although the latter is optional, it is recommended that you add source metadata to your knowledge graph to keep track of the provenance of information. Copy the following lines into the `source.cfg` file we have just created:

```ini
[source]
id=betsi

[source.metadata]
title=A database for soil invertebrate biological and ecological traits
creator=Hedde et al
subject=carabidae
description=The Biological and Ecological Traits of Soil Invertebrates database (BETSI, https://portail.betsi.cnrs.fr/) is a European database dedicated specifically to soil organisms’ traits.
date=2021
format=csv
identifier=hal-03581637
language=en
```

**inteGraph** will append the data source identifier to the graph base IRI specified in the graph configuration file to create the full IRI of the data source, here `https://nleguillarme.github.io/inteGraph/tutorial/trophic-kg/betsi`.

#### 3.2 Define semantic annotators

The role of a semantic annotator is to match a piece of data with the concept in the target ontology/taxonomy that best captures its meaning. **inteGraph** allows you to use three types of semantic annotators as part of your data integration pipelines:
- **Taxonomic annotator:** map taxon names to entities in a target taxonomy, and/or map taxon identifiers from a source taxonomy to a target taxonomy ;
- **Ontology-based annotator:** map free-text descriptions of entities to controlled terms in a target ontology ;
- **Dictionary-based annotator:** map entities to controlled terms in a target ontology/taxonomy using a user-supplied YAML mapping file.

All three types of semantic annotators are needed to add semantic annotations to BETSI data:
- A taxonomic annotator to map the names in the `taxon_name` column to taxonomic entities in the target taxonomy (NCBI) ;
- An ontology-based annotator to map the dietary terms in the `attribute_trait` column to concepts in the target ontology (SFWO) ;
- A dictionary-based annotator to map the remaining dietary terms that do not have an exact match in the SFWO.

Annotators are defined in the `[annotators]` section of the source configuration file. Copy the following lines at the end of `source.cfg`:

```ini
[annotators]

[annotators.TaxonAnnotator]
type=taxonomy
filter_on_ranks=["Carabidae"]
targets=["NCBI"]

[annotators.SFWO]
type=ontology
shortname=sfwo

[annotators.YAMLMap]
type=map
mapping_file=mapping.yml
```

**N.B. (1)** the `shortname` property of an ontology-based annotator can only take the name of one of the ontologies specified in the `[ontologies]` section of the graph configuration file.

**N.B. (2)** a dictionary-based annotator expects a YAML file containing label-IRI mappings. This mapping file should be in the same directory as the source configuration file. Create a file called `mapping.yml` in the source directory, and copy the following lines into this file:

```yaml
Necrophagous: http://purl.obolibrary.org/obo/ECOCORE_00000090
Granivorous: http://purl.obolibrary.org/obo/ECOCORE_00000143
Fructivore_carpophagous: http://purl.obolibrary.org/obo/ECOCORE_00000136
Phloem_sap_feeders: http://purl.org/sfwo/SFWO_0000021
Pollen_feeders: http://purl.obolibrary.org/obo/ECOCORE_00000137
Rhizophagous: http://purl.obolibrary.org/obo/ECOCORE_00000085
Zoophage: http://purl.obolibrary.org/obo/ECOCORE_00000088
```

### 4. Create the mapping spreadsheet for BETSI

### 5. Create the source configuration file for GloBi

### 6. Create the mapping spreadsheet for GloBi

### Execute the data integration pipelines

### Retrieve information from the knowledge graph
