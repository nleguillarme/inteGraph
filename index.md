## Welcome to the homepage of inteGraph

This website is under active development ! If you do not find the information you are looking for, please [contact us](#contact-us) or come back later.

### What is inteGraph?

[![Image providing a high-level overview of inteGraph.](/images/integraph-overview.png)](https://raw.githubusercontent.com/nleguillarme/inteGraph/gh-pages/images/integraph-overview.png)

`inteGraph` is a biodiversity data semantification and integration toolbox designed to help ecologists make the most of available data, regardless of the initial format and location of these data. 

`inteGraph` implements a declarative approach to building Extract-Transform-Load (ETL) pipelines that transform your structured data into RDF, thus minimizing the amount of manual effort and Semantic Web expertise required to publish your data as or integrate heterogenenous data into FAIR biodiversity knowledge graphs.

#### Features

- An (almost) code-free approach to building RDF knowledge graphs
- Fetch data from local/remote files or web-based APIs
- Automatic matching of taxon names and identifiers to target taxonomies using [nomer](https://github.com/globalbioticinteractions/nomer)
- Link your data to controlled terms in ontologies using [text2term](https://github.com/ccb-hms/ontology-mapper)
- User-friendly mappings with [Mapeathor](https://github.com/oeg-upm/mapeathor)
- Scalable knowledge graph materialization using [Morph-KGC](https://morph-kgc.readthedocs.io/en/latest/)
- Supports loading data to GraphDB, RDFox
- Dockerized for easy deployment across different environments
- Schedule and monitor pipelines execution using [Apache Airflow](https://airflow.apache.org/)

### User manual

*Coming soon.*

### Cite inteGraph

If you used inteGraph in your work, please cite our EJSB paper:

Le Guillarme, N., & Thuiller, W. (2023). [A practical approach to constructing a knowledge graph for soil ecological research](https://www.sciencedirect.com/science/article/abs/pii/S116455632300033X). European Journal of Soil Biology, 117, 103497.

### Contact us

Please use the [GitHub repository's Issue tracker](https://github.com/nleguillarme/integraph/issues) to report errors or specific concerns related to the use of inteGraph.

For all other enquiries, please contact Nicolas Le Guillarme: [nicolas.leguillarme(at)univ-grenoble-alpes(dot)fr](nicolas.leguillarme@univ-grenoble-alpes.fr)
