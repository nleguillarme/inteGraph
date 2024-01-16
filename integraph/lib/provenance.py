from ..util.path import ensure_path
from airflow.decorators import task


@task
def generate_prov_graph(data_graph_id, prov_graph_id, prov_metadata, output_filepath):
    from rdflib import ConjunctiveGraph, URIRef, Literal
    from rdflib.namespace import RDFS, OWL, DC

    output_filepath = ensure_path(output_filepath)
    output_filepath.parent.mkdir(parents=True, exist_ok=True)

    g = ConjunctiveGraph(identifier=prov_graph_id)

    subject = URIRef(data_graph_id)
    g.add((subject, DC.type, URIRef("http://purl.org/dc/dcmitype/Dataset")))
    if prov_metadata.get("contributor"):
        g.add((subject, DC.contributor, Literal(prov_metadata.get("contributor"))))
    if prov_metadata.get("coverage"):
        g.add((subject, DC.coverage, Literal(prov_metadata.get("coverage"))))
    if prov_metadata.get("creator"):
        g.add((subject, DC.creator, Literal(prov_metadata.get("creator"))))
    if prov_metadata.get("date"):
        g.add((subject, DC.date, Literal(prov_metadata.get("date"))))
    if prov_metadata.get("format"):
        g.add((subject, DC.format, Literal(prov_metadata.get("format"))))
    if prov_metadata.get("description"):
        g.add((subject, DC.description, Literal(prov_metadata.get("description"))))
    if prov_metadata.get("identifier"):
        g.add((subject, DC.identifier, Literal(prov_metadata.get("identifier"))))
    if prov_metadata.get("language"):
        g.add((subject, DC.language, Literal(prov_metadata.get("language"))))
    if prov_metadata.get("publisher"):
        g.add((subject, DC.publisher, Literal(prov_metadata.get("publisher"))))
    if prov_metadata.get("relation"):
        g.add((subject, DC.relation, Literal(prov_metadata.get("relation"))))
    if prov_metadata.get("rights"):
        g.add((subject, DC.rights, Literal(prov_metadata.get("rights"))))
    if prov_metadata.get("source"):
        g.add((subject, DC.source, Literal(prov_metadata.get("source"))))
    if prov_metadata.get("subject"):
        g.add((subject, DC.subject, Literal(prov_metadata.get("subject"))))
    if prov_metadata.get("title"):
        g.add((subject, DC.title, Literal(prov_metadata.get("title"))))
    if prov_metadata.get("type"):
        g.add((subject, DC.type, Literal(prov_metadata.get("type"))))

    g.serialize(destination=output_filepath, format="nquads")
    return str(output_filepath)
