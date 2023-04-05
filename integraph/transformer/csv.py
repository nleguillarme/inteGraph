from ..util.csv_helper import get_csv_file_reader, write, read
from ..lib.annotators import AnnotatorFactory
from ..lib.taxo_annotator import TaxonomyAnnotator
import logging
import os
import pandas as pd
from pathlib import Path


def to_tsv(filepath, sep, output_dir):
    df = read(filepath, sep=sep)
    output_filepath = output_dir / (str(Path(filepath).stem) + ".tsv")
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    write(df, output_filepath, sep="\t")
    return str(output_filepath)


def split(filepath, chunksize, delimiter, output_dir):
    if filepath:
        reader = get_csv_file_reader(
            csv_file=filepath,
            columns=None,
            dtype=str,
            delimiter=delimiter,
            chunksize=chunksize,
        )
        chunk_count = 0
        chunks = []
        output_dir.mkdir(parents=True, exist_ok=True)
        for df in reader:
            chunk_filepath = os.path.join(output_dir, f"{chunk_count}.tsv")
            write(df, chunk_filepath, sep="\t", index=True)
            chunks.append(chunk_filepath)
            chunk_count += 1
        return chunks
    else:
        raise ValueError(filepath)


def annotate(filepath, id_col, label_col, iri_col, source, target, replace, output_dir):
    annotator = AnnotatorFactory.get_annotator(label=target)
    df = read(filepath, index_col=0)
    ann_df = annotator.annotate(df, id_col, label_col, iri_col, source, target, replace)
    output_dir.mkdir(parents=True, exist_ok=True)
    ann_filepath = output_dir / Path(filepath).name
    write(ann_df, ann_filepath, index=True)
    return str(ann_filepath)


def map(filepath, output_dir):
    from ..util.taxo_helper import TAXONOMIES

    annotator = TaxonomyAnnotator()
    df = read(filepath, index_col=0)
    mapped, _ = annotator.map(df)
    df_ncbitaxon = mapped[mapped["matchId"].str.startswith("NCBI:")]
    df_ncbitaxon["matchId"] = df_ncbitaxon["matchId"].apply(
        lambda x: x.replace("NCBI:", "NCBITaxon:")
    )
    df_ncbitaxon["iri"] = df_ncbitaxon["iri"].apply(
        lambda x: x.replace(
            TAXONOMIES["NCBI:"]["url_prefix"], TAXONOMIES["NCBITaxon:"]["url_prefix"]
        )
    )
    mapped = pd.concat([mapped, df_ncbitaxon])
    mapped = mapped[mapped["queryId"] != mapped["matchId"]]
    output_dir.mkdir(parents=True, exist_ok=True)
    map_filepath = output_dir / Path(filepath).name
    write(mapped, map_filepath, index=True)
    return str(map_filepath)


def concat(
    filepaths, output_filepath, drop=False, index_col=None, join="outer", axis=0
):
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    frames = [read(f, index_col=index_col) for f in filepaths]
    for df in frames:
        print(df.shape)
    merged = pd.concat(frames, join=join, axis=axis)
    merged = merged.drop(columns=["integraph.id", "integraph.label"], errors="ignore")
    if drop:
        merged = merged.drop_duplicates()
    merged = merged.iloc[:, ~merged.columns.duplicated()]
    write(merged, output_filepath, index=True)
    return str(output_filepath)


def triplify(filepath, output_filepath):
    from rdflib import Graph, URIRef, Literal
    from rdflib.namespace import RDFS, OWL
    from ..util.taxo_helper import TAXONOMIES

    output_filepath.parent.mkdir(parents=True, exist_ok=True)

    df = read(filepath, index_col=0)
    g = Graph()
    for _, row in df.iterrows():
        prefix = row["queryId"].split(":")[0] + ":"
        full_iri = TAXONOMIES[prefix]["url_prefix"]
        queryIRI = row["queryId"].replace(prefix, full_iri)

        g.add((URIRef(queryIRI), OWL.equivalentClass, URIRef(row["iri"])))
        g.add((URIRef(queryIRI), RDFS.label, Literal(row["queryName"])))
        g.add((URIRef(row["iri"]), RDFS.label, Literal(row["matchName"])))

        if pd.notna(row["matchRank"]):
            g.add(
                (
                    URIRef(row["iri"]),
                    URIRef("http://purl.obolibrary.org/obo/ncbitaxon#has_rank"),
                    Literal(row["matchRank"]),
                )
            )

    g.serialize(destination=output_filepath, format="nt")
    return str(output_filepath)


def merge(filepaths, graph_id, output_filepath):
    from rdflib import ConjunctiveGraph, Graph

    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    g = ConjunctiveGraph(identifier=graph_id)
    for f in filepaths:
        g += Graph().parse(f, format="nquads")
    g.serialize(destination=output_filepath, format="nquads")
    return str(output_filepath)
