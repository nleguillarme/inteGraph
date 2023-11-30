from ..util.csv import get_csv_file_reader, write, read
from .annotator import AnnotatorFactory
from .annotator import TaxonomyAnnotator
import logging
import os
import pandas as pd
import numpy as np
from pathlib import Path
from airflow.decorators import task
from ..util.path import ensure_path


@task
def to_ets(filepath, ets_config, delimiter, output_dir):
    id_vars = ets_config.get("id_vars")
    value_vars = ets_config.get("value_vars")
    output_dir = ensure_path(output_dir)
    index_col = ets_config.get("index_col")
    df = read(filepath, sep=delimiter, index_col=index_col)

    if id_vars and value_vars:
        df = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name="traitName",
            value_name="traitValue",
            ignore_index=False,
        )

        df = df.dropna(subset="traitValue")
        if ets_config.get("na") is not None:
            df["traitValue"] = (
                df["traitValue"].astype(str).replace(str(ets_config.get("na")), np.nan)
            )
        df = df.dropna(subset="traitValue")

    vars_to_units = ets_config.get("units")
    if vars_to_units:
        for var in value_vars:
            vars_to_units[var] = vars_to_units.get(var)
        df["traitUnit"] = df["traitName"].replace(vars_to_units)
        df = df[
            ~(
                (df["traitUnit"] == "binary")
                & (df["traitValue"].astype(str).isin(["0", "0.0"]))
            )
        ]

    index_col = index_col if index_col else "index"
    df = df.reset_index().rename(columns={index_col: "occurrenceID"})
    df = df.reset_index().rename(columns={"index": "dataID"})
    output_dir.mkdir(parents=True, exist_ok=True)
    ets_filepath = output_dir / Path(filepath).name
    write(df, ets_filepath, sep=delimiter, index=True)
    return str(ets_filepath)


def annotate_entity(filepath, root_dir, entity, entity_cfg, output_dir):
    root_dir = ensure_path(root_dir)
    output_dir = ensure_path(output_dir)
    prev_task = filepath
    first_annotator = True
    for target in entity_cfg.get("target"):
        task_id = target
        if ensure_path(target).suffix == ".yml":
            target = root_dir / target
            task_id = target.name.replace(".", "_")
        prev_task = (
            task(task_id=task_id)(annotate)
            .partial(
                id_col=entity_cfg.get("id"),
                label_col=entity_cfg.get("label"),
                iri_col=entity + "_iri",
                source=entity_cfg.get("source"),
                target=target,
                output_dir=output_dir / entity / task_id,
                replace=first_annotator,
            )
            .expand(filepath=prev_task)
        )
        first_annotator = False
    # Concat chunks for the current entity
    ann_filepath = task(task_id=f"concat")(concat)(
        filepaths=prev_task,
        output_filepath=output_dir / entity / "all.tsv",
        drop=False,
        index_col=0,
    )
    return {"filepath": ann_filepath, "task": prev_task}


def map_taxo_entity(filepath, entity, output_dir):
    mapped_chunks = map.partial(output_dir=output_dir / entity).expand(
        filepath=filepath
    )
    return task(task_id="concat")(concat)(
        filepaths=mapped_chunks,
        output_filepath=output_dir / entity / "all.tsv",
        drop=False,
        index_col=0,
    )


@task
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


@task
def map(filepath, output_dir):
    from ..util.nomer import TAXONOMIES

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
    from ..util.nomer import TAXONOMIES

    output_filepath = ensure_path(output_filepath)
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

    output_filepath = ensure_path(output_filepath)
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    g = ConjunctiveGraph(identifier=graph_id)
    for f in filepaths:
        g += Graph().parse(f, format="nquads")
    g.serialize(destination=output_filepath, format="nquads")
    return str(output_filepath)
