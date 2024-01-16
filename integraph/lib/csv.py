from ..util.csv import get_csv_file_reader, write, read
from .annotator import AnnotatorFactory, get_annotator_config
import logging
import os
import pandas as pd
import numpy as np
from pathlib import Path
from airflow.decorators import task
from ..util.path import ensure_path


@task
def to_ets(filepath, ets_config, delimiter, output_dir):
    """Turn tabular data to ETS format

    Parameters
    ----------
    filepath : _type_
        _description_
    ets_config : _type_
        _description_
    delimiter : _type_
        _description_
    output_dir : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """

    # Read input data
    index_col = ets_config.get("index_col", None)
    df = read(filepath, sep=delimiter, index_col=index_col)

    # Unpivot data frame from wide to long format
    taxon_col = ets_config.get("taxon_col", None)
    id_vars = [taxon_col] if taxon_col else []
    id_vars += ets_config.get("additional_cols", [])
    value_vars = ets_config.get("measurement_cols")

    if id_vars and value_vars:
        df = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name="verbatimTraitName",
            value_name="verbatimTraitValue",
            ignore_index=False,
        )
        # df = df.dropna(subset="traitValue")
        if ets_config.get("na", None):
            df["verbatimTraitValue"] = (
                df["verbatimTraitValue"]
                .astype(str)
                .replace(str(ets_config.get("na")), np.nan)
            )
        df = df.dropna(subset="verbatimTraitValue").rename(
            columns={taxon_col: "verbatimScientificName"}
        )

    # Create unit column
    vars_to_units = ets_config.get("units", None)
    if vars_to_units:
        for var in value_vars:
            vars_to_units[var] = vars_to_units.get(var)
        df["verbatimTraitUnit"] = df["verbatimTraitName"].replace(vars_to_units)
        df = df[
            ~(
                (df["verbatimTraitUnit"] == "binary")
                & (df["verbatimTraitValue"].astype(str).isin(["0", "0.0"]))
            )
        ]

    index_col = index_col if index_col else "index"
    df = df.reset_index().rename(columns={index_col: "occurrenceID"})
    df = df.reset_index().rename(columns={"index": "measurementID"})

    output_dir = ensure_path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ets_filepath = output_dir / Path(filepath).name
    write(df, ets_filepath, sep=delimiter, index=True)
    return str(ets_filepath)


@task
def old_to_ets(filepath, ets_config, delimiter, output_dir):
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


def annotate_entity(filepath, src_id, entity, entity_cfg, output_dir):
    output_dir = ensure_path(output_dir)

    prev_task = filepath
    first_annotator = True

    taxonomy_annotator = None
    for annotator in entity_cfg.get("annotators"):
        if get_annotator_config(src_id, annotator)["type"] == "taxonomy":
            taxonomy_annotator = annotator
        prev_task = (
            task(task_id=annotator)(annotate)
            .partial(
                annotator=AnnotatorFactory.get_annotator(src_id, annotator),
                id_col=entity_cfg.get("id"),
                label_col=entity_cfg.get("label"),
                iri_col=entity + "ID",
                output_dir=output_dir / entity / annotator,
                replace=first_annotator,
            )
            .expand(filepath=prev_task)
        )
        first_annotator = False

    # Concat chunks for the current entity
    ann_filepath = task(task_id=f"concat_annotations")(concat)(
        filepaths=prev_task,
        output_filepath=output_dir / entity / "all.tsv",
        drop=False,
        index_col=0,
    )

    map_filepath = None
    if taxonomy_annotator:
        mapped_chunks = map.partial(
            annotator=AnnotatorFactory.get_annotator(src_id, taxonomy_annotator),
            output_dir=output_dir / entity / annotator,
        ).expand(filepath=prev_task)
        map_filepath = task(task_id="concat_mappings")(concat)(
            filepaths=mapped_chunks,
            output_filepath=output_dir / entity / "mapped_all.tsv",
            drop=True,
            subset=["matchId"],
            index_col=0,
        )

    return {"ann_filepath": ann_filepath, "map_filepath": map_filepath}


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


def annotate(filepath, annotator, id_col, label_col, iri_col, replace, output_dir):
    df = read(filepath, index_col=0)
    ann_df = annotator.annotate(df, id_col, label_col, iri_col, replace)
    ann_df[f"integraph_{iri_col}"] = df.index
    mask = ann_df[iri_col].isna()
    ann_df[f"integraph_{iri_col}"] = ann_df[f"integraph_{iri_col}"].mask(mask, "")
    output_dir.mkdir(parents=True, exist_ok=True)
    ann_filepath = output_dir / Path(filepath).name
    write(ann_df, ann_filepath, index=True)
    return str(ann_filepath)


@task
def map(filepath, annotator, output_dir):
    df = read(filepath, index_col=0)
    mapped = annotator.map(df)
    output_dir.mkdir(parents=True, exist_ok=True)
    map_filepath = output_dir / Path("mapped_" + Path(filepath).name)
    write(mapped, map_filepath, index=True)
    return str(map_filepath)


def concat(
    filepaths,
    output_filepath,
    drop=False,
    subset=None,
    index_col=None,
    join="outer",
    axis=0,
):
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    frames = [read(f, index_col=index_col) for f in filepaths if f]
    merged = pd.concat(frames, join=join, axis=axis)
    merged = merged.drop(columns=["integraph.id", "integraph.label"], errors="ignore")
    if drop:
        merged = merged.drop_duplicates(subset=subset)
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
