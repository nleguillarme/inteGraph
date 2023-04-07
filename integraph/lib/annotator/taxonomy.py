from .base import Annotator
from ...util.nomer import NomerHelper
from ...util.gnparser import normalize_names
import pandas as pd
import numpy as np
import logging


class UnsupportedTaxonomyException(Exception):
    pass


class NoValidColumnException(Exception):
    pass


class ConfigurationError(Exception):
    pass


class TaxonomyAnnotator(Annotator):
    def __init__(self):
        self.nomer = NomerHelper()
        self.matchers = {
            "gbif": "gbif",
            "ncbi": "ncbi",
            "ifungorum": "indexfungorum",
            "silva": "ncbi",
            "eol": "eol",
        }
        self.name_matcher = "globalnames"
        self.keep_only = ["NCBI", "GBIF", "IF"]

    def annotate(
        self, df, id_col, label_col, iri_col, source=None, target="ncbi", replace=False
    ):
        # Validate against source taxonomy
        valid_df = self.validate(df, id_col, label_col, iri_col, source, replace)
        return valid_df

    def map(
        self, df, id_col="integraph.id", label_col="integraph.label", target="ncbi"
    ):
        subset = [col for col in [id_col, label_col] if col]
        drop_df = (
            df.dropna(subset=id_col).drop_duplicates(subset=subset).copy()
        ).replace("", np.nan)
        matches = None
        matchers = ["wikidata-web", target]
        not_found_df = drop_df
        for matcher in matchers:
            if not not_found_df.empty:
                new_matches = self.map_taxonomic_entities(not_found_df, matcher=matcher)
                if not new_matches.empty:
                    new_matches = new_matches[
                        new_matches["matchId"].str.startswith(tuple(self.keep_only))
                    ]
                    matches = (
                        pd.concat([matches, new_matches], ignore_index=True)
                        if matches is not None
                        else new_matches
                    )
                    mapped_ents = matches.groupby("queryId", dropna=False)
                    not_found_in_target = []
                    for name, group in mapped_ents:
                        if group[group["matchId"].str.startswith(target.upper())].empty:
                            not_found_in_target.append(name)
                    not_found_df = not_found_df[
                        not_found_df[id_col].isin(not_found_in_target)
                    ]
        return matches, not_found_df

    def validate(self, df, id_col, label_col, iri_col, source, replace):
        def add_prefix(col, source):
            """
            Add the source taxonomy name as a prefix to all taxids in a column
            """

            def return_prefixed(id, source):
                if pd.notnull(id):
                    items = str(id).strip("\n").split(":")
                    if len(items) == 2:
                        if pd.isna(pd.to_numeric(items[-1], errors="coerce")):
                            return np.nan
                        else:
                            return str(id)
                    elif pd.isna(pd.to_numeric(id, errors="coerce")):
                        return np.nan
                    else:
                        return f"{source.upper()}:{id}"
                return None

            return col.map(lambda id: return_prefixed(id, source))

        assert id_col or label_col
        subset = [col for col in [id_col, label_col] if col]
        sub_df = df[subset].astype(pd.StringDtype(), errors="ignore")
        sub_df[iri_col] = np.nan if replace else df[iri_col]
        sub_df["integraph.id"] = np.nan if replace else df["integraph.id"]
        sub_df["integraph.label"] = np.nan if replace else df["integraph.label"]

        drop_df = sub_df[sub_df[iri_col].isna()]
        drop_df = sub_df.drop_duplicates(subset=subset).replace("", np.nan)

        if id_col:
            if source:
                if source in self.matchers:
                    drop_df["integraph.id"] = add_prefix(drop_df[id_col], source)
                else:
                    raise UnsupportedTaxonomyException(
                        f"Invalid taxonomy {source}. Possible values are {list(self.matchers.keys())}"
                    )
        if label_col:
            drop_df["integraph.label"] = drop_df[label_col]
            names = drop_df[label_col].dropna().to_list()
            names = normalize_names(names)
            drop_df = drop_df.replace({"integraph.label": names})

        matcher = self.matchers[source] if (id_col and source) else self.name_matcher
        valid_ents = self.map_taxonomic_entities(drop_df, matcher)

        by = ["queryId"] if id_col else [] + ["queryName"] if label_col else []
        by = (
            by[-1] if len(by) == 1 else by
        )  # To remove pandas warning for lists of length 1
        valid_ents = valid_ents.groupby(by, dropna=False)

        iri_map = {}
        id_map = {}
        label_map = {}

        for index, row in drop_df.iterrows():
            query_id = row["integraph.id"]
            query_name = row["integraph.label"]
            iri_index = ((row[id_col],) if id_col else ()) + (
                (row[label_col],) if label_col else ()
            )
            try:
                group_name = (
                    [query_id] if id_col else [] + [query_name] if label_col else []
                )
                group_name = group_name[-1] if len(group_name) == 1 else group_name
                valid = valid_ents.get_group(group_name).iloc[0]
            except KeyError:
                iri_map[iri_index] = np.nan
                id_map[iri_index] = np.nan
                label_map[iri_index] = np.nan
            else:
                iri_map[iri_index] = valid["iri"]
                id_map[iri_index] = valid["matchId"]
                label_map[iri_index] = valid["matchName"]

        for index, row in sub_df.iterrows():
            if pd.isna(row[iri_col]):
                iri_index = ((row[id_col],) if id_col else ()) + (
                    (row[label_col],) if label_col else ()
                )
                sub_df.at[index, iri_col] = iri_map[iri_index]
                sub_df.at[index, "integraph.id"] = id_map[iri_index]
                sub_df.at[index, "integraph.label"] = label_map[iri_index]
        return sub_df

    def map_taxonomic_entities(self, df, matcher):
        """
        Validate all taxonomic identifiers in a DataFrame column against a given taxonomy
        """
        query = self.nomer.df_to_query(
            df=df,
            id_column="integraph.id",
            name_column="integraph.label",
        )
        if query:
            matching = self.nomer.ask_nomer(query, matcher=matcher)
            if not matching.empty:
                mask = matching["matchType"].isin(
                    ["SAME_AS", "SYNONYM_OF", "HAS_ACCEPTED_NAME"]
                )
                return matching[mask]
        return pd.DataFrame()
