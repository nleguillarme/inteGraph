from .base import Annotator
from ...util.nomer import NomerHelper, TAXONOMIES
from ...util.gnparser import normalize_names
import pandas as pd
import numpy as np
import logging


class UnsupportedTaxonomyException(Exception):
    pass


class MultipleMatch(Exception):
    pass


class TaxonomyAnnotator(Annotator):
    def __init__(self, config):
        self.nomer = NomerHelper()
        self.matchers = {
            "gbif": "gbif",
            "ncbi": "ncbi",
            "ifungorum": "indexfungorum",
            "silva": "globalnames",
        }
        logging.info(config)
        self.source = config.get("source", "default")
        self.name_matcher = config.get("name_matcher", "ncbi")
        self.kingdom = config.get("kingdom", None)
        self.keep_only = config.get("targets", ["NCBI", "GBIF", "IF", "EOL"])

    def annotate(
        self,
        df,
        id_col,
        label_col,
        iri_col,
        replace=False,
    ):
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

        """
            Return a tuple containing the taxon identifier and/or name if available
        """

        def build_taxon_index(row, id_col, label_col):
            return ((row[id_col],) if id_col else ()) + (
                (row[label_col],) if label_col else ()
            )

        def build_mapping_dict(queried, mapped, id_col, label_col):
            mapping = {}
            for _, row in queried.iterrows():
                query_id = row["integraph.id"]
                query_name = row["integraph.label"]
                iri_index = build_taxon_index(row, id_col, label_col)
                try:
                    group_name = (
                        [query_id] if id_col else [] + [query_name] if label_col else []
                    )
                    group_name = group_name[-1] if len(group_name) == 1 else group_name
                    if mapped.get_group(group_name).shape[0] > 1:
                        logging.info(
                            f"Multiple matches for taxon {iri_index} : {mapped.get_group(group_name)}"
                        )
                        raise MultipleMatch
                    else:
                        match = mapped.get_group(group_name).iloc[0]
                except (KeyError, MultipleMatch):
                    mapping[iri_index] = {"iri": np.nan, "id": np.nan, "label": np.nan}
                else:
                    mapping[iri_index] = {
                        "iri": match["iri"],
                        "id": match["matchId"],
                        "label": match["matchName"],
                    }
            return mapping

        assert id_col or label_col
        subset = [col for col in [id_col, label_col] if col]
        df = df[subset].astype(pd.StringDtype(), errors="ignore")
        if replace:
            df.loc[:, [iri_col, "integraph.id", "integraph.label"]] = np.nan

        taxa_to_annotate = (
            df[df[iri_col].isna()].drop_duplicates(subset=subset).replace("", np.nan)
        )

        if id_col:
            if self.source:
                if self.source in self.matchers:
                    taxa_to_annotate["integraph.id"] = add_prefix(
                        taxa_to_annotate[id_col], self.source
                    )
                else:
                    raise UnsupportedTaxonomyException(
                        f"Invalid taxonomy {self.source}. Possible values are {list(self.matchers.keys())}"
                    )
        if label_col:
            # Normalize scientific names using Global Names Parser
            taxa_to_annotate["integraph.label"] = taxa_to_annotate[label_col]
            names = taxa_to_annotate[label_col].dropna().to_list()
            names = normalize_names(names)
            taxa_to_annotate = taxa_to_annotate.replace({"integraph.label": names})

        # matcher = (
        #     self.matchers[self.source]
        #     if (id_col and self.source)
        #     else self.name_matcher
        # )
        matcher = self.matchers.get(self.source, self.name_matcher)
        logging.info(f"Received source {self.source}, use matcher {matcher}")
        annotated = self.map_taxonomic_entities(
            taxa_to_annotate, matcher=matcher, kingdom=self.kingdom
        )

        if not annotated.empty:
            annotated = annotated[
                annotated["matchId"].str.startswith(tuple(TAXONOMIES.keys()))
            ]

        by = ["queryId"] if id_col else [] + ["queryName"] if label_col else []
        by = (
            by[-1] if len(by) == 1 else by
        )  # To remove pandas warning for lists of length 1
        annotated = annotated.groupby(by, dropna=False)

        mapping = build_mapping_dict(taxa_to_annotate, annotated, id_col, label_col)

        for index, row in df.iterrows():
            if pd.isna(row[iri_col]):
                iri_index = build_taxon_index(row, id_col, label_col)
                df.at[index, iri_col] = mapping[iri_index]["iri"]
                df.at[index, "integraph.id"] = mapping[iri_index]["id"]
                df.at[index, "integraph.label"] = mapping[iri_index]["label"]

        return df

    def map(
        self, df, id_col="integraph.id", label_col="integraph.label", target="ncbi"
    ):
        subset = [col for col in [id_col, label_col] if col]
        taxa_to_map = (
            df.dropna(subset=id_col).drop_duplicates(subset=subset).replace("", np.nan)
        )
        mapped = pd.DataFrame()
        matchers = ["wikidata-web", target]
        not_found = taxa_to_map
        for matcher in matchers:
            if not not_found.empty:
                new_matches = self.map_taxonomic_entities(
                    not_found, matcher=matcher, kingdom=self.kingdom
                )

                if not new_matches.empty:
                    new_matches = new_matches[
                        new_matches["matchId"].str.startswith(tuple(self.keep_only))
                    ]
                    mapped = (
                        pd.concat([mapped, new_matches], ignore_index=True)
                        if mapped is not None
                        else new_matches
                    )
                    found_in_target = []
                    for name, group in mapped.groupby("queryId", dropna=False):
                        if not group[
                            group["matchId"].str.startswith(target.upper())
                        ].empty:
                            found_in_target.append(name)

                    not_found = not_found[~not_found[id_col].isin(found_in_target)]

        if not mapped.empty:
            df_ncbitaxon = mapped[mapped["matchId"].str.startswith("NCBI:")]
            df_ncbitaxon["matchId"] = df_ncbitaxon["matchId"].apply(
                lambda x: x.replace("NCBI:", "NCBITaxon:")
            )
            df_ncbitaxon["iri"] = df_ncbitaxon["iri"].apply(
                lambda x: x.replace(
                    TAXONOMIES["NCBI:"]["url_prefix"],
                    TAXONOMIES["NCBITaxon:"]["url_prefix"],
                )
            )
            mapped = pd.concat([mapped, df_ncbitaxon])
            mapped = mapped[mapped["queryId"] != mapped["matchId"]]
        return mapped

    def map_taxonomic_entities(self, df, matcher, kingdom=None):
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
                matching = matching[mask]
                if matcher == self.name_matcher and kingdom:
                    kingdom_mask = []
                    for _, row in matching.iterrows():
                        ranks = [rank.strip(" ") for rank in row["linNames"].split("|")]
                        kingdom_mask.append(ranks and kingdom in ranks)
                    matching = matching[kingdom_mask]
                return matching
        return pd.DataFrame()
