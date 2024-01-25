from .base import Annotator
from ...util.nomer import NomerHelper, TAXONOMIES
from ...util.gnparser import normalize_names
import pandas as pd
import numpy as np
import logging

# EXACT_MATCH_TERMS = ["SAME_AS", "HAS_ACCEPTED_NAME"]  # "SYNONYM_OF", ]


class UnsupportedTaxonomyException(Exception):
    pass


class MultipleMatch(Exception):
    pass


class TaxonomyAnnotator(Annotator):
    def __init__(self, config):
        self.nomer = NomerHelper()
        self.matchers = {
            "GBIF": "gbif",
            "NCBI": "ncbi",
            "IF": "indexfungorum",
            "default": "globalnames",
        }
        self.exact_match_terms = ["SAME_AS", "HAS_ACCEPTED_NAME"]
        self.source = config.get("source", "default")
        # self.name_matcher = config.get("name_matcher", "globalnames")
        self.filters = config.get("filter_on_ranks", None)
        if self.filters:
            self.filters = (
                [self.filters] if isinstance(self.filters, str) else self.filters
            )
        self.targets = config.get("targets", ["NCBI", "GBIF", "IF", "EOL", "OTT"])
        self.multiple_match = config.get("multiple_match", "warning")
        self.include_synonym = config.get("include_synonym", False)
        if self.include_synonym:
            self.exact_match_terms.append("SYNONYM_OF")

    def annotate(
        self,
        df,
        id_col,
        label_col,
        iri_col,
        replace=False,
    ):
        def add_prefix(taxids, taxo):
            """
            Prefix taxonomic ids in with the id of a taxonomy

            Parameters
            ----------
            taxids : pandas.Series
                A pandas.Series contaning the taxonomic ids.
            taxo : str
                The id of the taxonomy.
            """

            def build_prefixed_id(id, taxo):
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
                        return f"{taxo.upper()}:{id}"
                return None

            return taxids.map(lambda id: build_prefixed_id(id, taxo))

        def build_taxon_index(taxon_info, id_col, label_col):
            """
            Return a tuple containing a taxon identifier and/or name if available

            Parameters
            ----------
            taxon_info : pandas.Series
                The row of a pandas.DataFrame contaning the information about a taxon.
            id_col : str
                The name of the column containing the taxonomic id.
            label_col : str
                The name of the column containing the scientific name.

            Returns
            -------
            Tuple
                A possibly empty tuple (taxid, sciName).
            """
            return ((taxon_info[id_col],) if id_col else ()) + (
                (taxon_info[label_col],) if label_col else ()
            )

        def build_mapping_dict(queried, mapped, id_col, label_col):
            """
            Build a mapping between the taxa in the original pandas.DataFrame
            and the match in the target taxonomies.

            Parameters
            ----------
            queried : pandas.DataFrame
                The original taxonomic data.
            mapped : pandas.DataFrame
                The result of taxonomic entity mapping.
            id_col : str
                The name of the column containing the taxonomic id.
            label_col : str
                The name of the column containing the scientific name.

            Returns
            -------
            _type_
                _description_

            Raises
            ------
            MultipleMatch
                _description_
            """
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
                    match = mapped.get_group(group_name)
                    preferred_match = self.handle_multiple_match(iri_index, match)
                except (KeyError, MultipleMatch):
                    mapping[iri_index] = {"iri": np.nan, "id": np.nan, "label": np.nan}
                else:
                    mapping[iri_index] = {
                        "iri": preferred_match["iri"],
                        "id": preferred_match["matchId"],
                        "label": preferred_match["matchName"],
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

        # Prepare taxonomic data for annotation
        if id_col and self.source != "default":
            # Build prefixed taxonomic ids
            taxa_to_annotate["integraph.id"] = add_prefix(
                taxa_to_annotate[id_col], self.source
            )
        if label_col:
            # Normalize scientific names using Global Names Parser
            taxa_to_annotate["integraph.label"] = taxa_to_annotate[label_col]
            names = taxa_to_annotate[label_col].dropna().to_list()
            names = normalize_names(names)
            taxa_to_annotate = taxa_to_annotate.replace({"integraph.label": names})

        matcher = self.matchers.get(self.source)
        logging.info(f"Received source {self.source}, use matcher {matcher}")

        filters = self.filters if self.source == "default" else None
        annotated = self.map_taxonomic_entities(
            taxa_to_annotate, matcher=matcher, filters=filters
        )

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

    def map(self, df, id_col="integraph.id", label_col="integraph.label"):
        """Map taxonomic annotations to target taxonomies

        Parameters
        ----------
        df : pandas.DataFrame
            A DataFrame with a column containing scientific names and a column containing taxonomic identifiers
        id_col : str, optional
            The name of the column containing taxonomic identifiers, by default "integraph.id"
        label_col : str, optional
            The name of the column containing scientific names, by default "integraph.label"
        target : str, optional
            _description_, by default "ncbi"

        Returns
        -------
        _type_
            _description_
        """
        subset = [col for col in [id_col, label_col] if col]
        taxa_to_map = (
            df.dropna(subset=id_col).drop_duplicates(subset=subset).replace("", np.nan)
        )
        mapped = pd.DataFrame()
        target = "ncbi"
        not_found = taxa_to_map
        for matcher in ["wikidata-web", "ott", target]:
            # if not not_found.empty:
            filters = self.filters if matcher == target else None
            new_matches = self.map_taxonomic_entities(
                not_found, matcher=matcher, filters=filters
            )

            if not new_matches.empty:
                mapped = pd.concat([mapped, new_matches], ignore_index=True)
                # found_in_target = []
                # for name, group in mapped.groupby("queryId", dropna=False):
                #     if not group[
                #         group["matchId"].str.startswith(target.upper())
                #     ].empty:
                #         found_in_target.append(name)

                # not_found = not_found[~not_found[id_col].isin(found_in_target)]

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
            mapped = mapped[mapped["queryId"] != mapped["matchId"]].drop_duplicates(
                subset=["matchId"]
            )

        return mapped

    def map_taxonomic_entities(self, df, matcher, filters=None):
        """
        Validate all taxonomic identifiers in a DataFrame column against a given taxonomy
        """
        query = self.nomer.df_to_query(
            df=df,
            id_column="integraph.id",
            name_column="integraph.label",
        )
        if query:
            match = self.nomer.ask_nomer(query, matcher=matcher)

            # Keep only exact match
            if not match.empty:
                mask = match["matchType"].isin(self.exact_match_terms)
                match = match[mask]

            # Keep only match in target taxonomies
            if not match.empty:
                mask = match["matchId"].str.startswith(tuple(self.targets))
                match = match[mask]

            # Apply filters on taxonomic lineages
            if not match.empty and filters:
                mask = []
                for _, row in match.iterrows():
                    ranks = (
                        [rank.strip(" ") for rank in row["linNames"].split("|")]
                        if isinstance(row["linNames"], str)
                        else []
                    )
                    mask.append(any([r in ranks for r in filters]))
                match = match[mask]

            return match
        return pd.DataFrame()

    def handle_multiple_match(self, iri_index, match):
        def get_preferred_match(match_per_target):
            for target in self.targets:
                if target in match_per_target and match_per_target[target]:
                    return match_per_target[target][0]
            return None

        multiple_match = False
        # Group match per target taxonomy
        match_per_target = {}
        for index, row in match.iterrows():
            target_id = row["matchId"].split(":")[0]
            if target_id not in match_per_target:
                match_per_target[target_id] = []
            match_per_target[target_id].append(index)

        # For each target taxonomy, check if there is multiple match
        for target_id in match_per_target:
            match_in_target = match.loc[match_per_target[target_id]]
            mask = match_in_target["matchType"].isin(self.exact_match_terms)
            multiple_match_in_target = match_in_target[mask].shape[0] > 1
            if multiple_match_in_target:
                if self.multiple_match != "ignore":
                    logging.warn(
                        f"Multiple matches for taxon {iri_index} in taxonomy {target_id} : {match_in_target}"
                    )
                multiple_match = multiple_match_in_target

        if multiple_match and self.multiple_match == "strict":
            raise MultipleMatch(f"Multiple matches for taxon {iri_index}: {match}")

        # print(match)
        preferred_match_index = get_preferred_match(match_per_target)
        # print(preferred_match_index)
        return (
            match.loc[preferred_match_index]
            if preferred_match_index is not None
            else None
        )
