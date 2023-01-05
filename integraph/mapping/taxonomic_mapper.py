import logging
import yaml
import os
import docker
import re
import sys
from tempfile import NamedTemporaryFile
import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError
from docker.errors import NotFound, APIError
from io import StringIO

# from pynomer.client import NomerClient
# from ..core import URIMapper, URIManager, TaxId

from ..util.taxo_helper import *

pd.options.mode.chained_assignment = None


"""
https://github.com/globalbioticinteractions/globalbioticinteractions/wiki/Taxonomy-Matching
"""


class NoValidColumnException(Exception):
    pass


class ConfigurationError(Exception):
    pass


def create_mapping(df):
    """
    Return a dict that keeps track of duplicated items in a DataFrame
    """
    return (
        df.reset_index()
        .groupby(df.columns.tolist(), dropna=False)["index"]
        .agg(["first", tuple])
        .set_index("first")["tuple"]
        .to_dict()
    )


class TaxonomicEntityValidator:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.taxo_to_matcher = {
            "GBIF": "gbif",
            "NCBI": "ncbi",
            "IF": "indexfungorum",
            "SILVA": "ncbi",
            "EOL": "eol",
        }
        self.default_name_matcher = "globalnames"
        self.nomer = NomerHelper()

    def validate(self, df):
        """For a subset of columns (e.g. consumers and resources),
        validate taxonomic ids and/or names against a source taxonomy.

        Returns the input DataFrame with new columns containing the valid
        ids and names for each query column.
        """

        for column_config in self.config.columns:

            # Set default values
            assert column_config.uri_column != None
            column_config.id_column = (
                column_config.id_column if "id_column" in column_config else None
            )
            column_config.name_column = (
                column_config.name_column if "name_column" in column_config else None
            )
            column_config.source_taxonomy = (
                column_config.source_taxonomy
                if "source_taxonomy" in column_config
                else None
            )
            if not (column_config.id_column or column_config.name_column):
                raise NoValidColumnException(
                    "You should specify at least one valid column containing the taxon names or ids."
                )

            # Map taxa to target taxonomy
            self.logger.info(
                f"Validate {df.shape[0]} taxa from columns ({column_config.id_column},{column_config.name_column})"
            )

            valid_df = self.validate_columns(
                df,
                id_column=column_config.id_column,
                name_column=column_config.name_column,
                source_taxonomy=column_config.source_taxonomy,
            )

            df[column_config.uri_column] = valid_df["iri"]
            df[column_config.valid_name_column] = valid_df["valid_name"]
            df[column_config.valid_id_column] = valid_df["valid_id"]

        return df

    def validate_columns(
        self, df, id_column=None, name_column=None, source_taxonomy=None
    ):
        """
        Taxonomic entity validation consists in checking that the pair (taxid, name)
        is valid in a given taxonomy (both taxid and name are optional, but at least
        one of them must exist). This function adds a column "valid_id" and a column
        "valid_name" to the input DataFrame. If both values are NaN, the corresponding
        entity is considered invalid.
        """

        def add_prefix(col, src_taxo):
            """
            Add the source taxonomy name as a prefix to all taxids in a column
            """

            def return_prefixed(id, src_taxo):
                if (
                    pd.notnull(id) and len(str(id).split(":")) == 2
                ):  # .startswith(src_taxo + ":"):
                    return (
                        id
                        if not pd.isna(
                            pd.to_numeric(str(id).split(":")[-1], errors="coerce")
                        )
                        else np.nan
                    )
                elif pd.notnull(id) and pd.isna(pd.to_numeric(id, errors="coerce")):
                    return np.nan
                elif pd.notnull(id):
                    return f"{src_taxo}:{id}"
                else:
                    return None

            return col.map(lambda id: return_prefixed(id, src_taxo))

        assert id_column or name_column

        subset = [col for col in [id_column, name_column] if col]
        sub_df = df[subset].astype(pd.StringDtype(), errors="ignore")
        mapping = create_mapping(
            sub_df
        )  # Mapping from items in drop_df to all duplicates in sub_df
        drop_df = sub_df.drop_duplicates(subset=subset).replace("", np.nan)

        id_df = None
        name_df = None

        if id_column:
            assert source_taxonomy
            if source_taxonomy in self.taxo_to_matcher:
                drop_df[id_column] = add_prefix(drop_df[id_column], source_taxonomy)
                id_df = drop_df.dropna(subset=[id_column])
        if name_column:
            drop_df["canonical_name"] = drop_df[name_column]
            names = drop_df["canonical_name"].dropna().to_list()
            norm_names = self.normalize_names(names)
            drop_df.replace({"canonical_name": norm_names}, inplace=True)
            if id_df is not None:
                name_df = drop_df.loc[~drop_df.index.isin(id_df.index)]
            else:
                name_df = drop_df.dropna(subset=["canonical_name"])

        sub_df["valid_id"] = None
        sub_df["valid_name"] = None
        sub_df["iri"] = None

        if id_df is not None and not id_df.empty:
            valid_ids = self.validate_taxids(
                id_df, id_column, name_column, source_taxonomy
            )
            valid_ids = valid_ids.groupby(
                ["queryId"], dropna=False
            )  # Get all matches for each id
            for index, row in drop_df.iterrows():
                id = row[id_column]
                if pd.notnull(id) and id in valid_ids.groups:
                    valid = valid_ids.get_group(id).iloc[0]
                    for i in mapping[index]:
                        sub_df.at[i, "valid_id"] = valid["matchId"]
                        sub_df.at[i, "valid_name"] = valid["matchName"]
                        sub_df.at[i, "iri"] = valid["iri"]

        if name_df is not None and not name_df.empty:
            valid_names = self.validate_names(name_df, "canonical_name")
            valid_names = valid_names.groupby(
                ["queryName"], dropna=False
            )  # Get all matches for each name
            for index, row in drop_df.iterrows():
                name = row["canonical_name"]  # name_column]
                if pd.notnull(name) and name in valid_names.groups:
                    valid = valid_names.get_group(name).iloc[0]
                    for i in mapping[index]:
                        sub_df.at[i, "valid_id"] = valid["matchId"]
                        sub_df.at[i, "valid_name"] = valid["matchName"]
                        sub_df.at[i, "iri"] = valid["iri"]

        if source_taxonomy == "SILVA":
            self.logger.debug("SILVA : all names and ids are valid by default")
            for index, row in drop_df.iterrows():
                for i in mapping[index]:
                    if id_column:
                        sub_df.at[i, "valid_id"] = (
                            row[id_column]
                            if row[id_column].startswith("SILVA:")
                            else sub_df.at[i, "valid_id"]
                        )
                        taxid = row[id_column].split(":")[-1]
                        sub_df.at[i, "iri"] = (
                            f"https://www.arb-silva.de/{taxid}"
                            if row[id_column].startswith("SILVA:")
                            else sub_df.at[i, "iri"]
                        )
                    if name_column:
                        sub_df.at[i, "valid_name"] = (
                            row[name_column]
                            if sub_df.at[i, "valid_id"].startswith("SILVA:")
                            else sub_df.at[i, "valid_name"]
                        )

        self.logger.debug(sub_df[["valid_id", "valid_name", "iri"]])

        # Get some statistics
        df_drop = sub_df.drop_duplicates(subset=subset)
        nb_unique = df_drop.shape[0]
        nb_valid = df_drop.dropna(subset=["valid_id"]).shape[0]
        self.logger.info(f"Found {nb_valid}/{nb_unique} valid taxonomic entities")
        return sub_df

    def normalize_names(self, names):
        """
        Given a list of taxonomic names, return the corresponding canonical forms
        """
        f_temp = NamedTemporaryFile(delete=True)  # False)
        self.logger.debug(f"Write names to {f_temp.name} for validation using gnparser")
        names_str = "\n".join([name.replace("\n", " ") for name in names])
        f_temp.write(names_str.encode())
        f_temp.read()  # I don't know why but it is needed or sometimes the file appears empty when reading
        canonical_names = get_canonical_names(f_temp.name)
        f_temp.close()
        canonical_names = canonical_names["CanonicalSimple"].to_list()
        assert len(names) == len(canonical_names)
        return {names[i]: canonical_names[i] for i in range(len(names))}

    def validate_names(self, df, name_column):
        """
        Validate all taxonomic names in a DataFrame column
        """
        names = df[name_column].to_list()
        self.logger.debug(f"Validate names {names} using {self.default_name_matcher}")
        query = self.nomer.df_to_query(
            df=df,
            name_column=name_column,
        )
        matching = self.nomer.ask_nomer(query, matcher=self.default_name_matcher)
        if not matching.empty:
            mask = matching["matchType"].isin(
                ["SAME_AS", "SYNONYM_OF", "HAS_ACCEPTED_NAME"]  # , "SIMILAR_TO"]
            )
            return matching[mask]
        return matching

    def validate_taxids(self, df, id_column, name_column=None, source_taxonomy=None):
        """
        Validate all taxonomic identifiers in a DataFrame column against a given taxonomy
        """
        matcher = self.taxo_to_matcher[source_taxonomy]
        taxids = df[id_column].to_list()
        self.logger.debug(f"Validate taxids {taxids} using {matcher}")
        query = self.nomer.df_to_query(
            df=df,
            id_column=id_column,
            name_column=name_column,
        )
        matching = self.nomer.ask_nomer(query, matcher=matcher)
        if not matching.empty:
            mask = matching["matchType"].isin(
                ["SAME_AS", "SYNONYM_OF", "HAS_ACCEPTED_NAME"]  # , "SIMILAR_TO"]
            )
            return matching[mask]
        return matching


def test_validator():
    df = pd.read_csv(
        "/home/leguilln/workspace/KNOWLEDGE_INTEGRATION/inteGraph/taxo_valid_test.csv",
        sep=";",
        keep_default_na=False,
    )
    validator = TaxonomicEntityValidator()
    df = validator.validate(
        df, id_column="consumer_key", name_column="consumer_scientificName"
    )
    df.to_csv(
        "/home/leguilln/workspace/KNOWLEDGE_INTEGRATION/inteGraph/taxo_valid_result.csv",
        sep=";",
    )


class TaxonomicEntityMapper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.default_matcher = "wikidata-web"
        self.ncbi_matcher = "ncbi"
        self.target_taxonomy = "NCBI"
        self.keep_taxo = ["NCBI", "GBIF", "IF"]
        self.nomer = NomerHelper()

    def df_to_triples(self, df):
        from rdflib import Graph, URIRef, Literal
        from rdflib.namespace import RDFS, OWL

        g = Graph()
        for index, row in df.iterrows():
            if row["queryIRI"] != row["iri"]:
                g.add((URIRef(row["queryIRI"]), OWL.sameAs, URIRef(row["iri"])))
                g.add((URIRef(row["queryIRI"]), RDFS.label, Literal(row["queryName"])))
            if row["matchId"].split(":")[0].startswith("NCBI"):
                taxid = row["matchId"].split(":")[-1]
                g.add(
                    (
                        URIRef(row["queryIRI"]),
                        OWL.sameAs,
                        URIRef(f"http://purl.obolibrary.org/obo/NCBITaxon_{taxid}"),
                    )
                )
            g.add((URIRef(row["iri"]), RDFS.label, Literal(row["matchName"])))
            if pd.notna(row["matchRank"]):
                g.add(
                    (
                        URIRef(row["iri"]),
                        URIRef("http://purl.obolibrary.org/obo/ncbitaxon#has_rank"),
                        Literal(row["matchRank"]),
                    )
                )
        return g

    def map(self, df):
        """For a subset of columns (e.g. consumers and resources),
        try to map taxon ids and/or names to IRIs in a target taxonomy
        using a TaxonomicEntityMapper.

        Returns the input DataFrame with new columns containing the IRIs for each
        query column.
        """

        taxa_dfs = []
        for column_config in self.config.columns:

            assert column_config.uri_column != None
            # Set default values
            column_config.id_column = (
                column_config.id_column if "id_column" in column_config else None
            )
            column_config.name_column = (
                column_config.name_column if "name_column" in column_config else None
            )
            if not (column_config.id_column or column_config.name_column):
                raise NoValidColumnException(
                    "You should specify at least one valid column containing the taxon names or ids."
                )

            # Map taxa to target taxonomy
            self.logger.info(
                f"Map {df.shape[0]} taxons from columns ({column_config.id_column},{column_config.name_column}) to target taxo {self.target_taxonomy}"
            )

            id_to_iri = (
                df[[column_config.id_column, column_config.uri_column]]
                .astype(pd.StringDtype(), errors="ignore")
                .drop_duplicates()
            )
            id_to_iri = id_to_iri.set_index(column_config.id_column, drop=True)[
                column_config.uri_column
            ].to_dict()

            taxa_df = self.map_columns(
                df,
                id_column=column_config.id_column,
                name_column=column_config.name_column,
            )
            taxa_df["queryIRI"] = taxa_df["queryId"].map(id_to_iri)
            taxa_dfs.append(taxa_df)

        return pd.concat(taxa_dfs, ignore_index=True)

    def map_columns(self, df, id_column=None, name_column=None):
        assert id_column or name_column

        subset = [col for col in [id_column, name_column] if col]
        sub_df = df[subset].astype(pd.StringDtype(), errors="ignore")
        mapping = create_mapping(
            sub_df
        )  # Mapping from items in drop_df to all duplicates in sub_df
        drop_df = sub_df.drop_duplicates(  # .dropna(how="all", subset=subset)
            subset=subset
        ).replace("", np.nan)

        matches = pd.DataFrame()
        if id_column:
            self.logger.debug(
                f"Map {drop_df.shape[0]} unique taxa to target taxonomy {self.target_taxonomy} using {self.default_matcher}"
            )
            matches = self.map_ids_and_names_to_target_taxo(
                drop_df, id_column, name_column, matcher=self.default_matcher
            )
            # Get all matches for each id
            if not matches.empty:
                matches = matches[
                    matches["matchId"].str.startswith(tuple(self.keep_taxo))
                ]

            if matches.empty:
                not_found_df = drop_df
            else:
                not_found_in_target_taxo = []
                for name, group in matches.groupby(
                    ["queryId", "queryName"], dropna=False
                ):
                    if group[
                        group["matchId"].str.startswith(self.target_taxonomy)
                    ].empty:
                        self.logger.debug(
                            f"Entity {name} not found in target taxonomy {self.target_taxonomy}"
                        )
                        not_found_in_target_taxo.append(name)

                not_found_df = pd.DataFrame.from_records(
                    not_found_in_target_taxo, columns=[id_column, name_column]
                )

                not_found_df = pd.concat(  # Required if we want to use SILVA taxonomy
                    [
                        not_found_df,
                        drop_df[~drop_df[id_column].isin(matches["queryId"])],
                    ]
                )
        else:
            not_found_df = drop_df

        if not not_found_df.empty:
            self.logger.debug(
                f"Map {not_found_df.shape[0]} remaining taxa to target taxonomy {self.target_taxonomy} using {self.ncbi_matcher}"
            )
            additional_matches = self.map_ids_and_names_to_target_taxo(
                not_found_df, id_column, name_column, matcher=self.ncbi_matcher
            )

            # Remove queries with multiple matching names in the same taxo
            keep = []
            for name, group in additional_matches.groupby(["queryId"], dropna=False):
                matches_in_taxo = group["matchId"].unique()
                taxos = [x.split(":")[0] for x in matches_in_taxo]
                keep_match = True
                for taxo in taxos:
                    if taxos.count(taxo) > 1:
                        self.logger.debug(
                            f"Multiple matches for {name} in taxonomy {taxo} : {matches_in_taxo} -> discard !"
                        )
                        keep_match = False
                        break
                if keep_match:
                    keep.append(name)
            additional_matches = additional_matches[
                additional_matches["queryId"].isin(keep)
            ]

            if not matches.empty:
                matches = pd.concat([matches, additional_matches], ignore_index=True)
            else:
                matches = additional_matches

        return matches

    def map_ids_and_names_to_target_taxo(self, df, id_column, name_column, matcher):
        temp_df = df.copy()
        temp_df[id_column] = temp_df[id_column].fillna("")
        mask = temp_df[id_column].str.startswith("SILVA:")
        # print(mask.unique())
        # print(temp_df[id_column].fillna("").astype(str))
        temp_df[id_column] = temp_df[id_column].astype(str).mask(mask.astype("bool"))
        query = self.nomer.df_to_query(
            df=temp_df,
            id_column=id_column,
            name_column=name_column,
        )
        matching = self.nomer.ask_nomer(query, matcher=matcher)
        # print(matching[["queryId", "matchType"]])
        if not matching.empty:
            mask = matching["matchType"].isin(
                ["SAME_AS", "SYNONYM_OF", "HAS_ACCEPTED_NAME"]
            )
            matching = matching[mask]

            if not matching.empty:
                # Required if we want to use SILVA taxonomy : if the queryId column is empty,
                # use the valid_id in df instead
                matching["queryId"] = matching.apply(
                    lambda x: df[df[name_column] == x["queryName"]][id_column].iloc[0]
                    if x["queryId"] == ""
                    else x["queryId"],
                    axis=1,
                )
        return matching  # [mask]


def test_mapper():
    df = pd.read_csv(
        "/home/leguilln/workspace/KNOWLEDGE_INTEGRATION/inteGraph/taxo_valid_result.csv",
        sep=";",
        keep_default_na=False,
    )
    mapper = TaxonomicEntityMapper(None, None)
    mapper.map(df, id_column="valid_id", name_column="valid_name")

    # def source_id_to_target_id(self, df, id_column, name_column=None, matcher=None):
    #     """
    #     Ask nomer about a batch of taxon identifiers using a given matcher.
    #     """
    #
    #     df_copy = df.drop_duplicates(subset=id_column)
    #
    #     # Keep a reference to the index of the row containing the identifier
    #     query_to_ref_index_map = {
    #         row[id_column]: index for index, row in df_copy.iterrows()
    #     }
    #
    #     query = self.nomer.df_to_query(
    #         df=df_copy,
    #         id_column=id_column,
    #         name_column=name_column,
    #     )
    #     res_df = self.nomer.ask_nomer(query, matcher=matcher)
    #
    #     # Keep non empty matches with types SAME_AS or SYNONYM_OF in the target taxonomy
    #     mapped_df = res_df.dropna(axis=0, subset=["matchId"])
    #     mapped_df = mapped_df[
    #         mapped_df["matchType"].isin(["SAME_AS", "SYNONYM_OF", "HAS_ACCEPTED_NAME"])
    #     ]
    #
    #     if not mapped_df.empty:
    #         mapped_df = mapped_df[
    #             mapped_df["matchId"].str.startswith(self.target_taxonomy)
    #         ]
    #
    #         # Check if a single taxon matches with several (distinct) ids in the target taxonomy
    #         # Sometimes, the same taxon is matched several times to the same target id,
    #         # so we start by dropping these duplicates, then looks for remaining duplicates.
    #         mapped_df_dropped = mapped_df.drop_duplicates(
    #             subset=["queryId", "matchId"], keep=False
    #         )
    #         duplicated = mapped_df_dropped.duplicated(subset=["queryId"], keep=False)
    #         # print(mapped_df)
    #         # print(duplicated)
    #         if duplicated.any():
    #             self.logger.error(
    #                 f"Found several target IDs for a single taxa in df: {mapped_df_dropped[duplicated]}"
    #             )
    #             # mapped_df[duplicated].to_csv(f"duplicated_{id_column}.csv")
    #             mapped_df = mapped_df_dropped[~duplicated]
    #             # raise Exception("Unable to handle multiple candidates at id level.")
    #
    #         # Reset index using the (id, index) map
    #         mapped_index = mapped_df["queryId"].map(query_to_ref_index_map)
    #         mapped_df.set_index(
    #             pd.Index(mapped_index.tolist()).rename("externalId"),
    #             inplace=True,
    #         )
    #
    #         # others_df contains all the taxa that have no match in the target taxonomy
    #         others_df = df_copy[~df_copy[id_column].isin(mapped_df["queryId"])]
    #
    #     else:
    #         others_df = df
    #
    #     return mapped_df, others_df


class TaxonomicEntityMapperOld:
    def __init__(self, prefix, config):
        self.logger = logging.getLogger(__name__)
        # self.uri_mapper = URIMapper()
        self.config = config
        self.prefix = prefix

        self.default_taxonomy = "NCBI"

        # Validate source taxonomy
        self.source_taxonomy = (
            self.config.source_taxonomy if "source_taxonomy" in self.config else None
        )
        if not (self.source_taxonomy and self.source_taxonomy in self.prefix):
            self.logger.error(
                "Invalid source taxonomy {} : use default taxonomy {}".format(
                    self.source_taxonomy, self.default_taxonomy
                )
            )
            self.source_taxonomy = self.default_taxonomy

        # Validate target taxonomy
        self.target_taxonomy = (
            self.config.target_taxonomy if "target_taxonomy" in self.config else None
        )
        if not (self.target_taxonomy and self.target_taxonomy in self.prefix):
            self.logger.error(
                "Invalid target taxonomy {} : use default taxonomy {}".format(
                    self.target_taxonomy, self.default_taxonomy
                )
            )
            self.target_taxonomy = self.default_taxonomy

        self.nomer = NomerHelper()
        self.taxo_to_matcher = {"GBIF": "gbif", "NCBI": "ncbi", "IF": "indexfungorum"}
        self.gnparser = GNParserHelper()

    # def format_id_column(self, df, id_column, taxonomy):
    #     """
    #     Format taxon identifier as DB:XXXX (e.g. NCBI:6271, GBIF:234581...).
    #     """
    #
    #     return df.apply(
    #         lambda row: f"{self.source_taxonomy}:" + row[id_column]
    #         if (
    #             pd.notnull(row[id_column])
    #             and not row[id_column].startswith(self.source_taxonomy + ":")
    #         )
    #         else row[id_column],
    #         axis=1,
    #     )
    #
    # def validate(self, df, id_column=None, name_column=None, matcher=None):
    #     """
    #     Match taxon from the source taxonomy to the source taxonomy
    #     (e.g.GBIF to GBIF) to filter out taxa with invalid ids.
    #     """
    #
    #     # Drop duplicates for efficiency
    #     # subset = [
    #     #     col for col in [id_column, name_column] if col
    #     # ]
    #     subset = id_column if id_column else name_column
    #     df_copy = df.copy().drop_duplicates(subset=subset)
    #
    #     # For NCBI and GBIF, the id must be an integer
    #
    #     if id_column:
    #         # valid_df = df_copy
    #         valid_df = df_copy[df_copy[id_column].str.isnumeric()]
    #
    #         if self.source_taxonomy:  # Format taxon id
    #             valid_df[id_column] = valid_df[id_column].map(
    #                 lambda id: f"{self.source_taxonomy}:{id}"
    #                 if (
    #                     pd.notnull(id)
    #                     and not str(id).startswith(self.source_taxonomy + ":")
    #                 )
    #                 else id
    #             )
    #
    #         if not valid_df.empty:
    #
    #             query = self.nomer.df_to_query(
    #                 df=valid_df,
    #                 id_column=id_column,
    #                 name_column=name_column,
    #             )
    #             res_df = self.nomer.ask_nomer(query, matcher=matcher)
    #             res_df.set_index(
    #                 pd.Index(res_df["queryId"]).rename("externalId"),
    #                 drop=False,
    #                 inplace=True,
    #             )
    #             mask = res_df["matchType"].isin(
    #                 ["SAME_AS", "SYNONYM_OF", "HAS_ACCEPTED_NAME"]
    #             )
    #             valid_df = res_df[mask]
    #             # invalid_df = res_df[~mask]
    #             #
    #             # if not invalid_df.empty and name_column:
    #             #     invalid_df = invalid_df[id_column == None]
    #             #
    #             #     query = self.nomer.df_to_query(
    #             #         df=invalid_df,
    #             #         name_column=name_column,
    #             #     )
    #
    #         else:
    #             raise EmptyDataError("Empty query")
    #
    #     else:  # Validate names (all names are considered valid)
    #         f_temp = NamedTemporaryFile(delete=True)  # False)
    #         self.logger.debug(
    #             f"Write names to {f_temp.name} for validation using gnparser"
    #         )
    #         names = [name.replace("\n", " ") for name in df_copy[name_column].to_list()]
    #         names_str = "\n".join(names)
    #         f_temp.write(names_str.encode())
    #         f_temp.read()  # I don't know why but it is needed or sometimes the file appears empty when reading
    #         success, canonical_names = self.gnparser.get_canonical_name(f_temp.name)
    #         f_temp.close()
    #
    #         valid_df = pd.DataFrame(
    #             columns=self.nomer.columns,
    #         )
    #         valid_df["queryName"] = df_copy[name_column].to_list()
    #         valid_df["matchType"] = "SAME_AS"
    #         valid_df["matchName"] = valid_df["queryName"]
    #         if success:
    #             valid_df["matchName"] = canonical_names["CanonicalSimple"]
    #         # valid_df.to_csv("valid_name.csv")
    #         valid_df = valid_df.set_index("queryName", drop=False)
    #         valid_df = valid_df.set_index(valid_df.index.rename("externalId"))
    #         valid_df = valid_df.drop_duplicates(subset="matchName")
    #
    #     return valid_df  # , invalid_df

    # def source_id_to_target_id(self, df, id_column, name_column=None, matcher=None):
    #     """
    #     Ask nomer about a batch of taxon identifiers using a given matcher.
    #     """
    #
    #     df_copy = df.drop_duplicates(subset=id_column)
    #
    #     # Keep a reference to the index of the row containing the identifier
    #     query_to_ref_index_map = {
    #         row[id_column]: index for index, row in df_copy.iterrows()
    #     }
    #
    #     query = self.nomer.df_to_query(
    #         df=df_copy,
    #         id_column=id_column,
    #         name_column=name_column,
    #     )
    #     res_df = self.nomer.ask_nomer(query, matcher=matcher)
    #
    #     # Keep non empty matches with types SAME_AS or SYNONYM_OF in the target taxonomy
    #     mapped_df = res_df.dropna(axis=0, subset=["matchId"])
    #     mapped_df = mapped_df[
    #         mapped_df["matchType"].isin(["SAME_AS", "SYNONYM_OF", "HAS_ACCEPTED_NAME"])
    #     ]
    #
    #     if not mapped_df.empty:
    #         mapped_df = mapped_df[
    #             mapped_df["matchId"].str.startswith(self.target_taxonomy)
    #         ]
    #
    #         # Check if a single taxon matches with several (distinct) ids in the target taxonomy
    #         # Sometimes, the same taxon is matched several times to the same target id,
    #         # so we start by dropping these duplicates, then looks for remaining duplicates.
    #         mapped_df_dropped = mapped_df.drop_duplicates(
    #             subset=["queryId", "matchId"], keep=False
    #         )
    #         duplicated = mapped_df_dropped.duplicated(subset=["queryId"], keep=False)
    #         # print(mapped_df)
    #         # print(duplicated)
    #         if duplicated.any():
    #             self.logger.error(
    #                 f"Found several target IDs for a single taxa in df: {mapped_df_dropped[duplicated]}"
    #             )
    #             # mapped_df[duplicated].to_csv(f"duplicated_{id_column}.csv")
    #             mapped_df = mapped_df_dropped[~duplicated]
    #             # raise Exception("Unable to handle multiple candidates at id level.")
    #
    #         # Reset index using the (id, index) map
    #         mapped_index = mapped_df["queryId"].map(query_to_ref_index_map)
    #         mapped_df.set_index(
    #             pd.Index(mapped_index.tolist()).rename("externalId"),
    #             inplace=True,
    #         )
    #
    #         # others_df contains all the taxa that have no match in the target taxonomy
    #         others_df = df_copy[~df_copy[id_column].isin(mapped_df["queryId"])]
    #
    #     else:
    #         others_df = df
    #
    #     return mapped_df, others_df
    #
    # def source_name_to_target_id(
    #     self, df, id_column=None, name_column=None, matcher="ncbi-taxon"
    # ):
    #     """Ask nomer about a batch of taxon names using a given matcher.
    #     When there are several taxa matching the same name, we try to find
    #     the best match using a maximum lineage similarity approach (this
    #     requires a valid id_column).
    #     """
    #
    #     # df_copy = df.drop_duplicates(subset=[id_column, name_column])
    #     ref_col = id_column if id_column else name_column
    #
    #     duplicated = df.duplicated(subset=ref_col, keep=False)
    #     # df.loc[duplicated].to_csv(f"duplicated_base_{ref_col}.csv")
    #
    #     # Keep a reference to the index of the row containing the identifier
    #     query_to_ref_index_map = {row[ref_col]: index for index, row in df.iterrows()}
    #
    #     query = self.nomer.df_to_query(
    #         df=df,
    #         name_column=name_column,
    #         id_column=id_column,
    #     )
    #
    #     res_df = self.nomer.ask_nomer(query, matcher=matcher)
    #
    #     # Keep non empty matches with types SAME_AS or SYNONYM_OF in the target taxonomy
    #     mapped_df = res_df.dropna(axis=0, subset=["matchId"])
    #     mapped_df = mapped_df[
    #         mapped_df["matchType"].isin(["SAME_AS", "SYNONYM_OF", "HAS_ACCEPTED_NAME"])
    #     ]
    #
    #     if not mapped_df.empty:
    #         mapped_df = mapped_df[
    #             mapped_df["matchId"].str.startswith(self.target_taxonomy)
    #         ]
    #
    #         subset = "queryId" if id_column else "queryName"
    #
    #         duplicated = mapped_df.duplicated(subset=[subset], keep=False)
    #
    #         if duplicated.any():
    #             self.logger.info(
    #                 f"Found several target entities for a single taxon in df :\n{mapped_df[duplicated]}"
    #             )
    #             # mapped_df.loc[duplicated].to_csv(f"duplicated_{name_column}.csv")
    #
    #             if id_column:
    #                 keep_index = self.resolve_duplicates(
    #                     df, mapped_df[duplicated], id_column, query_to_ref_index_map
    #                 )
    #                 for index in keep_index:
    #                     duplicated.loc[index] = False
    #             else:
    #                 # TODO : refactor
    #                 # If we do not have access to the ifentifier for disambiguation,
    #                 # we try to compare the lineages. Very often, we have the same
    #                 # name designating the same entity, but at different ranks (e.g. genus and subgenus)
    #                 # In this case, we keep the highest rank (e.g. genus)
    #                 keep_index = []
    #                 duplicates = mapped_df[duplicated]
    #                 unique_names = pd.unique(duplicates["matchName"])
    #                 for name in unique_names:
    #                     df_name = mapped_df[mapped_df["matchName"] == name]
    #                     resolved = False
    #                     if df_name.shape[0] == 2:
    #                         lin = []
    #                         for index, row in df_name.iterrows():
    #                             lin.append((index, row["linNames"].split(" | ")))
    #
    #                         if set(lin[0][1]) == set(lin[1][1]):
    #                             resolved = True
    #                             # We keep the highest rank
    #                             if len(lin[0][1]) < len(lin[1][1]):
    #                                 duplicated.loc[lin[0][0]] = False
    #                             else:
    #                                 duplicated.loc[lin[1][0]] = False
    #                     if not resolved:
    #                         self.logger.debug(
    #                             f"Cannot resolve duplicates : discard taxon {name}."
    #                         )
    #
    #         mapped_df = mapped_df[~duplicated]
    #         mapped_index = mapped_df[subset].map(query_to_ref_index_map)
    #
    #         mapped_df.set_index(
    #             pd.Index(mapped_index.tolist()).rename("externalId"),
    #             inplace=True,
    #         )
    #         others_df = df[~df[ref_col].isin(mapped_df[subset])]
    #
    #     else:
    #         others_df = df
    #
    #     return mapped_df, others_df
    #
    # def resolve_duplicates(self, ref_df, duplicates, id_column, query_to_ref_index_map):
    #
    #     unique_duplicates = duplicates["queryId"].unique()
    #     keep_index = []
    #     self.logger.debug(f"Unique duplicates {unique_duplicates}")
    #     for id in unique_duplicates:
    #         candidates = duplicates[duplicates["queryId"] == id]
    #         if id not in query_to_ref_index_map:
    #             raise KeyError(f"Candidate id {id} has no match")
    #         ref_index = query_to_ref_index_map[id]
    #         reference = ref_df.loc[ref_index, :]  # [ref_df[ref_df.columns[0]] == id]
    #         self.logger.info(f"Get best match for taxon\n{reference}")
    #         best_index = self.get_best_match(
    #             reference,
    #             candidates,
    #         )
    #         keep_index.append(best_index)
    #     return keep_index
    #
    # def get_best_match(self, reference, candidates):
    #     """
    #     Given nomer's response for a given taxon, return the best match.
    #     If only one match, returns the candidate IRI
    #     If more than one match, get the full lineage of the query taxon from its taxid
    #         For each candidate
    #             Compute the similarity between the query taxon lineage and the candidate lineage
    #             Similarity = the length of the intersection of the two lineages
    #         Return best match = the IRI of the candidate with the maximum similarity
    #     """
    #     ref_lineage = [x.strip(" ") for x in reference["linNames"].split("|") if x]
    #
    #     candidate_lineages = [
    #         {
    #             "index": index,
    #             "lineage": [x.strip(" ") for x in row["linNames"].split("|") if x],
    #         }
    #         for index, row in candidates.iterrows()
    #     ]
    #
    #     if len(set([",".join(tgt["lineage"]) for tgt in candidate_lineages])) == 1:
    #         best_index = candidate_lineages[0]["index"]
    #         best_match = candidates.loc[best_index]
    #         best_match_id = best_match["matchId"]
    #         self.logger.debug(
    #             f"Found multiple matches with similar lineages, return the first one : {best_match_id}"
    #         )
    #     else:
    #         similarities = [
    #             1.0
    #             * len(set(ref_lineage).intersection(tgt["lineage"]))
    #             / len(set(ref_lineage))
    #             for tgt in candidate_lineages
    #         ]
    #
    #         # print(tgt_lineages, similarities)
    #         max_sim = max(similarities)
    #         max_indexes = [i for i, sim in enumerate(similarities) if sim == max_sim]
    #         if len(max_indexes) > 1:
    #             self.logger.debug(
    #                 f"Found multiple best matches with similarity {max_sim:.2f}: cannot find best match"
    #             )
    #             return None
    #
    #         best_index = candidate_lineages[max_indexes[0]]["index"]
    #         best_match = candidates.loc[best_index]
    #         best_match_id = best_match["matchId"]
    #         self.logger.debug(
    #             f"Found best match with similarity {max_sim:.2f} : {best_match_id}"
    #         )
    #     return best_index
    #
    # def map(self, df, id_column, name_column):
    #     """
    #     Using nomer taxonomic mapping capabilities, try to get IRIs in the
    #     target taxonomy from taxon names and/or identifiers in the source
    #     taxonomy.
    #
    #     First, validate taxon names/ids by querying the source taxonomy
    #     Then, try to map taxon ids to the target taxonomy using wikidata-web
    #     For each taxon without a match
    #         Try to map the taxon name using globi
    #         For each taxon without a match
    #             Try to map the taxon name using ncbi
    #     """
    #
    #     # Drop duplicates for efficiency
    #     subset = [x for x in [id_column, name_column] if x]
    #     w_df = df.dropna(subset=subset).drop_duplicates(subset=subset)
    #     # w_df = df.dropna(subset=subset, how="all").drop_duplicates(subset=subset)
    #
    #     self.logger.debug(f"Found {w_df.shape[0]} unique taxa")
    #
    #     # Step 1 : validate taxa against the source taxonomy
    #     self.logger.debug(f"Validate taxa using info from columns {subset}")
    #     valid_df = self.validate(
    #         w_df,
    #         id_column,
    #         name_column,
    #         matcher=self.taxo_to_matcher[self.source_taxonomy],
    #     )
    #     self.logger.debug(f"Found {valid_df.shape[0]}/{w_df.shape[0]} valid taxa")
    #
    #     # src_to_src_mappings is a data frame containing mappings between ids
    #     # in the same source taxonomy. Indeed, a taxon can have several ids
    #     # in a single taxonomy, and the id used in the data may not be the preferred
    #     # id for this taxon.
    #     src_to_src_mappings = valid_df[
    #         (pd.notnull(valid_df["matchId"]))
    #         & (valid_df["queryId"] != valid_df["matchId"])
    #     ]
    #
    #     mapped = []
    #
    #     # Step 2 : map valid taxa using their id in the source taxonomy (if available)
    #     # if id_column:
    #     #     self.logger.debug(
    #     #         f"Map {valid_df.shape[0]} unique taxa to target taxonomy {self.target_taxonomy} using wikidata-web"
    #     #     )
    #     #     mapped_df, others_df = self.source_id_to_target_id(
    #     #         valid_df,
    #     #         id_column="matchId",
    #     #         # name_column="matchName",
    #     #         matcher="wikidata-web",
    #     #     )
    #     #     self.logger.debug(
    #     #         f"Found {mapped_df.shape[0]}/{valid_df.shape[0]} taxa in target taxonomy {self.target_taxonomy}"
    #     #     )
    #     #     mapped.append(mapped_df)
    #     # else:
    #     #     others_df = valid_df
    #     if id_column:
    #         self.logger.debug(
    #             f"Map {valid_df.shape[0]} unique taxa to target taxonomy {self.target_taxonomy} using globi"
    #         )
    #         mapped_df, others_df = self.source_id_to_target_id(
    #             valid_df,
    #             id_column="matchId",
    #             # name_column="matchName",
    #             matcher="globi",
    #         )
    #         self.logger.debug(
    #             f"Found {mapped_df.shape[0]}/{valid_df.shape[0]} taxa in target taxonomy {self.target_taxonomy} using globi"
    #         )
    #         mapped.append(mapped_df)
    #     else:
    #         others_df = valid_df
    #
    #     if not others_df.empty and id_column:
    #         nb_taxa = others_df.shape[0]
    #         self.logger.debug(
    #             f"Map {nb_taxa} remaining taxa to target taxonomy {self.target_taxonomy} using wikidata-web"
    #         )
    #         mapped_df, others_df = self.source_id_to_target_id(
    #             others_df,
    #             id_column="matchId",
    #             # name_column="matchName",
    #             matcher="wikidata-web",
    #         )
    #         self.logger.debug(
    #             f"Found {mapped_df.shape[0]}/{nb_taxa} taxa in target taxonomy {self.target_taxonomy} using wikidata-web"
    #         )
    #         mapped.append(mapped_df)
    #     else:
    #         others_df = valid_df
    #
    #     # Step 3 : map the remaining taxa using their name (if available)
    #     if not others_df.empty and name_column:
    #         nb_taxa = others_df.shape[0]
    #         self.logger.debug(
    #             f"Map {nb_taxa} remaining taxa to target taxonomy {self.target_taxonomy} using ncbi"
    #         )
    #         # others_df.to_csv("others.csv")
    #         mapped_df, others_df = self.source_name_to_target_id(
    #             others_df,
    #             id_column="matchId" if id_column else None,
    #             name_column="matchName",
    #             matcher="ncbi",
    #         )
    #         # mapped_df.to_csv("mapped.csv")
    #         self.logger.debug(
    #             f"Found {mapped_df.shape[0]}/{nb_taxa} taxa in target taxonomy {self.target_taxonomy} using ncbi"
    #         )
    #         mapped.append(mapped_df)
    #
    #     if not others_df.empty and name_column:
    #         nb_taxa = others_df.shape[0]
    #         self.logger.debug(
    #             f"Map {nb_taxa} remaining taxa to target taxonomy {self.target_taxonomy} using globi"
    #         )
    #         mapped_df, others_df = self.source_name_to_target_id(
    #             others_df,
    #             id_column="matchId" if id_column else None,
    #             name_column="matchName",
    #             matcher="globi",
    #         )
    #         self.logger.debug(
    #             f"Found {mapped_df.shape[0]}/{nb_taxa} taxa in target taxonomy {self.target_taxonomy} using globi"
    #         )
    #         mapped.append(mapped_df)
    #
    #     mapped_df = pd.concat(mapped, ignore_index=False)
    #     self.logger.info(
    #         f"Found {mapped_df.shape[0]}/{valid_df.shape[0]} valid taxa in target taxonomy {self.target_taxonomy}"
    #     )
    #
    #     if not id_column:
    #         mapped_df["queryId"] = mapped_df["matchId"]
    #         valid_df = mapped_df
    #
    #     # Create a taxon-iri map and create the Series containing IRIs
    #     ref_col = id_column if id_column else name_column
    #     subset = "queryId" if id_column else "matchId"
    #
    #     if id_column and self.source_taxonomy:
    #         df[id_column] = self.format_id_column(df, id_column, self.source_taxonomy)
    #
    #     src_tgt_map = {}
    #     for external_id in df[ref_col].unique():
    #         print(
    #             external_id,
    #             valid_df[subset][external_id]
    #             if external_id in valid_df.index
    #             else None,
    #         )
    #         valid_id = (
    #             valid_df[subset][external_id] if external_id in valid_df.index else None
    #         )
    #         src_tgt_map[external_id] = valid_id
    #     valid_id_series = df[ref_col].map(src_tgt_map)
    #
    #     return valid_id_series, pd.concat(
    #         [mapped_df, src_to_src_mappings], ignore_index=True
    #     )


class TaxonomicMapper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.config = config
        with open(os.path.abspath(self.config.prefix_file), "r") as ymlfile:
            self.prefix = yaml.load(ymlfile, Loader=yaml.FullLoader)

    def map(self, df):
        """For a subset of columns (e.g. consumers and resources),
        try to map taxon ids and/or names to IRIs in a target taxonomy
        using a TaxonomicEntityMapper.

        Returns the input DataFrame with new columns containing the IRIs for each
        query column.
        """

        def get_full_iri(taxid):
            print(taxid)
            if pd.notnull(taxid):
                items = taxid.split(":")
                return self.prefix[items[0]] + items[-1]
            else:
                return None

        taxon_info = []

        for column_config in self.config.columns:

            # Set default values
            column_config.id_column = (
                column_config.id_column if "id_column" in column_config else None
            )
            column_config.name_column = (
                column_config.name_column if "name_column" in column_config else None
            )
            if not (column_config.id_column or column_config.name_column):
                raise NoValidColumnException(
                    "You should specify at least one valid column containing the taxon names or ids."
                )

            # Map taxa to target taxonomy
            self.logger.info(
                f"Map {df.shape[0]} taxons from columns ({column_config.id_column},{column_config.name_column}) to target taxo {column_config.target_taxonomy}"
            )
            mapper = TaxonomicEntityMapper(self.prefix, column_config)
            tgt_id_series, taxon_info_df = mapper.map(
                df,
                id_column=column_config.id_column,
                name_column=column_config.name_column,
            )
            taxon_info.append(taxon_info_df)
            df[column_config.uri_column] = tgt_id_series
            nb_found = tgt_id_series.count()
            self.logger.info(
                f"Found {nb_found}/{df.shape[0]} ids in target taxo {column_config.target_taxonomy}"
            )

            print(tgt_id_series)

            df[column_config.uri_column] = df[column_config.uri_column].apply(
                lambda x: get_full_iri(x)
            )

        # All info concerning mapped taxa are stored in a separate dataframe (one row per taxon)
        taxon_info_df = pd.concat(taxon_info, ignore_index=True)
        taxon_info_df.drop(
            columns=[
                "linNames",
                "linIds",
                "linRanks",
                "iri",
                "alternativeNames",
                "lastCol",
            ],
            inplace=True,
        )
        taxon_info_df.rename(
            columns={
                "queryId": "src_iri",
                "queryName": "verbatim",
                "matchType": "match_type",
                "matchId": "tgt_iri",
                "matchName": "scientific_name",
                "matchRank": "rank",
            },
            inplace=True,
        )
        taxon_info_df.drop_duplicates(inplace=True)
        taxon_info_df["src_iri"] = taxon_info_df["src_iri"].apply(
            lambda x: get_full_iri(x)
        )
        taxon_info_df["tgt_iri"] = taxon_info_df["tgt_iri"].apply(
            lambda x: get_full_iri(x)
        )
        # print(taxon_info_df)
        return df, taxon_info_df
