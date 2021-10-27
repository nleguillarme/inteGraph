import logging
import yaml
import os
import docker
import re
import sys
import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError
from docker.errors import NotFound, APIError
from io import StringIO

# from pynomer.client import NomerClient
from ..core import URIMapper, URIManager, TaxId


"""
https://github.com/globalbioticinteractions/globalbioticinteractions/wiki/Taxonomy-Matching
"""


class NoValidColumnException(Exception):
    pass


class ConfigurationError(Exception):
    pass


class NomerHelper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.client = docker.from_env()
        self.nomer_image = self.client.images.get("nomer:latest")
        self.nomer_cache_dir = os.getenv("INTEGRAPH__CONFIG__NOMER_CACHE_DIR")
        self.columns = [
            "queryId",
            "queryName",
            "matchType",
            "matchId",
            "matchName",
            "matchRank",
            "alternativeNames",
            "linNames",
            "linIds",
            "linRanks",
            "iri",
            "lastCol",
        ]

    def ask_nomer(self, query, matcher):
        """Send query to nomer and parse nomer response.

        Returns a DataFrame.
        """
        while True:
            response = self.run_nomer_container(query, matcher=matcher)
            not_empty, res_df = self.parse_nomer_response(response)
            if not_empty:
                return res_df
            else:
                self.logger.debug(f"Nomer raised EmptyDataError : try again.")

    def run_nomer_container(self, query, matcher):
        """Run pynomer append command in Docker container.

        See pynomer documentation : https://github.com/nleguillarme/pynomer.
        """
        volume = {
            os.path.abspath(self.nomer_cache_dir): {"bind": "/.nomer", "mode": "rw"},
        }
        self.logger.debug(f"run_nomer_container : mount volume {volume}")

        append_command = f"'echo {query} | nomer append {matcher}'"
        # self.logger.debug(f"run_nomer_container : command {append_command}")

        return self.client.containers.run(
            self.nomer_image,
            append_command,
            volumes=volume,
            remove=False,
        )

    def parse_nomer_response(self, response):
        """Transform nomer response into a pandas DataFrame.

        Nomer response format is (normally) a valid TSV string.
        """
        try:
            res_df = pd.read_csv(
                StringIO(response.decode("utf-8")), sep="\t", header=None
            )
            res_df.columns = self.columns
            # res_df.drop(columns=["alternativeNames", "lastCol"], inplace=True)
        except EmptyDataError as e:
            self.logger.error(e)
            return False, None
        else:
            return True, res_df

    def df_to_query(self, df, id_column=None, name_column=None):
        """Convert a DataFrame into a valid nomer query.

        Nomer can be asked about (a list of) names and/or taxids.
        """
        query_df = pd.DataFrame()
        query_df["id"] = df[id_column] if id_column is not None else np.nan
        query_df["name"] = df[name_column] if name_column is not None else np.nan
        query = query_df.to_csv(sep="\t", header=False, index=False)
        query = query.strip("\n")
        query = query.replace("\t", "\\t")
        query = query.replace("\n", "\\n")
        return f'"{query}"'


class TaxonomicEntityMapper:
    def __init__(self, prefix, config):
        self.logger = logging.getLogger(__name__)
        self.uri_mapper = URIMapper()
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
        self.taxo_to_matcher = {"GBIF": "gbif-taxon-id", "NCBI": "ncbi-taxon"}

    def format_id_column(self, df, id_column, taxonomy):
        # def format_id(id):
        #     if pd.isnull(id):
        #         return id
        #     return str(int(id)) if str(id).replace(".", "", 1).isdigit() else id
        return df.apply(
            lambda row: f"{self.source_taxonomy}:" + row[id_column]
            if (
                pd.notnull(row[id_column])
                and not row[id_column].startswith(self.source_taxonomy + ":")
            )
            else row[id_column],
            axis=1,
        )

    def validate(self, df, id_column=None, name_column=None):

        # Drop duplicates for efficiency
        subset = id_column if id_column else name_column
        df_copy = df.drop_duplicates(subset=subset)
        if id_column and self.source_taxonomy:
            df_copy[id_column] = self.format_id_column(
                df_copy, id_column, self.source_taxonomy
            )

        if id_column:
            # Create nomer query
            query = self.nomer.df_to_query(
                df=df_copy,
                id_column=id_column,
                name_column=name_column,
            )
            res_df = self.nomer.ask_nomer(
                query, matcher=self.taxo_to_matcher[self.source_taxonomy]
            )
            res_df.set_index(
                pd.Index(res_df["queryId"]).rename("externalId"),
                drop=False,
                inplace=True,
            )
            mask = res_df["matchType"].isin(["SAME_AS", "SYNONYM_OF"])
            valid_df = res_df[mask]
            invalid_df = res_df[~mask]

        else:
            valid_df = pd.DataFrame(
                index=pd.Index(df_copy[name_column]).rename("externalId"),
                columns=self.nomer.columns,
            )
            # valid_df = valid_df.fill_na()
            valid_df["queryName"] = df_copy[name_column].to_list()
            valid_df["matchType"] = "SAME_AS"
            valid_df["matchName"] = df_copy[name_column].to_list()
            invalid_df = None

        return valid_df, invalid_df

    def source_id_to_target_id(
        self, df, id_column, name_column, matcher="wikidata-taxon-id-web"
    ):
        """Ask nomer about a batch of taxon IDs."""

        df_copy = df.drop_duplicates(subset=id_column)

        query_to_ref_index_map = {}
        for index, row in df_copy.iterrows():
            key = row[id_column]
            if key in query_to_ref_index_map:
                raise ValueError(f"Key {key} already in map")
            query_to_ref_index_map[key] = index

        query = self.nomer.df_to_query(
            df=df_copy,
            id_column=id_column,
            name_column=name_column,
        )

        res_df = self.nomer.ask_nomer(query, matcher=matcher)

        res_df = res_df.dropna(axis=0, subset=["matchId"])
        res_df = res_df[res_df["matchType"].isin(["SAME_AS", "SYNONYM_OF"])]

        mapped_df = res_df[res_df["matchId"].str.startswith(self.target_taxonomy)]

        duplicated = mapped_df.duplicated(subset=["queryId"], keep=False)
        if duplicated.any():
            self.logger.error(
                f"Found several target IDs for a single taxa in df: {mapped_df[duplicated]}"
            )
            raise Exception("Unable to handle multiple candidates at id level")

        mapped_index = mapped_df["queryId"].map(query_to_ref_index_map)
        mapped_df.set_index(
            pd.Index(mapped_index.tolist()).rename("externalId"),
            inplace=True,
        )

        others_df = df_copy[~df_copy[id_column].isin(mapped_df["queryId"])]

        return mapped_df, others_df

    def source_name_to_target_id(
        self, df, id_column=None, name_column=None, matcher="ncbi-taxon"
    ):
        """Ask nomer about a batch of taxon names.

        When there are several taxa matching with the same name, this function
        tries to get the best match using a maximum lineage similarity approach.
        """

        # df_copy = df.drop_duplicates(subset=[id_column, name_column])
        ref_col = id_column if id_column else name_column

        query_to_ref_index_map = {}
        for index, row in df.iterrows():
            key = row[ref_col]
            if key in query_to_ref_index_map:
                raise ValueError(f"Key {key} already in map")
            query_to_ref_index_map[key] = index

        query = self.nomer.df_to_query(
            df=df,
            name_column=name_column,
            id_column=id_column,
        )
        res_df = self.nomer.ask_nomer(query, matcher=matcher)
        res_df = res_df.dropna(axis=0, subset=["matchId"])
        res_df = res_df[res_df["matchType"].isin(["SAME_AS", "SYNONYM_OF"])]

        if not res_df.empty:
            mapped_df = res_df[res_df["matchId"].str.startswith(self.target_taxonomy)]

            subset = "queryId" if id_column else "queryName"
            duplicated = mapped_df.duplicated(subset=[subset], keep=False)
            if duplicated.any():
                self.logger.info(
                    f"Found several target entities for a single taxa in df :\n{mapped_df.loc[duplicated]}"
                )
                if id_column:
                    keep_index = self.resolve_duplicates(
                        df, mapped_df[duplicated], id_column, query_to_ref_index_map
                    )
                    for index in keep_index:
                        duplicated.loc[index] = False
                else:
                    self.logger.info(
                        "Cannot resolve duplicates without a valid id column : discard the taxon"
                    )

            mapped_df = mapped_df[~duplicated]
            mapped_index = mapped_df[subset].map(query_to_ref_index_map)

            mapped_df.set_index(
                pd.Index(mapped_index.tolist()).rename("externalId"),
                inplace=True,
            )

            others_df = df[~df[ref_col].isin(mapped_df[subset])]
        else:
            mapped_df = res_df
            others_df = df

        return mapped_df, others_df

    def resolve_duplicates(self, ref_df, duplicates, id_column, query_to_ref_index_map):
        unique_duplicates = duplicates["queryId"].unique()
        keep_index = []
        self.logger.debug(f"Unique duplicates {unique_duplicates}")
        for id in unique_duplicates:
            candidates = duplicates[duplicates["queryId"] == id]
            if id not in query_to_ref_index_map:
                raise KeyError(f"Candidate id {id} has no match")
            ref_index = query_to_ref_index_map[id]
            reference = ref_df.loc[ref_index, :]  # [ref_df[ref_df.columns[0]] == id]
            self.logger.info(f"Get best match for taxon\n{reference}")
            best_index = self.get_best_match(
                reference,
                candidates,
            )
            keep_index.append(best_index)
        return keep_index

    def get_best_match(self, reference, candidates):
        """Given nomer response for a given taxon, return the best match.

        If only one match, returns the candidate IRI
        If more than one match, get the full lineage of the query taxon from its taxid
            For each candidate
                Compute the similarity between the query taxon lineage and the candidate lineage
                Similarity = the length of the intersection of the two lineages
            Return best match = the IRI of the candidate with the maximum similarity

        """
        ref_lineage = [x.strip(" ") for x in reference["linNames"].split("|") if x]

        candidate_lineages = [
            {
                "index": index,
                "lineage": [x.strip(" ") for x in row["linNames"].split("|") if x],
            }
            for index, row in candidates.iterrows()
        ]

        if len(set([",".join(tgt["lineage"]) for tgt in candidate_lineages])) == 1:
            best_index = candidate_lineages[0]["index"]
            best_match = candidates.loc[best_index]
            best_match_id = best_match["matchId"]
            self.logger.debug(
                f"Found multiple matches with similar lineages, return the first one : {best_match_id}"
            )
        else:
            similarities = [
                1.0
                * len(set(ref_lineage).intersection(tgt["lineage"]))
                / len(set(ref_lineage))
                for tgt in candidate_lineages
            ]

            # print(tgt_lineages, similarities)
            max_sim = max(similarities)
            max_indexes = [i for i, sim in enumerate(similarities) if sim == max_sim]
            if len(max_indexes) > 1:
                self.logger.debug(
                    f"Found multiple best matches with similarity {max_sim:.2f}: cannot find best match"
                )
                return None

            best_index = candidate_lineages[max_indexes[0]]["index"]
            best_match = candidates.loc[best_index]
            best_match_id = best_match["matchId"]
            self.logger.debug(
                f"Found best match with similarity {max_sim:.2f} : {best_match_id}"
            )
        return best_index

    # def source_to_target_id(self, df, id_column, matcher):
    #     """Ask nomer about a batch of taxon IDs."""
    #
    #     tgt_ids = pd.DataFrame(columns=["canonical_name", self.target_taxonomy])
    #
    #
    #
    #     query = self.df_to_query(
    #         df=df.drop_duplicates(subset=id_column), id_column=id_column
    #     )
    #     res_df = self.ask_nomer(query, matcher=matcher)
    #     res_df = res_df.dropna(axis=0, subset=[3])
    #     res_df = res_df[res_df[2] != "NONE"]
    #     if not res_df.empty:
    #         for index, row in df.iterrows():
    #             loc = res_df[0].isin([row[id_column]])
    #             if loc.any():
    #                 matched = res_df[loc]
    #                 res = {"canonical_name": matched[4].mode().iloc[0]}
    #                 tgt_taxo_match = matched[
    #                     matched[3].str.startswith(self.target_taxonomy)
    #                 ]
    #                 res[self.target_taxonomy] = (
    #                     None if tgt_taxo_match.empty else tgt_taxo_match.iloc[0][3]
    #                 )
    #                 tgt_ids.at[index] = res
    #     return tgt_ids

    # def get_best_match(self, src_taxid, src_name, df):
    #     """Given nomer response for a given taxon, return the best match.
    #
    #     If only one match, returns the candidate IRI
    #     If more than one match, get the full lineage of the query taxon from its taxid
    #         For each candidate
    #             Compute the similarity between the query taxon lineage and the candidate lineage
    #             Similarity = the length of the intersection of the two lineages
    #         Return best match = the IRI of the candidate with the maximum similarity
    #
    #     """
    #     if df.shape[0] == 1:
    #         return [
    #             {"canonical_name": df[4].iloc[0], self.target_taxonomy: df[3].iloc[0]}
    #         ]
    #     else:
    #         self.logger.debug(
    #             f"Found multiple matches for taxon {src_name}: get best match"
    #         )
    #         query = f'"{src_taxid}\\t"'  # f"{src_taxid}\t"
    #         # Get lineage from source taxid using cache matcher
    #         res_df = self.ask_nomer(query, matcher=self.cache_matcher)
    #         if res_df[2].iloc[0] not in [
    #             "SAME_AS",
    #             "SYNONYM_OF",
    #         ]:  # If not found, try again using enrich matcher
    #             res_df = self.ask_nomer(query, matcher="globi-enrich")
    #         if res_df[2].iloc[0] in [
    #             "SAME_AS",
    #             "SYNONYM_OF",
    #         ]:
    #             src_lineage = res_df[7].iloc[0].split(" | ")
    #         else:
    #             self.logger.debug(
    #                 f"Could not get lineage from taxid {src_taxid}: cannot find best match"
    #             )
    #             return None
    #
    #         tgt_lineages = [
    #             {
    #                 "index": index,
    #                 "lineage": [x.strip(" ") for x in row[7].split("|") if x],
    #             }
    #             for index, row in df.iterrows()
    #         ]
    #
    #         if len(set([",".join(tgt["lineage"]) for tgt in tgt_lineages])) == 1:
    #             best_index = tgt_lineages[0]["index"]
    #             best_match = df[3].loc[best_index]
    #             self.logger.debug(
    #                 f"Found multiple matches with same lineage for taxon {src_name}: {best_match}"
    #             )
    #         else:
    #             similarities = [
    #                 1.0
    #                 * len(set(src_lineage).intersection(tgt["lineage"]))
    #                 / len(set(src_lineage))
    #                 for tgt in tgt_lineages
    #             ]
    #             # print(tgt_lineages, similarities)
    #             max_sim = max(similarities)
    #             max_indexes = [{name_column} column
    #                 i for i, sim in enumerate(similarities) if sim == max_sim
    #             ]
    #             if len(max_indexes) > 1:
    #                 self.logger.debug(
    #                     f"Found multiple best matches with similarity {max_sim:.2f}: cannot find best match"
    #                 )
    #                 return [
    #                     {
    #                         "canonical_name": df[4].loc[tgt_lineages[index]["index"]],
    #                         self.target_taxonomy: df[3].loc[
    #                             tgt_lineages[index]["index"]
    #                         ],
    #                     }
    #                     for index in max_indexes
    #                 ]  # None
    #
    #             best_index = tgt_lineages[max_indexes[0]]["index"]
    #             best_match = {
    #                 "canonical_name": df[4].loc[best_index],
    #                 self.target_taxonomy: df[3].loc[best_index],
    #             }
    #             self.logger.debug(
    #                 f"Found best match with similarity {max_sim:.2f} for taxon {src_name}: {best_match}"
    #             )
    #         return [best_match]

    # def name_to_target_id(self, df, name_column, id_column, matcher):
    #     """Ask nomer about a batch of taxon names.
    #
    #     When there are several taxa matching with the same name, this function
    #     tries to get the best match using a maximum lineage similarity approach.
    #     """
    #     # tgt_ids = pd.Series()
    #     tgt_ids = pd.DataFrame(columns=["canonical_name", self.target_taxonomy])
    #     query = self.df_to_query(
    #         df=df.drop_duplicates(subset=name_column), name_column=name_column
    #     )
    #     res_df = self.ask_nomer(query, matcher=matcher)
    #     res_df = res_df.dropna(axis=0, subset=[3])
    #     res_df = res_df[res_df[2] != "NONE"]
    #
    #     name_id_map = {}
    #     if not res_df.empty:
    #         df_names = df.drop_duplicates(subset=name_column)
    #
    #         for index, row in df_names.iterrows():
    #
    #             loc = res_df[1].isin([row[name_column]])
    #             if loc.any():
    #
    #                 tax_res_df = res_df[loc]
    #                 matched = tax_res_df[
    #                     tax_res_df[3].str.startswith(self.target_taxonomy)
    #                 ]
    #
    #                 if not matched.empty:
    #
    #                     best_match = self.get_best_match(
    #                         src_taxid=row[id_column],
    #                         src_name=row[name_column],
    #                         df=matched.drop_duplicates(),
    #                     )
    #
    #                     if best_match:
    #                         name_id_map[row[name_column]] = best_match[0]
    #
    #         for index, row in df.iterrows():
    #             name = row[name_column]
    #             if name in name_id_map:
    #                 tgt_ids.at[index] = name_id_map[name]
    #
    #         print(tgt_ids)
    #
    #     return tgt_ids

    def map(self, df, id_column, name_column):
        """Using nomer taxonomic mapping capabilities, try to get IRIs in a given
        target_taxonomy from taxon names and/or taxids.

        First, try to map taxon ids into the target taxonomy using wikidata-id-matcher
        For each taxon with no match
            Try to map the taxon name using globi-taxon-cache
            For each taxon with no match
                Try to map the taxon name using ncbi-taxon
        """

        subset = [x for x in [id_column, name_column] if x]
        w_df = df.dropna(subset=subset).drop_duplicates(subset=subset)

        # Step 1 : validate taxa against the source taxonomy
        self.logger.debug(f"Validate taxa using info from columns {subset}")
        valid_df, invalid_df = self.validate(w_df, id_column, name_column)
        self.logger.debug(f"Found {valid_df.shape[0]}/{w_df.shape[0]} valid taxa")
        # src_to_src_mappings is a dataframe containing mappings between ids
        # in the same source taxonomy. Indeed, a taxon can have several ids
        # in a single taxonomy, and the id used in the data may not be the preferred
        # id for this taxon.
        src_to_src_mappings = valid_df[
            (pd.notnull(valid_df["matchId"]))
            & (valid_df["queryId"] != valid_df["matchId"])
        ]

        # print(valid_df)
        mapped = []

        # Step 2 : map valid taxa using their id in the source taxonomy (if available)
        if id_column:
            self.logger.debug(
                f"Map {valid_df.shape[0]} unique taxa to target taxonomy {self.target_taxonomy} using wikidata-taxon-id-web"
            )
            mapped_df, others_df = self.source_id_to_target_id(
                valid_df,
                id_column="matchId",
                name_column="matchName",
            )
            self.logger.debug(
                f"Found {mapped_df.shape[0]}/{valid_df.shape[0]} taxa in target taxonomy {self.target_taxonomy}"
            )
            mapped.append(mapped_df)
        else:
            others_df = valid_df

        # Step 3 : map the remaining taxa using their name (if available)
        if not others_df.empty and name_column:
            nb_taxa = others_df.shape[0]
            self.logger.debug(
                f"Map {nb_taxa} remaining taxa to target taxonomy {self.target_taxonomy} using ncbi-taxon"
            )
            mapped_df, others_df = self.source_name_to_target_id(
                others_df,
                id_column="matchId" if id_column else None,
                name_column="matchName",
                matcher="ncbi-taxon",
            )
            self.logger.debug(
                f"Found {mapped_df.shape[0]}/{nb_taxa} taxa in target taxonomy {self.target_taxonomy}"
            )
            mapped.append(mapped_df)

        if not others_df.empty and name_column:
            nb_taxa = others_df.shape[0]
            self.logger.debug(
                f"Map {nb_taxa} remaining taxa to target taxonomy {self.target_taxonomy} using globi-globalnames"
            )
            mapped_df, others_df = self.source_name_to_target_id(
                others_df,
                id_column="matchId" if id_column else None,
                name_column="queryName",  # TODO : change for matchName
                matcher="globi-globalnames",
            )
            self.logger.debug(
                f"Found {mapped_df.shape[0]}/{nb_taxa} taxa in target taxonomy {self.target_taxonomy}"
            )
            mapped.append(mapped_df)

        mapped_df = pd.concat(mapped, ignore_index=False)
        self.logger.info(
            f"Found {mapped_df.shape[0]}/{valid_df.shape[0]} valid taxa in target taxonomy {self.target_taxonomy}"
        )

        if not id_column:
            mapped_df["queryId"] = mapped_df["matchId"]
            valid_df = mapped_df

        # Create a taxon-iri map and create the Series containing IRIs
        ref_col = id_column if id_column else name_column
        subset = "queryId" if id_column else "matchId"

        if id_column and self.source_taxonomy:
            df[id_column] = self.format_id_column(df, id_column, self.source_taxonomy)

        src_tgt_map = {}
        for external_id in df[ref_col].unique():
            valid_id = (
                valid_df[subset][external_id] if external_id in valid_df.index else None
            )
            src_tgt_map[external_id] = valid_id
        valid_id_series = df[ref_col].map(src_tgt_map)

        return valid_id_series, pd.concat(
            [mapped_df, src_to_src_mappings], ignore_index=True
        )

    # def scrub_taxname(self, name):
    #     name = name.splname_columnit(" sp. ")[0]
    #     name = name.split(" ssp. ")[0]
    #     name = name.strip()
    #     return " ".join(name.split())


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
