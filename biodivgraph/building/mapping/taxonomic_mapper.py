import logging
import pandas as pd
from ...core import URIMapper, URIManager, TaxId
import os
import docker
import re
from docker.errors import NotFound, APIError
from pynomer.client import NomerClient
from io import StringIO
import sys
import numpy as np
from pandas.errors import EmptyDataError

"""
https://github.com/globalbioticinteractions/globalbioticinteractions/wiki/Taxonomy-Matching
"""


class TaxonomicEntityMapper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.uri_mapper = URIMapper()
        self.uri_manager = URIManager()
        self.config = config

        self.default_taxonomy = "GBIF"
        self.source_taxonomy = (
            self.config.source_taxonomy if "source_taxonomy" in self.config else None
        )

        if self.source_taxonomy and not self.uri_mapper.is_valid_db_prefix(
            self.source_taxonomy + ":"
        ):
            raise ValueError(
                "Fatal error : invalid source taxonomy {}".format(self.source_taxonomy)
            )

        self.target_taxonomy = self.config.target_taxonomy
        if not self.uri_mapper.is_valid_db_prefix(self.target_taxonomy + ":"):
            self.logger.error(
                "Invalid target taxonomy {} : use default taxonomy {}".format(
                    self.target_taxonomy, self.default_taxonomy
                )
            )
            self.target_taxonomy = self.default_taxonomy

        self.cache_matcher = "globi-taxon-cache"
        self.enrich_matcher = "globi-globalnames"  # "globi-enrich"
        self.scrubbing_matcher = "globi-correct"
        self.wikidata_id_matcher = "wikidata-taxon-id-web"

        self.client = docker.from_env()
        self.nomer_image = self.client.images.get("pynomer:latest")
        self.nomer_cache_dir = os.getenv("NOMER_DIR")

    def run_nomer_container(self, query, matcher):
        volume = {
            os.path.abspath(self.nomer_cache_dir): {"bind": "/.nomer", "mode": "rw"},
        }
        self.logger.debug(f"run_nomer_container : mount volume {volume}")

        append_command = f"pynomer append {query} -m {matcher} -e"
        self.logger.debug(f"run_nomer_container : command {append_command}")

        return self.client.containers.run(
            self.nomer_image,
            append_command,
            volumes=volume,
            remove=False,
        )

    def parse_nomer_response(self, response):
        try:
            res_df = pd.read_csv(
                StringIO(response.decode("utf-8")), sep="\t", header=None
            )
        except EmptyDataError as e:
            self.logger.error(e)
            return False, None
        else:
            return True, res_df

    def ask_nomer(self, query, matcher):
        while True:
            response = self.run_nomer_container(query, matcher=matcher)
            not_empty, res_df = self.parse_nomer_response(response)
            if not_empty:
                return res_df
            else:
                self.logger.debug(f"Nomer raised EmptyDataError : try again.")

    def get_best_match(self, src_taxid, src_name, df):
        if df.shape[0] == 1:
            return df[3].iloc[0]
        else:
            self.logger.debug(
                f"Found multiple matches for taxon {src_name}: get best match"
            )
            query = f'"{src_taxid}\\t"'  # f"{src_taxid}\t"
            # Get lineage from source taxid using cache matcher
            res_df = self.ask_nomer(query, matcher=self.cache_matcher)
            # print(res_df, res_df[2].iloc[0])
            if (
                res_df[2].iloc[0] != "SAME_AS"
            ):  # If not found, try again using enrich matcher
                res_df = self.ask_nomer(query, matcher="globi-enrich")
            # print(res_df, res_df[2].iloc[0])
            if res_df[2].iloc[0] == "SAME_AS":
                src_lineage = res_df[7].iloc[0].split(" | ")
            else:
                self.logger.debug(
                    f"Could not get lineage from taxid {src_taxid}: cannot find best match"
                )
                return None

            tgt_lineages = [
                {
                    "index": index,
                    "lineage": [x.strip(" ") for x in row[7].split("|") if x],
                }
                for index, row in df.iterrows()
            ]

            if len(set([",".join(tgt["lineage"]) for tgt in tgt_lineages])) == 1:
                best_index = tgt_lineages[0]["index"]
                best_match = df[3].loc[best_index]
                self.logger.debug(
                    f"Found multiple matches with same lineage for taxon {src_name}: {best_match}"
                )
            else:
                similarities = [
                    1.0
                    * len(set(src_lineage).intersection(tgt["lineage"]))
                    / len(set(src_lineage))
                    for tgt in tgt_lineages
                ]
                # print(tgt_lineages, similarities)
                max_sim = max(similarities)
                max_indexes = [
                    i for i, sim in enumerate(similarities) if sim == max_sim
                ]
                if len(max_indexes) > 1:
                    self.logger.debug(
                        f"Found multiple best matches with similarity {max_sim:.2f}: cannot find best match"
                    )
                    return None

                best_index = tgt_lineages[max_indexes[0]]["index"]
                best_match = df[3].loc[best_index]
                self.logger.debug(
                    f"Found best match with similarity {max_sim:.2f} for taxon {src_name}: {best_match}"
                )
            return best_match

    def id_to_target_id(self, df, id_column, matcher):
        tgt_ids = pd.Series()
        query = self.df_to_query(
            df=df.drop_duplicates(subset=id_column), id_column=id_column
        )
        res_df = self.ask_nomer(query, matcher=matcher)
        res_df = res_df.dropna(axis=0, subset=[3])
        if not res_df.empty:
            matched = res_df[
                res_df[3].str.startswith(self.target_taxonomy)
            ]  # Grep "NCBI:"
            for index, row in df.iterrows():
                loc = matched[0].isin([row[id_column]])
                if loc.any():
                    tgt_ids.at[index] = matched[loc][3].iloc[0]
        return tgt_ids

    def name_to_target_id(self, df, name_column, id_column, matcher):
        tgt_ids = pd.Series()
        query = self.df_to_query(
            df=df.drop_duplicates(subset=name_column), name_column=name_column
        )
        res_df = self.ask_nomer(query, matcher=matcher)
        res_df = res_df.dropna(axis=0, subset=[3])

        name_id_map = {}
        if not res_df.empty:
            matched = res_df[
                res_df[3].str.startswith(self.target_taxonomy)
            ]  # Grep "NCBI:"

            df_names = df.drop_duplicates(subset=name_column)
            for index, row in df_names.iterrows():
                loc = matched[1].isin([row[name_column]])
                if loc.any():
                    best_match = self.get_best_match(
                        src_taxid=row[id_column],
                        src_name=row[name_column],
                        df=matched[loc].drop_duplicates(),
                    )
                    if best_match:
                        name_id_map[row[name_column]] = best_match

            for index, row in df.iterrows():
                name = row[name_column]
                if name in name_id_map:
                    tgt_ids.at[index] = name_id_map[name]
                # loc = matched[1].isin(
                #     [row[name_column]]
                # )  # Find all matches for a taxon name
                # if loc.any():
                #     best_match = self.get_best_match(
                #         src_taxid=row[id_column],
                #         src_name=row[name_column],
                #         df=matched[loc].drop_duplicates(),
                #     )
                #     if best_match:
                #         tgt_ids.at[index] = best_match
                # tgt_ids.at[index] = matched[loc][3].iloc[0]
        return tgt_ids

    def df_to_query(self, df, id_column=None, name_column=None):
        query_df = pd.DataFrame()
        query_df["id"] = df[id_column] if id_column is not None else np.nan
        query_df["name"] = df[name_column] if name_column is not None else np.nan
        query = query_df.to_csv(sep="\t", header=False, index=False)
        query = query.strip("\n")
        query = query.replace("\t", "\\t")
        query = query.replace("\n", "\\n")
        return f'"{query}"'

    def map(self, df, id_column, name_column):
        tgt_id_series = pd.Series(np.nan, index=range(df.shape[0]))

        self.logger.debug(
            f"Map ids from {id_column} column to target taxo {self.target_taxonomy}"
        )

        if self.source_taxonomy:
            df[id_column] = df.apply(
                lambda row: f"{self.source_taxonomy}:" + str(row[id_column])
                if not str(row[id_column]).startswith(self.source_taxonomy)
                else row[id_column],
                axis=1,
            )

        tgt_ids = self.id_to_target_id(df, id_column, matcher=self.wikidata_id_matcher)
        found = df.index.isin(tgt_ids.index)
        self.logger.debug(
            f"Found {found.sum()}/{df.shape[0]} ids in target taxo {self.target_taxonomy}"
        )
        df_not_found = df[~found]
        tgt_id_series.loc[tgt_ids.index] = tgt_ids

        if not df_not_found.empty:
            self.logger.debug(
                f"Map names from {name_column} column to target taxo {self.target_taxonomy} using {self.cache_matcher}"
            )
            tgt_ids = self.name_to_target_id(
                df_not_found, name_column, id_column, matcher=self.cache_matcher
            )
            found = df_not_found.index.isin(tgt_ids.index)
            self.logger.debug(
                f"Found {found.sum()}/{df_not_found.shape[0]} ids in target taxo {self.target_taxonomy}"
            )
            df_not_found = df_not_found[~found]
            tgt_id_series.loc[tgt_ids.index] = tgt_ids

            if not df_not_found.empty:
                self.logger.debug(
                    f"Map names from {name_column} column to target taxo {self.target_taxonomy} using {self.enrich_matcher}"
                )
                tgt_ids = self.name_to_target_id(
                    df_not_found, name_column, id_column, matcher=self.enrich_matcher
                )
                found = df_not_found.index.isin(tgt_ids.index)
                self.logger.debug(
                    f"Found {found.sum()}/{df_not_found.shape[0]} ids in target taxo {self.target_taxonomy}"
                )
                df_not_found = df_not_found[~found]
                tgt_id_series.loc[tgt_ids.index] = tgt_ids
        return tgt_id_series

    # def scrub_taxname(self, name):
    #     name = name.split(" sp. ")[0]
    #     name = name.split(" ssp. ")[0]
    #     name = name.strip()
    #     return " ".join(name.split())

    # def parse_entry(self, entry):
    #     if entry[2] == "NONE":
    #         return None, None, None
    #     taxonomy = entry[3].split(":")[0]
    #     uri = entry[-1]
    #     taxid = entry[3]
    #     return taxid, taxonomy, uri


class TaxonomicMapper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.config = config
        # for column_config in self.config.columns:
        #     column_config.run_on_localhost = (
        #         self.config.run_on_localhost
        #         if "run_on_localhost" in self.config
        #         else False
        #     )

    def map(self, df):

        for column_config in self.config.columns:
            self.logger.info(
                f"Map {df.shape[0]} taxons from columns ({column_config.id_column},{column_config.name_column}) to target taxo {column_config.target_taxonomy}"
            )
            mapper = TaxonomicEntityMapper(column_config)

            src_id_series = df[column_config.id_column]

            tgt_id_series = mapper.map(
                df,
                id_column=column_config.id_column,
                name_column=column_config.name_column,
            )

            nb_found = tgt_id_series.count()
            self.logger.info(
                f"Found {nb_found}/{df.shape[0]} ids in target taxo {column_config.target_taxonomy}"
            )

            # ids_to_uri
            tgt_id_series = tgt_id_series.apply(
                lambda id: "http://purl.obolibrary.org/obo/NCBITaxon_{}".format(
                    id.strip("NCBI:")
                )
                if pd.notnull(id)
                else id
            )

            df[column_config.uri_column] = tgt_id_series
            df[column_config.id_column] = src_id_series

        uri_colnames = [
            column_config.uri_column for column_config in self.config.columns
        ]
        return df, uri_colnames
