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

    def id_to_target_id(self, df, id_column, matcher):
        tgt_ids = pd.Series()
        query = self.df_to_query(
            df=df.drop_duplicates(subset=id_column), id_column=id_column
        )
        response = self.run_nomer_container(query, matcher=matcher)
        res_df = pd.read_csv(StringIO(response.decode("utf-8")), sep="\t", header=None)
        res_df = res_df.dropna(axis=0, subset=[3])
        if not res_df.empty:
            matched = res_df[
                res_df[3].str.startswith(self.target_taxonomy)
            ]  # Grep NCBI:
            for index, row in df.iterrows():
                loc = matched[0].isin([row[id_column]])
                if loc.any():
                    tgt_ids.at[index] = matched[loc][3].iloc[0]
        return tgt_ids

    def name_to_target_id(self, df, name_column, matcher):
        tgt_ids = pd.Series()
        query = self.df_to_query(
            df=df.drop_duplicates(subset=name_column), name_column=name_column
        )
        response = self.run_nomer_container(query, matcher=matcher)
        res_df = pd.read_csv(StringIO(response.decode("utf-8")), sep="\t", header=None)
        res_df = res_df.dropna(axis=0, subset=[3])
        if not res_df.empty:
            matched = res_df[
                res_df[3].str.startswith(self.target_taxonomy)
            ]  # Grep NCBI:
            for index, row in df.iterrows():
                loc = matched[1].isin([row[name_column]])
                if loc.any():
                    tgt_ids.at[index] = matched[loc][3].iloc[0]
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
                df_not_found, name_column, matcher=self.cache_matcher
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
                    df_not_found, name_column, matcher=self.enrich_matcher
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
