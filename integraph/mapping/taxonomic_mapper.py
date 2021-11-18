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
from ..core import URIMapper, URIManager, TaxId


"""
https://github.com/globalbioticinteractions/globalbioticinteractions/wiki/Taxonomy-Matching
"""


class NoValidColumnException(Exception):
    pass


class ConfigurationError(Exception):
    pass


class GNParserHelper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.client = docker.from_env()
        self.gnparser_image = self.client.images.get("gnames/gognparser:latest")

    def get_canonical_name(self, f_in):

        with open(f_in, "r") as f:
            print(f.read())

        volume = {
            f_in: {"bind": f_in},
        }

        response = self.client.containers.run(
            self.gnparser_image,
            f"-j 20 {f_in}",
            volumes=volume,
            remove=True,
        )
        try:
            print(response.decode("utf-8"))
            res_df = pd.read_csv(StringIO(response.decode("utf-8")), sep=",")
            print(res_df)
        except EmptyDataError as e:
            self.logger.error(e)
            return False, None
        else:
            return True, res_df


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
        query = query.replace("'", " ")
        return f'"{query}"'


class TaxonomicEntityMapper:
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
        self.taxo_to_matcher = {"GBIF": "gbif", "NCBI": "ncbi"}
        self.gnparser = GNParserHelper()

    def format_id_column(self, df, id_column, taxonomy):
        """
        Format taxon identifier as DB:XXXX (e.g. NCBI:6271, GBIF:234581...).
        """

        return df.apply(
            lambda row: f"{self.source_taxonomy}:" + row[id_column]
            if (
                pd.notnull(row[id_column])
                and not row[id_column].startswith(self.source_taxonomy + ":")
            )
            else row[id_column],
            axis=1,
        )

    def validate(self, df, id_column=None, name_column=None, matcher=None):
        """
        Match taxon from the source taxonomy to the source taxonomy
        (e.g.GBIF to GBIF) to filter out taxa with invalid ids.
        """

        # Drop duplicates for efficiency
        subset = id_column if id_column else name_column
        df_copy = df.copy().drop_duplicates(subset=subset)

        # For NCBI and GBIF, the id must be an integer

        if id_column:
            valid_df = df_copy[df_copy[id_column].str.isnumeric()]

            if self.source_taxonomy:  # Format taxon id
                valid_df[id_column] = valid_df[id_column].map(
                    lambda id: f"{self.source_taxonomy}:{id}"
                    if (
                        pd.notnull(id)
                        and not str(id).startswith(self.source_taxonomy + ":")
                    )
                    else id
                )

            query = self.nomer.df_to_query(
                df=valid_df,
                id_column=id_column,
                name_column=name_column,
            )
            res_df = self.nomer.ask_nomer(query, matcher=matcher)
            res_df.set_index(
                pd.Index(res_df["queryId"]).rename("externalId"),
                drop=False,
                inplace=True,
            )
            mask = res_df["matchType"].isin(["SAME_AS", "SYNONYM_OF"])
            valid_df = res_df[mask]

            # invalid_df = res_df[~mask]

        else:  # Validate names (all names are considered valid)
            f_temp = NamedTemporaryFile(delete=True)  # False)
            self.logger.debug(
                f"Write names to {f_temp.name} for validation using gnparser"
            )
            names = [name.replace("\n", " ") for name in df_copy[name_column].to_list()]
            names_str = "\n".join(names)
            f_temp.write(names_str.encode())
            f_temp.read()  # I don't know why but it is needed or sometimes the file appears empty when reading
            success, canonical_names = self.gnparser.get_canonical_name(f_temp.name)
            f_temp.close()

            valid_df = pd.DataFrame(
                columns=self.nomer.columns,
            )
            valid_df["queryName"] = df_copy[name_column].to_list()
            valid_df["matchType"] = "SAME_AS"
            valid_df["matchName"] = valid_df["queryName"]
            if success:
                valid_df["matchName"] = canonical_names["CanonicalSimple"]
            # valid_df.to_csv("valid_name.csv")
            valid_df = valid_df.set_index("queryName", drop=False)
            valid_df = valid_df.set_index(valid_df.index.rename("externalId"))
            valid_df = valid_df.drop_duplicates(subset="matchName")

        return valid_df  # , invalid_df

    def source_id_to_target_id(self, df, id_column, name_column=None, matcher=None):
        """
        Ask nomer about a batch of taxon identifiers using a given matcher.
        """

        df_copy = df.drop_duplicates(subset=id_column)

        # Keep a reference to the index of the row containing the identifier
        query_to_ref_index_map = {
            row[id_column]: index for index, row in df_copy.iterrows()
        }

        query = self.nomer.df_to_query(
            df=df_copy,
            id_column=id_column,
            name_column=name_column,
        )
        res_df = self.nomer.ask_nomer(query, matcher=matcher)

        # Keep non empty matches with types SAME_AS or SYNONYM_OF in the target taxonomy
        mapped_df = res_df.dropna(axis=0, subset=["matchId"])
        mapped_df = mapped_df[mapped_df["matchType"].isin(["SAME_AS", "SYNONYM_OF"])]

        if not mapped_df.empty:
            mapped_df = mapped_df[
                mapped_df["matchId"].str.startswith(self.target_taxonomy)
            ]

            # Check if a single taxon matches with several (distinct) ids in the target taxonomy
            # Sometimes, the same taxon is matched several times to the same target id,
            # so we start by dropping these duplicates, then looks for remaining duplicates.
            duplicated = mapped_df.drop_duplicates(
                subset=["queryId", "matchId"], keep=False
            ).duplicated(subset=["queryId"], keep=False)
            if duplicated.any():
                self.logger.error(
                    f"Found several target IDs for a single taxa in df: {mapped_df[duplicated]}"
                )
                # mapped_df[duplicated].to_csv(f"duplicated_{id_column}.csv")
                mapped_df = mapped_df[~duplicated]
                # raise Exception("Unable to handle multiple candidates at id level.")

            # Reset index using the (id, index) map
            mapped_index = mapped_df["queryId"].map(query_to_ref_index_map)
            mapped_df.set_index(
                pd.Index(mapped_index.tolist()).rename("externalId"),
                inplace=True,
            )

            # others_df contains all the taxa that have no match in the target taxonomy
            others_df = df_copy[~df_copy[id_column].isin(mapped_df["queryId"])]

        else:
            others_df = df

        return mapped_df, others_df

    def source_name_to_target_id(
        self, df, id_column=None, name_column=None, matcher="ncbi-taxon"
    ):
        """Ask nomer about a batch of taxon names using a given matcher.
        When there are several taxa matching the same name, we try to find
        the best match using a maximum lineage similarity approach (this
        requires a valid id_column).
        """

        # df_copy = df.drop_duplicates(subset=[id_column, name_column])
        ref_col = id_column if id_column else name_column

        duplicated = df.duplicated(subset=ref_col, keep=False)
        # df.loc[duplicated].to_csv(f"duplicated_base_{ref_col}.csv")

        # Keep a reference to the index of the row containing the identifier
        query_to_ref_index_map = {row[ref_col]: index for index, row in df.iterrows()}

        query = self.nomer.df_to_query(
            df=df,
            name_column=name_column,
            id_column=id_column,
        )

        res_df = self.nomer.ask_nomer(query, matcher=matcher)

        # Keep non empty matches with types SAME_AS or SYNONYM_OF in the target taxonomy
        mapped_df = res_df.dropna(axis=0, subset=["matchId"])
        mapped_df = mapped_df[mapped_df["matchType"].isin(["SAME_AS", "SYNONYM_OF"])]

        if not mapped_df.empty:
            mapped_df = mapped_df[
                mapped_df["matchId"].str.startswith(self.target_taxonomy)
            ]

            subset = "queryId" if id_column else "queryName"

            duplicated = mapped_df.duplicated(subset=[subset], keep=False)

            if duplicated.any():
                self.logger.info(
                    f"Found several target entities for a single taxon in df :\n{mapped_df.loc[duplicated]}"
                )
                # mapped_df.loc[duplicated].to_csv(f"duplicated_{name_column}.csv")

                if id_column:
                    keep_index = self.resolve_duplicates(
                        df, mapped_df[duplicated], id_column, query_to_ref_index_map
                    )
                    for index in keep_index:
                        duplicated.loc[index] = False
                else:
                    # TODO : refactor
                    # If we do not have access to the ifentifier for disambiguation,
                    # we try to compare the lineages. Very often, we have the same
                    # name designating the same entity, but at different ranks (e.g. genus and subgenus)
                    # In this case, we keep the highest rank (e.g. genus)
                    keep_index = []
                    duplicates = mapped_df.loc[duplicated]
                    unique_names = pd.unique(duplicates["matchName"])
                    for name in unique_names:
                        df_name = mapped_df[mapped_df["matchName"] == name]
                        resolved = False
                        if df_name.shape[0] == 2:
                            lin = []
                            for index, row in df_name.iterrows():
                                lin.append((index, row["linNames"].split(" | ")))

                            if set(lin[0][1]) == set(lin[1][1]):
                                resolved = True
                                # We keep the highest rank
                                if len(lin[0][1]) < len(lin[1][1]):
                                    duplicated.loc[lin[0][0]] = False
                                else:
                                    duplicated.loc[lin[1][0]] = False
                        if not resolved:
                            self.logger.debug(
                                f"Cannot resolve duplicates : discard taxon {name}."
                            )

            mapped_df = mapped_df[~duplicated]
            mapped_index = mapped_df[subset].map(query_to_ref_index_map)

            mapped_df.set_index(
                pd.Index(mapped_index.tolist()).rename("externalId"),
                inplace=True,
            )
            others_df = df[~df[ref_col].isin(mapped_df[subset])]

        else:
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
        """
        Given nomer's response for a given taxon, return the best match.
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

    def map(self, df, id_column, name_column):
        """
        Using nomer taxonomic mapping capabilities, try to get IRIs in the
        target taxonomy from taxon names and/or identifiers in the source
        taxonomy.

        First, validate taxon names/ids by querying the source taxonomy
        Then, try to map taxon ids to the target taxonomy using wikidata-web
        For each taxon without a match
            Try to map the taxon name using globi
            For each taxon without a match
                Try to map the taxon name using ncbi
        """

        # Drop duplicates for efficiency
        subset = [x for x in [id_column, name_column] if x]
        w_df = df.dropna(subset=subset).drop_duplicates(subset=subset)

        self.logger.debug(f"Found {w_df.shape[0]} unique taxa")

        # Step 1 : validate taxa against the source taxonomy
        self.logger.debug(f"Validate taxa using info from columns {subset}")
        valid_df = self.validate(
            w_df,
            id_column,
            name_column,
            matcher=self.taxo_to_matcher[self.source_taxonomy],
        )
        self.logger.debug(f"Found {valid_df.shape[0]}/{w_df.shape[0]} valid taxa")

        # src_to_src_mappings is a data frame containing mappings between ids
        # in the same source taxonomy. Indeed, a taxon can have several ids
        # in a single taxonomy, and the id used in the data may not be the preferred
        # id for this taxon.
        src_to_src_mappings = valid_df[
            (pd.notnull(valid_df["matchId"]))
            & (valid_df["queryId"] != valid_df["matchId"])
        ]

        mapped = []

        # Step 2 : map valid taxa using their id in the source taxonomy (if available)
        # if id_column:
        #     self.logger.debug(
        #         f"Map {valid_df.shape[0]} unique taxa to target taxonomy {self.target_taxonomy} using wikidata-web"
        #     )
        #     mapped_df, others_df = self.source_id_to_target_id(
        #         valid_df,
        #         id_column="matchId",
        #         # name_column="matchName",
        #         matcher="wikidata-web",
        #     )
        #     self.logger.debug(
        #         f"Found {mapped_df.shape[0]}/{valid_df.shape[0]} taxa in target taxonomy {self.target_taxonomy}"
        #     )
        #     mapped.append(mapped_df)
        # else:
        #     others_df = valid_df
        if id_column:
            self.logger.debug(
                f"Map {valid_df.shape[0]} unique taxa to target taxonomy {self.target_taxonomy} using globi"
            )
            mapped_df, others_df = self.source_id_to_target_id(
                valid_df,
                id_column="matchId",
                # name_column="matchName",
                matcher="globi",
            )
            self.logger.debug(
                f"Found {mapped_df.shape[0]}/{valid_df.shape[0]} taxa in target taxonomy {self.target_taxonomy} using globi"
            )
            mapped.append(mapped_df)
        else:
            others_df = valid_df

        if not others_df.empty and id_column:
            nb_taxa = others_df.shape[0]
            self.logger.debug(
                f"Map {nb_taxa} remaining taxa to target taxonomy {self.target_taxonomy} using wikidata-web"
            )
            mapped_df, others_df = self.source_id_to_target_id(
                others_df,
                id_column="matchId",
                # name_column="matchName",
                matcher="wikidata-web",
            )
            self.logger.debug(
                f"Found {mapped_df.shape[0]}/{nb_taxa} taxa in target taxonomy {self.target_taxonomy} using wikidata-web"
            )
            mapped.append(mapped_df)
        else:
            others_df = valid_df

        # Step 3 : map the remaining taxa using their name (if available)
        if not others_df.empty and name_column:
            nb_taxa = others_df.shape[0]
            self.logger.debug(
                f"Map {nb_taxa} remaining taxa to target taxonomy {self.target_taxonomy} using ncbi"
            )
            mapped_df, others_df = self.source_name_to_target_id(
                others_df,
                id_column="matchId" if id_column else None,
                name_column="matchName",
                matcher="ncbi",
            )
            self.logger.debug(
                f"Found {mapped_df.shape[0]}/{nb_taxa} taxa in target taxonomy {self.target_taxonomy} using ncbi"
            )
            mapped.append(mapped_df)

        if not others_df.empty and name_column:
            nb_taxa = others_df.shape[0]
            self.logger.debug(
                f"Map {nb_taxa} remaining taxa to target taxonomy {self.target_taxonomy} using globi"
            )
            mapped_df, others_df = self.source_name_to_target_id(
                others_df,
                id_column="matchId" if id_column else None,
                name_column="matchName",
                matcher="globi",
            )
            self.logger.debug(
                f"Found {mapped_df.shape[0]}/{nb_taxa} taxa in target taxonomy {self.target_taxonomy} using globi"
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
