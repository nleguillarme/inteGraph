import logging
import pandas as pd
from ...core import URIMapper, URIManager, TaxId
import os
import docker
import re
from docker.errors import NotFound, APIError
from pynomer.client import NomerClient

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

        host = "localhost" if self.config.run_on_localhost else "nomer"
        self.client = NomerClient(base_url=f"http://{host}:9090/")  # docker.from_env()
        # self.pynomer_image = self.client.images.get("pynomer:latest")

    def run_pynomer_append(self, name, id, matcher):
        return self.client.append(
            query=f"{id}\t{name}", matcher=matcher, p=None, o="tsv"
        )
        # volume = {
        #     os.path.abspath("~/.biodivgraph/nomer"): {"bind": "/nomer", "mode": "rw"}
        # }
        # append_command = (
        #     f'pynomer append -p /nomer/properties "{id}\t{name}" -m {matcher} -e -o tsv'
        # )
        # return self.client.containers.run(
        #     self.pynomer_image, append_command, volumes=volume, remove=False
        # )

    def scrub_taxname(self, name):
        name = name.split(" sp. ")[0]
        name = name.split(" ssp. ")[0]
        name = name.strip()
        return " ".join(name.split())

    def parse_entry(self, entry):
        if entry[2] == "NONE":
            return None, None, None
        taxonomy = entry[3].split(":")[0]
        uri = entry[-1]
        taxid = entry[3]
        return taxid, taxonomy, uri

    # def get_preferred_entry(self, entries):
    #     for entry in entries:
    #         matched, taxid, taxonomy, uri = self.parse_entry(entry)
    #         self.logger.debug(
    #             f"{matched}, {taxid}, {taxonomy}, {uri}, {self.target_taxonomy}, {taxonomy == self.target_taxonomy}"
    #         )
    #         if matched and taxonomy == self.target_taxonomy:
    #             return True, taxid, taxonomy, uri
    #     return False, taxid, taxonomy, uri

    def parse_tsv_result(self, result):
        if not result:
            raise ValueError("Nomer result is {}".format(result))
        result_str = result  # .decode("utf-8")
        match = False
        for line in result_str.splitlines():
            if re.search(self.target_taxonomy, line):
                self.logger.debug(
                    f"Found entry for target taxo {self.target_taxonomy} : {line}"
                )
                match = True
                break
        entry = line.strip(" ").rstrip("\t").split("\t")
        taxid, taxonomy, uri = self.parse_entry(entry)
        return match, taxid, taxonomy, uri

        # entries = result_str.split("\n")
        # entries = [entry.strip(" ").rstrip("\t").split("\t") for entry in entries]
        # entries = [entry for entry in entries if len(entry) > 1]
        # if len(entries) < 1:
        #     raise ValueError("Nomer result is an empty string")
        # return self.get_preferred_entry(entries)

    def get_taxid_from_name(self, name):
        self.logger.debug(
            "Match {} in target taxo {}".format(name, self.target_taxonomy)
        )
        matched, taxid, taxonomy, uri = self.parse_tsv_result(
            self.run_pynomer_append(name=name, id="", matcher=self.cache_matcher)
        )
        if not matched:  # Look for taxid using external APIs
            matched, taxid, taxonomy, uri = self.parse_tsv_result(
                self.run_pynomer_append(name=name, id="", matcher=self.enrich_matcher)
            )
        return matched, taxid, taxonomy, uri

    def get_taxid_in_target_taxo(self, taxid):
        self.logger.debug(
            "Match {} in target taxo {}".format(taxid, self.target_taxonomy)
        )
        return self.parse_tsv_result(
            self.run_pynomer_append(name="", id=taxid, matcher=self.wikidata_id_matcher)
        )

    def map(self, name="", taxid=""):
        if (
            taxid != ""
            and self.source_taxonomy
            and self.source_taxonomy not in str(taxid)
        ):
            taxid = self.source_taxonomy + ":{}".format(taxid)
        while True:
            try:
                self.logger.info(
                    "Match ({}, {}) in target taxo {}".format(
                        name, taxid, self.target_taxonomy
                    )
                )

                matched = False
                # If we know the id in the source taxo, use wikidata matcher
                # to find the corresponding id in the target taxo
                if taxid != "":
                    matched, _, _, uri = self.get_taxid_in_target_taxo(taxid)
                    if not matched:
                        self.logger.debug(
                            "No match for {} in target taxo {}".format(
                                taxid, self.target_taxonomy
                            )
                        )

                # If no match from id, try to get taxid from name
                if not matched:
                    matched, taxid, _, uri = self.get_taxid_from_name(name)
                    if not matched:
                        self.logger.debug(
                            "No match for {} in target taxo {}".format(
                                name, self.target_taxonomy
                            )
                        )

                # No match in the target taxo, but a match in another taxo -> use wikidata matcher
                if not matched and taxid:
                    self.logger.debug("Match for {} : {}".format(name, taxid))
                    self.logger.debug(
                        "Match {} in target taxo {}".format(taxid, self.target_taxonomy)
                    )
                    matched, _, _, uri = self.get_taxid_in_target_taxo(taxid)
                    if not matched:
                        self.logger.debug(
                            "No match for {} in target taxo {}".format(
                                taxid, self.target_taxonomy
                            )
                        )

                if not matched:
                    self.logger.info(
                        "No match for taxon ({}, {}) in target taxonomy {}".format(
                            name, taxid, self.target_taxonomy
                        )
                    )
                else:
                    self.logger.info(
                        "Matching result for taxon ({}, {}) in target taxonomy {} : {}".format(
                            name, taxid, self.target_taxonomy, uri
                        )
                    )
            except ValueError as e:
                self.logger.error(e)
                continue
            break
        return {"type": "uri", "value": uri}


class TaxonomicMapper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.config = config
        for column_config in self.config.columns:
            column_config.run_on_localhost = (
                self.config.run_on_localhost
                if "run_on_localhost" in self.config
                else False
            )

    def map(self, df):
        self.logger.info("Start mapping taxonomic entities")

        for column_config in self.config.columns:
            mapper = TaxonomicEntityMapper(column_config)

            colnames = []
            if "id_column" not in column_config:
                column_config.id_column = None
            else:
                colnames += [column_config.id_column]

            if "name_column" not in column_config:
                column_config.name_column = None
            else:
                colnames += [column_config.name_column]

            map = self.map_entities(
                entities=df[colnames],
                id_column=column_config.id_column,
                name_column=column_config.name_column,
                mapper=mapper,
            )

            df[column_config.uri_column] = pd.Series(
                list(map.values()), index=list(map.keys())
            )

        uri_colnames = [
            column_config.uri_column for column_config in self.config.columns
        ]
        # matched_df = df.dropna(subset=uri_colnames)
        # not_matched_df = df[df[uri_colnames].isnull().any(axis=1)]

        return df, uri_colnames  # matched_df, not_matched_df

    def map_entities(self, entities, id_column, name_column, mapper):
        unique = entities.drop_duplicates()
        self.logger.info(
            "Start mapping {}/{} unique taxonomic entities".format(
                unique.shape[0], entities.shape[0]
            )
        )
        unique_entity_map = {}
        for index, row in unique.iterrows():
            name, taxid = self.get_name_and_taxid(row, id_column, name_column)
            if taxid == "" and name == "":
                raise ValueError

            res = mapper.map(name=name, taxid=taxid)
            if res["type"] == "uri":
                unique_entity_map[(taxid, name)] = (
                    res["value"] if res["value"] else None
                )

        entity_map = {}
        for index, row in entities.iterrows():
            name, taxid = self.get_name_and_taxid(row, id_column, name_column)
            entity_map[index] = unique_entity_map[(taxid, name)]

        return entity_map

    def get_name_and_taxid(self, row, id_column, name_column):
        taxid = row[id_column] if (id_column and not pd.isnull(row[id_column])) else ""
        name = (
            row[name_column]
            if (name_column and not pd.isnull(row[name_column]))
            else ""
        )
        return name, taxid
