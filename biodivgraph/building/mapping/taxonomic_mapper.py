import logging
from pynomer import NomerClient
import json
import pandas as pd
from ..core import Linker
from ...core import URIMapper, URIManager, TaxId

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
        self.enrich_matcher = "globi-enrich"
        self.scrubbing_matcher = "globi-correct"
        self.wikidata_id_matcher = "wikidata-taxon-id-web"

        if config.run_on_localhost:
            self.client = NomerClient(base_url="http://localhost:5000/")
        else:
            self.client = NomerClient(base_url="http://nomer:5000/")

    def scrub_taxname(self, name):
        name = name.split(" sp. ")[0]
        name = name.split(" ssp. ")[0]
        name = name.strip()
        return " ".join(name.split())

    def parse_entry(self, entry):
        if entry[2] == "NONE":
            return False, None, None, None
        taxonomy = entry[3].split(":")[0]
        uri = entry[-1]
        taxid = entry[3]
        return True, taxid, taxonomy, uri

    def get_preferred_entry(self, entries):
        for entry in entries:
            matched, taxid, taxonomy, uri = self.parse_entry(entry)
            if matched and taxonomy == self.target_taxonomy:
                return True, taxid, taxonomy, uri
        return False, taxid, taxonomy, uri

    def parse_tsv_result(self, result):
        if not result:
            raise ValueError("Nomer result is {}".format(result))
        entries = result.split("\n")
        entries = [entry.strip(" ").rstrip("\t").split("\t") for entry in entries]
        if len(entries) < 1:
            raise ValueError("Nomer result is an empty string")
        return self.get_preferred_entry(entries)

    def get_taxid_from_name(self, name):
        self.logger.info("Try to match name {}".format(name))
        matched, taxid, taxonomy, uri = self.parse_tsv_result(
            self.client.append(name=name, id="", matcher=self.cache_matcher)
        )
        if not matched:  # Look for taxid using external APIs
            matched, taxid, taxonomy, uri = self.parse_tsv_result(
                self.client.append(name=name, id="", matcher=self.enrich_matcher)
            )
        return matched, taxid, taxonomy, uri

    def map(self, name="", taxid=""):
        if (
            taxid != ""
            and self.source_taxonomy
            and self.source_taxonomy not in str(taxid)
        ):
            taxid = self.source_taxonomy + ":{}".format(taxid)
        while True:
            try:
                self.logger.info("Match ({}, {})".format(name, taxid))

                matched = False
                if taxid != "":
                    matched, _, _, uri = self.parse_tsv_result(
                        self.client.append(
                            name="", id=taxid, matcher=self.wikidata_id_matcher
                        )
                    )

                if not matched:
                    matched, taxid, _, uri = self.get_taxid_from_name(name)

                if not matched and taxid:
                    matched, _, _, uri = self.parse_tsv_result(
                        self.client.append(
                            name="", id=taxid, matcher=self.wikidata_id_matcher
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
            column_config.run_on_localhost = self.config.run_on_localhost

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
        matched_df = df.dropna(subset=uri_colnames)
        not_matched_df = df[df[uri_colnames].isnull().any(axis=1)]

        return matched_df, not_matched_df

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
