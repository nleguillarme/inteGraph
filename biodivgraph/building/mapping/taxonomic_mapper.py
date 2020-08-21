import logging
from pynomer import NomerClient
import json
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

        self.default_taxo = "GBIF"
        self.src_taxo = (
            config.source_taxonomy + ":" if "source_taxonomy" in config else None
        )
        self.tgt_taxo = config.target_taxonomy + ":"

        if self.src_taxo and not self.uri_mapper.is_valid_db_prefix(self.src_taxo):
            raise ValueError(
                "Fatal error : invalid source taxonomy {}".format(
                    config.source_taxonomy
                )
            )
        if not self.uri_mapper.is_valid_db_prefix(self.tgt_taxo):
            self.logger.error(
                "Invalid target taxonomy {} : use default taxonomy {}".format(
                    config.target_taxonomy, self.default_taxo
                )
            )

        self.cache_matcher = "globi-taxon-cache"
        self.enrich_matcher = "globi-enrich"
        self.scrubbing_matcher = "globi-correct"

        # self.db_preferences = ["GBIF:", "NCBI:", "NCBITaxon:", "WORMS:", "ITIS:"]

        if config.run_on_localhost:
            self.client = NomerClient(base_url="http://localhost:5000/")
        else:
            self.client = NomerClient(base_url="http://nomer:5000/")
        # self.client.append(id="GBIF:1", matcher=self.cache_matcher)

    def scrub_taxname(self, name):
        name = name.split(" sp. ")[0]
        name = name.split(" ssp. ")[0]
        name = name.strip()
        return " ".join(name.split())

    def get_preferred_uri(self, entries):
        print(entries)
        entry_map = {}
        for entry in entries:
            same_as_taxid = TaxId().init_from_string(entry[3])
            uri = self.uri_manager.get_uri_from_taxid(same_as_taxid)
            db_prefix = self.uri_mapper.get_db_prefix_from_uri(uri)
            entry_map[db_prefix] = uri
        if self.tgt_taxo in entry_map:
            return entry_map[self.tgt_taxo]
        else:
            self.logger.info(
                "No entry for target taxonomy {} : {}".format(self.tgt_taxo, entry_map)
            )
        # for db_prefix in self.db_preferences:
        #     if db_prefix in entry_map:
        #         return entry_map[db_prefix]
        # return next(iter(entry_map.values()))

    def get_uri_from_tsv_result(self, result):
        if not result:
            raise ValueError("Nomer result is {}".format(result))
        entries = result.split("\n")
        entries = [entry.strip(" ").rstrip("\t").split("\t") for entry in entries]
        if len(entries) < 1:
            raise ValueError("Nomer result is an empty string")
        parsing = entries[0]
        if len(parsing) < 3:
            raise ValueError("Nomer result is an empty string")
        if parsing[2] == "NONE":
            return None
        return self.get_preferred_uri(entries)

    def map(self, name="", taxid=""):
        if taxid != "" and self.src_taxo and self.src_taxo not in str(taxid):
            taxid = self.src_taxo + str(taxid)
        while True:
            try:  # Look for taxid in local cache
                self.logger.info("Match {} {}".format(name, taxid))
                uri = self.get_uri_from_tsv_result(
                    self.client.append(name=name, id=taxid, matcher=self.cache_matcher)
                )
                if uri == None:  # Look for taxid using external APIs
                    uri = self.get_uri_from_tsv_result(
                        self.client.append(
                            name=name, id=taxid, matcher=self.enrich_matcher
                        )
                    )
                self.logger.info("Matching result {} : {}".format(taxid, uri))
            except ValueError as e:
                self.logger.error(e)
                continue
            break
        return {"type": "uri", "value": uri}

    # def get_genus(self, taxon):
    #     tokens = taxon.split()
    #     if len(tokens) > 1:
    #         return tokens[0]


class TaxNameMapper(Linker):
    def __init__(self, config, transforms):
        Linker.__init__(self, transforms)
        self.logger = logging.getLogger(__name__)
        self.mapper = TaxonomicEntityMapper(config)

    def get_uri(self, entity):
        entity = self.apply_transforms(entity)
        name = self.mapper.scrub_taxname(entity)
        uri = self.mapper.map(name=name)
        return uri


class TaxIdMapper(Linker):
    def __init__(self, config, transforms=None):
        Linker.__init__(self, transforms)
        self.logger = logging.getLogger(__name__)
        self.mapper = TaxonomicEntityMapper(config)

    def get_uri(self, entity):
        entity = self.apply_transforms(entity)
        uri = self.mapper.map(taxid=entity)
        return uri


class TaxonomicMapper:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        factory = TaxonomicMapperFactory()
        self.mappers = {}
        for column_config in config.columns:
            column_config.run_on_localhost = config.run_on_localhost
            mapper = factory.get_mapper(column_config)
            if mapper:
                self.mappers[column_config.column_name] = mapper

    def map(self, df):
        self.logger.info("Start mapping taxonomic entities")
        for colname in self.mappers:
            df[colname] = self.map_entities(df[colname], self.mappers[colname])
        # sub = self.linkEntities(triples.iloc[:, 0], self.sub_linker)
        # pred = self.linkEntities(triples.iloc[:, 1], self.pred_linker)
        # obj = self.linkEntities(triples.iloc[:, 2], self.obj_linker)
        # df = pd.concat([sub, pred, obj], axis=1, sort=False)
        # df.columns = ["s", "p", "o"]
        return df

    def map_entities(self, entities, mapper):
        map = {}
        unique = entities.unique()
        self.logger.info(
            "Start mapping {}/{} unique entities using {}".format(
                len(unique), entities.shape[0], mapper.__class__.__name__
            )
        )
        for entity in unique:
            res = (
                mapper.get_uri(entity)
                if mapper != None
                else {"type": "uri", "value": entity}
            )
            if res["type"] == "uri":
                map[entity] = res["value"] if res["value"] else None
        self.logger.debug("Mapper {} terminated".format(mapper.__class__.__name__))
        return entities.replace(map)


class TaxonomicMapperFactory:
    def get_mapper(self, config):
        # tranforms = TransformFactory().get_transforms(config)
        if "column_type" not in config:
            return None
        if config.column_type == "name":
            return TaxNameMapper(config)
        elif config.column_type == "id":
            return TaxIdMapper(config)
        # elif os.path.exists(os.path.join(cfg["rootDir"], mapping)):
        #     return DictBasedLinker(os.path.join(cfg["rootDir"], mapping), tranforms)
        else:
            raise ValueError(config.column_type)
