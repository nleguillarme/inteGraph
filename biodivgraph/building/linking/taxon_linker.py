import logging
from pynomer import NomerClient
import json
from ..core import Linker
from biodivgraph.core import URIMapper, URIManager, TaxId

"""
https://github.com/globalbioticinteractions/globalbioticinteractions/wiki/Taxonomy-Matching
"""


class TaxonomyMatching:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.uri_mapper = URIMapper()
        self.uri_manager = URIManager()
        self.cache_matcher = "globi-taxon-cache"
        self.enrich_matcher = "globi-enrich"
        self.scrubbing_matcher = "globi-correct"
        self.db_preferences = ["GBIF:", "NCBI:", "NCBITaxon:", "WORMS:", "ITIS:"]
        self.client = NomerClient(base_url="http://nomer:5000/")
        # self.client.append(id="GBIF:1", matcher=self.cache_matcher)

    def scrub_taxname(self, name):
        name = name.split(" sp. ")[0]
        name = name.split(" ssp. ")[0]
        name = name.strip()
        return " ".join(name.split())

    # def scrub_taxname(self, name):
    #     res = pn.append(name=name, matcher=self.scrubbing_matcher)
    #     return {"type": "uri", "value": res}

    def get_preferred_uri(self, entries):
        entry_map = {}
        for entry in entries:
            same_as_taxid = TaxId().init_from_string(entry[3])
            uri = self.uri_manager.get_uri_from_taxid(same_as_taxid)
            db_prefix = self.uri_mapper.get_db_prefix_from_uri(uri)
            entry_map[db_prefix] = uri
        # print(entries, entry_map)
        for db_prefix in self.db_preferences:
            if db_prefix in entry_map:
                return entry_map[db_prefix]
        return next(iter(entry_map.values()))

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

    def match(self, name="", taxid=""):
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


class TaxNameLinker(Linker):
    def __init__(self, transforms):
        Linker.__init__(self, transforms)
        self.logger = logging.getLogger(__name__)
        self.matching = TaxonomyMatching()

    def get_uri(self, entity):
        entity = self.apply_transforms(entity)
        name = self.matching.scrub_taxname(entity)
        uri = self.matching.match(name=name)
        return uri


class TaxIdLinker(Linker):
    def __init__(self, transforms):
        Linker.__init__(self, transforms)
        self.logger = logging.getLogger(__name__)
        self.matching = TaxonomyMatching()

    def get_uri(self, entity):
        entity = self.apply_transforms(entity)
        uri = self.matching.match(taxid=entity)
        return uri
