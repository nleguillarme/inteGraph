import logging
import subprocess
import json
import sys
from bkgb.modules.core import Linker

"""
https://github.com/globalbioticinteractions/globalbioticinteractions/wiki/Taxonomy-Matching
"""


class TaxonomyMatching:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.cache_matcher = "globi-taxon-cache"
        self.enrich_matcher = "globi-enrich"
        # TODO : if necessary, cache GloBI's Taxon Graph
        # echo -e "NCBI:9606\t" | nomer append -Xmx4096m -Xms1024m
        # https://github.com/globalbioticinteractions/nomer#match-term-by-id-with-json-output

    def scrub_taxon_name(self, taxon):
        return taxon.replace("sp.", "").replace("ssp.", "").strip()

    def correct_taxon_name(self, taxon):
        taxon = r"'\t{}'".format(taxon)
        cmd = " ".join(["echo", taxon, "|", "nomer append globi-correct"])
        result = self.run_nomer(cmd)
        return result

    def get_nomer_name_cmd(self, name, matcher="globi-enrich"):
        query = r"'\t{}'".format(name)
        cmd = " ".join(["echo", query, "|", "nomer append", matcher])  # , "-o json"])
        return cmd

    def get_nomer_id_cmd(self, taxid, matcher="globi-enrich"):
        query = r"'{}\t'".format(taxid)
        cmd = " ".join(["echo", query, "|", "nomer append", matcher])  # , "-o json"])
        return cmd

    def run_nomer(self, cmd):
        p = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, shell=True
        )
        result = p.communicate()[0].decode("utf8")
        return result

    def parse_json_string(self, string):
        lines = string.strip().split("\n")
        entries = [json.loads(line) for line in lines]
        return entries

    def get_uri_from_nomer_result(self, result):
        parsing = result.strip().split("\t")
        if len(parsing) < 3:
            raise ValueError("Nomer result is an empty string")
        if parsing[2] == "NONE":
            return None
        return parsing[-1]

    def match_taxon_id(self, taxid):
        while True:
            try:
                uri = self.get_uri_from_nomer_result(
                    self.run_nomer(
                        self.get_nomer_id_cmd(taxid, matcher=self.cache_matcher)
                    )
                )
                if uri == None:
                    uri = self.get_uri_from_nomer_result(
                        self.run_nomer(
                            self.get_nomer_id_cmd(taxid, matcher=self.enrich_matcher)
                        )
                    )
            except ValueError:
                continue
            break
        # if uri != None and "gbif" in uri and "9797892" in uri:
        #     if taxid.split(":")[-1] == uri.split("/")[-1]:
        #         # print("Map {} as {}".format(taxid, uri))
        #         pass
        #     else:
        #         self.logger.error("Map {} as {}".format(taxid, uri))
        return uri

    def match_taxon_name(self, taxon):
        while True:
            try:
                uri = self.get_uri_from_nomer_result(
                    self.run_nomer(
                        self.get_nomer_name_cmd(taxon, matcher=self.cache_matcher)
                    )
                )
                if uri == None:
                    uri = self.get_uri_from_nomer_result(
                        self.run_nomer(
                            self.get_nomer_id_cmd(taxid, matcher=self.enrich_matcher)
                        )
                    )
            except ValueError:
                continue
            break
        return uri

    def get_genus(self, taxon):
        tokens = taxon.split()
        if len(tokens) > 1:
            return tokens[0]


class TaxonNameLinker(Linker):
    def __init__(self, transforms):
        Linker.__init__(self, transforms)
        self.logger = logging.getLogger(__name__)
        self.matching = TaxonomyMatching()

    def get_uri(self, entity):
        entity = self.apply_transforms(entity)
        uri = self.matching.match_taxon_name(entity)
        return uri
        # return "<" + uri + ">" if uri else None


class TaxonIdLinker(Linker):
    def __init__(self, transforms):
        Linker.__init__(self, transforms)
        self.logger = logging.getLogger(__name__)
        self.matching = TaxonomyMatching()

    def get_uri(self, entity):
        entity = self.apply_transforms(entity)
        uri = self.matching.match_taxon_id(entity)
        return uri
        # return "<" + uri + ">" if uri else None


if __name__ == "__main__":

    tm = TaxonomyMatching()

    tm.match_taxon_name(tm.scrub_taxon_name(" Homo sapiens sp."))
    tm.correct_taxon_name("Homo sapies")

    taxon = tm.scrub_taxon_name("Homo sapies")
    if tm.match_taxon_name(taxon) == None:
        genus = tm.get_genus(taxon)
        taxid = tm.match_taxon_name(genus)
    # TaxonomyMatching().correct_taxon_name("Mimesa bicolor")
    # TaxonomyMatching().match_taxon_id("NCBI:9606")
