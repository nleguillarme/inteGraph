import logging
import subprocess
import json
from bkgb.modules.core import Linker

"""
https://github.com/globalbioticinteractions/globalbioticinteractions/wiki/Taxonomy-Matching
"""


class TaxonomyMatching:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # TODO : if necessary, cache GloBI's Taxon Graph
        # echo -e "NCBI:9606\t" | nomer append -Xmx4096m -Xms1024m
        # https://github.com/globalbioticinteractions/nomer#match-term-by-id-with-json-output

    def scrub_taxon_name(self, taxon):
        return taxon.replace("sp.", "").replace("ssp.", "").strip()

    def correct_taxon_name(self, taxon):
        taxon = r"'\t{}'".format(taxon)
        cmd = " ".join(["echo", taxon, "|", "nomer append globi-correct"])
        result = self.run_nomer(cmd)
        print(result)
        return result

    def get_nomer_name_cmd(self, name, matcher="globi-enrich"):
        query = "'\t{}'".format(name)
        cmd = " ".join(["echo", query, "|", "nomer append", matcher])  # , "-o json"])
        return cmd

    def get_nomer_id_cmd(self, taxid, matcher="globi-enrich"):
        query = "'{}\t'".format(taxid)
        cmd = " ".join(["echo", query, "|", "nomer append", matcher])  # , "-o json"])
        return cmd

    def run_nomer(self, cmd):
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=None, shell=True)
        result = p.communicate()[0].decode("utf8")
        return result

    def parse_json_string(self, string):
        lines = string.strip().split("\n")
        entries = [json.loads(line) for line in lines]
        return entries

    def get_uri_from_nomer_result(self, result):
        parsing = result.strip().split("\t")
        if parsing[2] == "NONE":
            return None
        return parsing[-1]

    def match_taxon_id(self, taxid):
        uri = self.get_uri_from_nomer_result(
            self.run_nomer(self.get_nomer_id_cmd(taxid))
        )
        return uri

    def match_taxon_name(self, taxon):
        uri = self.get_uri_from_nomer_result(
            self.run_nomer(self.get_nomer_name_cmd(taxon))
        )
        return uri

    def get_genus(self, taxon):
        tokens = taxon.split()
        if len(tokens) > 1:
            return tokens[0]


class TaxonNameLinker(Linker):
    def __init__(self):
        Linker.__init__(self)
        self.logger = logging.getLogger(__name__)
        self.matching = TaxonomyMatching()

    def get_uri(self, entity):
        return self.matching.match_taxon_name(entity)


class TaxonIdLinker(Linker):
    def __init__(self):
        Linker.__init__(self)
        self.logger = logging.getLogger(__name__)
        self.matching = TaxonomyMatching()

    def get_uri(self, entity):
        return self.matching.match_taxon_id(entity)


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
