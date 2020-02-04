import logging
from . import TaxId

ID_PREFIXES = {
    # "http://": {"url_prefix": "http://", "url_suffix": ""},
    # "https://": {"url_prefix": "https://", "url_suffix": ""},
    "W:": {"url_prefix": "http://wikipedia.org/wiki/", "url_suffix": ""},
    "NBN:": {"url_prefix": "https://data.nbn.org.uk/Taxa/", "url_suffix": ""},
    "GBIF:": {"url_prefix": "http://www.gbif.org/species/", "url_suffix": ""},
    "AFD:": {
        "url_prefix": "http://www.environment.gov.au/biodiversity/abrs/online-resources/fauna/afd/taxa/",
        "url_suffix": "",
    },
    "https://cmecscatalog.org/cmecs/classification/aquaticSetting/": {
        "url_prefix": "https://cmecscatalog.org/cmecs/classification/aquaticSetting/",
        "url_suffix": "",
    },
    "FBC:SLB:SpecCode:": {
        "url_prefix": "http://sealifebase.org/Summary/SpeciesSummary.php?id=",
        "url_suffix": "",
    },
    "INAT_TAXON:": {"url_prefix": "https://inaturalist.org/taxa/", "url_suffix": ""},
    "GEONAMES:": {"url_prefix": "http://www.geonames.org/", "url_suffix": ""},
    "INAT:": {
        "url_prefix": "https://www.inaturalist.org/observations/",
        "url_suffix": "",
    },
    "WD:": {"url_prefix": "https://www.wikidata.org/wiki/", "url_suffix": ""},
    "bioinfo:ref:": {
        "url_prefix": "http://bioinfo.org.uk/html/b",
        "url_suffix": ".htm",
    },
    "GAME:": {
        "url_prefix": "https://public.myfwc.com/FWRI/GAME/Survey.aspx?id=",
        "url_suffix": "",
    },
    "ALATaxon:": {"url_prefix": "https://bie.ala.org.au/species/", "url_suffix": ""},
    "doi:": {"url_prefix": "https://doi.org/", "url_suffix": ""},
    "NCBI:": {
        "url_prefix": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=",
        "url_suffix": "",
    },
    "BioGoMx:": {
        "url_prefix": "http://gulfbase.org/biogomx/biospecies.php?species=",
        "url_suffix": "",
    },
    "FBC:FB:SpecCode:": {
        "url_prefix": "http://fishbase.org/summary/",
        "url_suffix": "",
    },
    "IRMNG:": {
        "url_prefix": "http://www.marine.csiro.au/mirrorsearch/ir_search.list_species?sp_id=",
        "url_suffix": "",
    },
    "NCBITaxon:": {
        "url_prefix": "http://purl.obolibrary.org/obo/NCBITaxon_",
        "url_suffix": "",
    },
    "ENVO:": {"url_prefix": "http://purl.obolibrary.org/obo/ENVO_", "url_suffix": ""},
    "OTT:": {
        "url_prefix": "https://tree.opentreeoflife.org/opentree/ottol@",
        "url_suffix": "",
    },
    "ITIS:": {
        "url_prefix": "http://www.itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value=",
        "url_suffix": "",
    },
    "WORMS:": {
        "url_prefix": "http://www.marinespecies.org/aphia.php?p=taxdetails&id=",
        "url_suffix": "",
    },
    "urn:lsid:biodiversity.org.au:apni.taxon:": {
        "url_prefix": "http://id.biodiversity.org.au/apni.taxon/",
        "url_suffix": "",
    },
    "EOL:": {"url_prefix": "http://eol.org/pages/", "url_suffix": ""},
    "EOL_V2:": {
        "url_prefix": "https://doi.org/10.5281/zenodo.1495266#",
        "url_suffix": "",
    },
    "IF:": {
        "url_prefix": "http://www.indexfungorum.org/names/NamesRecord.asp?RecordID=",
        "url_suffix": "",
    },
    "DOI:": {"url_prefix": "https://doi.org/", "url_suffix": ""},
}


class URIMapper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.map = ID_PREFIXES

    def get_uri_prefix_from_db_prefix(self, db_prefix):
        if db_prefix in self.map:
            return self.map[db_prefix]["url_prefix"]
        else:
            raise ValueError("No known URI for prefix ", db_prefix)

    def get_db_prefix_from_uri(self, uri):
        for key, val in self.map.items():
            if val["url_prefix"] in uri:
                return key
        raise ValueError("No known prefix for URI ", uri)


class URIManager:
    def __init__(self, mapper=URIMapper()):
        self.logger = logging.getLogger(__name__)
        self.mapper = mapper

    def get_uri_from_taxid(self, taxid):
        try:
            db, id = taxid.split()
            uri_prefix = self.mapper.get_uri_prefix_from_db_prefix(db)
            return "{}{}".format(uri_prefix, id)
            # return "<{}{}>".format(uri_prefix, id)
        except Exception as e:
            raise ValueError("Cannot format URI for source ", taxid.get_prefix())

    def get_taxid_from_uri(self, uri):
        try:
            db_prefix = self.mapper.get_db_prefix_from_uri(uri)
            uri_prefix = self.mapper.get_uri_prefix_from_db_prefix(db_prefix)
        except ValueError:
            logging.getLogger(__name__).error(
                "Cannot get taxid from URI {}".format(uri)
            )
            return None
        id = uri.strip("").replace(uri_prefix, "")
        # id = uri.strip("<>").replace(uri_prefix, "")
        return TaxId(db_prefix, id)
