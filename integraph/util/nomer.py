import docker
import os
import pandas as pd
from io import StringIO
import logging
import numpy as np
from tempfile import NamedTemporaryFile


class GlobalNameParserException(Exception):
    pass


class NomerException(Exception):
    pass


def get_canonical_names(f_in):
    client = docker.from_env()
    remote_file = os.path.join("/tmp", os.path.basename(f_in))
    volume = {
        f_in: {"bind": remote_file},
    }
    command = (f"{remote_file}",)
    result = run_container(
        client,
        image="gnames/gognparser:latest",
        command=command,
        volumes=volume,
        entrypoint="gnparser -j 20",
    )
    if result["StatusCode"] != 0:
        raise GlobalNameParserException(result["logs"])

    res_df = pd.read_csv(StringIO(result["logs"]), sep=",")
    return res_df


class NomerHelper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.client = docker.from_env()
        self.nomer_image = "nomer:latest"
        self.nomer_cache_dir = os.getenv("INTEGRAPH__CONFIG__NOMER_CACHE_DIR")
        self.volume = {
            os.path.abspath(self.nomer_cache_dir): {
                "bind": "/root/.cache/nomer",
                "mode": "rw",
            },
        }

        # [{"column":0,"type":"externalId"},
        # {"column": 1,"type":"name"},
        # {"column": 2,"type":"authorship"},
        # {"column": 3,"type":"rank"},
        # {"column": 4,"type":"commonNames"},
        # {"column": 5,"type":"path"},
        # {"column": 6,"type":"pathIds"},
        # {"column": 7,"type":"pathNames"},
        # {"column": 8,"type":"pathAuthorships"},
        # {"column": 9,"type":"externalUrl"}]
        self.columns = [
            "queryId",
            "queryName",
            "matchType",
            "matchId",
            "matchName",
            "matchAuthorship",
            "matchRank",
            "alternativeNames",
            "linNames",
            "linIds",
            "linRanks",
            "linAuthorships",
            "iri",
        ]

    def ask_nomer(self, query, matcher):
        """Send query to nomer and parse nomer response.

        Returns a DataFrame.
        """
        response = self.run_nomer_container(query, matcher=matcher)
        res_df = self.parse_nomer_response(response)
        if res_df is not None:
            return res_df
        else:
            return pd.DataFrame()

    def run_nomer_container(self, query, matcher):
        """Run pynomer append command in Docker container.

        See pynomer documentation : https://github.com/nleguillarme/pynomer.
        """
        append_command = f"'echo -e {query}|nomer append {matcher}'"
        self.logger.debug(f"run_nomer_container : command {append_command}")
        result = self.client.containers.run(
            self.nomer_image,
            append_command,
            volumes=self.volume,
            remove=False,
        )
        return result

    def parse_nomer_response(self, response):
        """Transform nomer response into a pandas DataFrame.

        Nomer response format is (normally) a valid TSV string.
        """
        try:
            res_df = pd.read_csv(
                StringIO(response.decode("utf-8")),
                sep="\t",
                header=None,
                keep_default_na=False,
            ).replace("", np.nan)
            res_df.columns = self.columns
        except pd.errors.EmptyDataError as e:
            self.logger.error(e)
            return None  # False, None
        else:
            return res_df  # True, res_df

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


def run_container(client, image, command, volumes, entrypoint=None, remove=True):
    container = client.containers.run(
        client.images.get(image),
        command=command,
        volumes=volumes,
        entrypoint=entrypoint,
        detach=True,
    )
    result = container.wait()
    result["logs"] = container.logs().decode("utf-8")
    if remove:
        container.remove()
    return result


def normalize_names(names):
    """
    Given a list of taxonomic names, return the corresponding canonical forms
    """
    f_temp = NamedTemporaryFile(delete=True)  # False)
    # self.logger.debug(f"Write names to {f_temp.name} for validation using gnparser")
    names_str = "\n".join([name.replace("\n", " ") for name in names])
    f_temp.write(names_str.encode())
    f_temp.read()  # I don't know why but it is needed or sometimes the file appears empty when reading
    canonical_names = get_canonical_names(f_temp.name)
    f_temp.close()
    canonical_names = canonical_names["CanonicalSimple"].to_list()
    assert len(names) == len(canonical_names)
    return {
        names[i]: canonical_names[i] if not pd.isna(canonical_names[i]) else ""
        for i in range(len(names))
    }


TAXONOMIES = {
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
    # "FBC:SLB:SpecCode:": {
    #     "url_prefix": "http://sealifebase.org/Summary/SpeciesSummary.php?id=",
    #     "url_suffix": "",
    # },
    "INAT_TAXON:": {"url_prefix": "https://inaturalist.org/taxa/", "url_suffix": ""},
    "GEONAMES:": {"url_prefix": "http://www.geonames.org/", "url_suffix": ""},
    "INAT:": {
        "url_prefix": "https://www.inaturalist.org/observations/",
        "url_suffix": "",
    },
    "WD:": {"url_prefix": "https://www.wikidata.org/wiki/", "url_suffix": ""},
    # "bioinfo:ref:": {
    #     "url_prefix": "http://bioinfo.org.uk/html/b",
    #     "url_suffix": ".htm",
    # },
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
    # "FBC:FB:SpecCode:": {
    #     "url_prefix": "http://fishbase.org/summary/",
    #     "url_suffix": "",
    # },
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
    # "urn:lsid:biodiversity.org.au:apni.taxon:": {
    #     "url_prefix": "http://id.biodiversity.org.au/apni.taxon/",
    #     "url_suffix": "",
    # },
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
