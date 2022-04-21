import docker
import os
import pandas as pd
from io import StringIO
import logging
import numpy as np


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
        # self.nomer_cache_dir = "~/.integraph/.nomer"
        self.nomer_cache_dir = os.getenv("INTEGRAPH__CONFIG__NOMER_CACHE_DIR")
        self.volume = {
            os.path.abspath(self.nomer_cache_dir): {"bind": "/.nomer", "mode": "rw"},
        }
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
        # while True:
        response = self.run_nomer_container(query, matcher=matcher)
        res_df = self.parse_nomer_response(response)
        if res_df is not None:
            return res_df
        else:
            return pd.DataFrame()
            # self.logger.debug(f"Nomer raised EmptyDataError : try again.")

    def run_nomer_container(self, query, matcher):
        """Run pynomer append command in Docker container.

        See pynomer documentation : https://github.com/nleguillarme/pynomer.
        """
        append_command = f"'echo {query} | nomer append {matcher}'"
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
            )
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
        # print(query)
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
