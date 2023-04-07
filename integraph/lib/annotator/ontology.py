import warnings
from owlready2 import *
from .base import Annotator
import pandas as pd
import numpy as np
import ssl
import logging

ssl._create_default_https_context = ssl._create_unverified_context
# warnings.filterwarnings("ignore", category=pronto.warnings.ProntoWarning)


class MultipleMatchesException(Exception):
    pass


class OntologyAnnotator(Annotator):
    def __init__(self, ontology):
        self.onto = get_ontology(ontology)
        self.onto.load()

    def search(self, label):
        iris = self.onto.search(label=label, _case_sensitive=False)
        if not iris:
            iris = self.onto.search(hasExactSynonym=label, _case_sensitive=False)
        if not iris:
            iris = self.onto.search(hasRelatedSynonym=label, _case_sensitive=False)
        if not iris:
            iris = self.onto.search(hasBroadSynonym=label, _case_sensitive=False)
        return iris

    def get_iri(self, label):
        iris = self.search(label)
        if not iris:
            logging.debug(f"No match for {label}")
            return None
        elif len(iris) == 1:
            return iris[0].iri
        else:
            raise MultipleMatchesException(f"Multiple matches for {label} : {iris}")

    def annotate(
        self, df, id_col, label_col, iri_col, source=None, target=None, replace=False
    ):
        assert id_col or label_col
        subset = [col for col in [id_col, label_col] if col]
        sub_df = df[subset].astype(pd.StringDtype(), errors="ignore")
        sub_df[iri_col] = np.nan if replace else df[iri_col]
        sub_df["integraph.id"] = np.nan if replace else df["integraph.id"]
        sub_df["integraph.label"] = np.nan if replace else df["integraph.label"]

        drop_df = sub_df[sub_df[iri_col].isna()]
        drop_df = sub_df.drop_duplicates(subset=subset).replace("", np.nan)

        if id_col:
            pass  # TODO : use hasDbXref annotations
        if label_col:
            iri_map = {}
            for index, row in drop_df.iterrows():
                label = row[label_col]
                matches = self.get_iri(label)
                iri_map[label] = matches

        for index, row in sub_df.iterrows():
            if pd.isna(row[iri_col]) and row[label_col] in iri_map:
                sub_df.at[index, iri_col] = iri_map[row[label_col]]
        return sub_df
