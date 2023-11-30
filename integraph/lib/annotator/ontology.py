import warnings

# from owlready2 import *
from .base import Annotator
import pandas as pd
import numpy as np
import ssl
import logging
import text2term
from text2term import Mapper


ssl._create_default_https_context = ssl._create_unverified_context


class MultipleMatchesException(Exception):
    pass


class OntologyAnnotator(Annotator):
    def __init__(self, ontology):
        self.ontology = ontology

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
        drop_df = (
            drop_df.drop_duplicates(subset=subset)
            .replace("", np.nan)
            .dropna(subset=subset)
        )

        if id_col:
            pass  # TODO : use hasDbXref annotations
        if label_col:
            iri_map = {}
            terms = [t.lower() for t in drop_df[label_col].tolist()]
            mapped_df = text2term.map_terms(
                terms,
                self.ontology,
                use_cache=True,
                excl_deprecated=True,
                mapper=Mapper.JARO,
                min_score=0.95,
                term_type="both",
            )
            for term in terms:
                iri_map[term] = None
                matches = mapped_df[
                    (mapped_df["Source Term"] == term)
                    & (mapped_df["Mapping Score"] > 0.95)
                ]
                if not matches.empty:
                    best_matches = matches.loc[matches["Mapping Score"].idxmax()]
                    if len(best_matches.shape) > 1:
                        raise MultipleMatchesException(
                            f"Multiple matches for {term} : {best_matches}"
                        )
                    iri_map[term] = best_matches["Mapped Term IRI"]

        for index, row in sub_df.iterrows():
            if pd.isna(row[iri_col]) and not pd.isna(
                row[label_col]
            ):  # and row[label_col].lower() in iri_map:
                sub_df.at[index, iri_col] = iri_map.get(row[label_col].lower())
        return sub_df


# class OntologyAnnotatorOWLReady(Annotator):
#     def __init__(self, ontology):
#         self.onto = get_ontology(ontology)
#         self.onto.load()

#     def search(self, label):
#         iris = self.onto.search(label=label, _case_sensitive=False)
#         if not iris:
#             iris = self.onto.search(hasExactSynonym=label, _case_sensitive=False)
#         if not iris:
#             iris = self.onto.search(hasRelatedSynonym=label, _case_sensitive=False)
#         if not iris:
#             iris = self.onto.search(hasBroadSynonym=label, _case_sensitive=False)
#         return iris

#     def get_iri(self, label):
#         iris = self.search(label)
#         if not iris:
#             logging.debug(f"No match for {label}")
#             return None
#         elif len(iris) == 1:
#             return iris[0].iri
#         else:
#             raise MultipleMatchesException(f"Multiple matches for {label} : {iris}")

#     def annotate(
#         self, df, id_col, label_col, iri_col, source=None, target=None, replace=False
#     ):
#         assert id_col or label_col
#         subset = [col for col in [id_col, label_col] if col]
#         sub_df = df[subset].astype(pd.StringDtype(), errors="ignore")
#         sub_df[iri_col] = np.nan if replace else df[iri_col]
#         sub_df["integraph.id"] = np.nan if replace else df["integraph.id"]
#         sub_df["integraph.label"] = np.nan if replace else df["integraph.label"]

#         drop_df = sub_df[sub_df[iri_col].isna()]
#         drop_df = drop_df.drop_duplicates(subset=subset).replace("", np.nan).dropna(subset=subset)

#         if id_col:
#             pass  # TODO : use hasDbXref annotations
#         if label_col:
#             iri_map = {}
#             for index, row in drop_df.iterrows():
#                 label = row[label_col]
#                 print(label_col, label)
#                 matches = self.get_iri(label)
#                 iri_map[label] = matches

#         for index, row in sub_df.iterrows():
#             if pd.isna(row[iri_col]) and row[label_col] in iri_map:
#                 sub_df.at[index, iri_col] = iri_map[row[label_col]]
#         return sub_df
