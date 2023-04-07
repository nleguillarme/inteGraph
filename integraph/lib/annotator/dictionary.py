import logging
from pathlib import Path
from .base import Annotator


class DictionaryAnnotator(Annotator):
    def __init__(self, filepath):
        self.dict = None
        self._load(Path(filepath))

    def annotate(
        self, df, id_col, label_col, iri_col, source=None, target=None, replace=False
    ):
        import pandas as pd
        import numpy as np

        assert id_col or label_col
        subset = [col for col in [id_col, label_col] if col]
        sub_df = df[subset].astype(pd.StringDtype(), errors="ignore")
        sub_df[iri_col] = np.nan if replace else df[iri_col]
        sub_df["integraph.id"] = np.nan if replace else df["integraph.id"]
        sub_df["integraph.label"] = np.nan if replace else df["integraph.label"]

        drop_df = sub_df[sub_df[iri_col].isna()]
        drop_df = sub_df.drop_duplicates(subset=subset).replace("", np.nan)

        iri_map = {}
        for _, row in drop_df.iterrows():
            index = ((row[id_col],) if id_col else ()) + (
                (row[label_col],) if label_col else ()
            )
            iri = None
            if id_col and not pd.isna(row[id_col]):
                iri = self.get_iri(row[id_col])
            if (not iri) and label_col and not pd.isna(row[label_col]):
                iri = self.get_iri(row[label_col])
                iri_map[index] = iri if iri else np.nan

        for index, row in sub_df.iterrows():
            iri_index = ((row[id_col],) if id_col else ()) + (
                (row[label_col],) if label_col else ()
            )
            if pd.isna(row[iri_col]) and iri_index in iri_map:
                sub_df.at[index, iri_col] = iri_map[iri_index]
        return sub_df

    def _load(self, filepath):
        import yaml

        if filepath and filepath.exists():
            with open(filepath, "r") as stream:
                try:
                    self.dict = yaml.safe_load(stream)
                except yaml.YAMLError as e:
                    logging.error(e)
        else:
            raise FileNotFoundError(filepath)

    def get_iri(self, term):
        return self.dict[term] if term in self.dict else None
