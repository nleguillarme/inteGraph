from abc import ABC, abstractmethod


class Annotator(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def annotate(self, df, id_col, label_col, iri_col, source, target, replace):
        return
