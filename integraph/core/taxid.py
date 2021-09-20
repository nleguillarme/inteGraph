import logging


class TaxId:
    def __init__(self, db_prefix=None, id=None):
        self.logger = logging.getLogger(__name__)
        self.db_prefix = db_prefix
        self.id = id

    def init_from_string(self, taxid_str):
        tokens = taxid_str.split(":")
        if len(tokens) > 1:
            self.db_prefix = ":".join(tokens[0:-1]) + ":"
            self.id = tokens[-1]
            return self
        else:
            raise ValueError(
                "{} is not a valid taxid. Expected format is DBPREFIX:ID.".format(
                    taxid_str
                )
            )

    def get_prefix(self):
        return self.db_prefix

    def get_id(self):
        return self.id

    def get_taxid(self):
        return {"type": "taxid", "value": str(self)}

    def split(self):
        return self.get_prefix(), self.get_id()

    def __str__(self):
        return self.get_prefix() + str(self.get_id())
