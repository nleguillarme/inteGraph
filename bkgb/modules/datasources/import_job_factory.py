from bkgb.modules.datasources import *


class ImportJobFactory:
    def get_import_job(self, cfg):
        print(cfg)
        if cfg["jobType"] == "dumpImport":
            return DumpImport(cfg)
        elif cfg["jobType"] == "tabularImport":
            return TabularDataImport(cfg)
        else:
            raise ValueError(cfg["jobType"])
