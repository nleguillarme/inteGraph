import logging
import subprocess
import os


class RMLMappingEngine:
    def __init__(self, dir):
        self.logger = logging.getLogger(__name__)
        self.rules = None
        self.dir = dir

    def set_rules(self, path):
        filename = os.path.splitext(os.path.basename(path))[0]
        self.rules = filename + ".rml"
        self.to_rml(path, os.path.join(self.dir, self.rules))

    def to_rml(self, yaml_file, rml_file):
        cmd = "yarrrml-parser -i {} -o {}".format(yaml_file, rml_file)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=None, shell=True)
        exit_code = p.wait()
        result = p.communicate()[0].decode("utf8")
        print(exit_code, result)

    def run_mapping(self, df, output_file):
        self.df_to_csv(df)
        if self.rules != None:
            cmd = "docker run --rm --name rmlmapper -v {}:/data \
                rmlmapper -m {} --serialization nquads --outputfile {}".format(
                os.path.abspath(self.dir), self.rules, output_file
            )
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=None, shell=True)
            result = p.communicate()[0].decode("utf8")
            print(result)

    def df_to_csv(self, df):
        df.columns = ["sub", "pred", "obj"]

        # df_sub = df.loc[:, ["sub"]]
        df["id"] = df.index

        df_obj = df.loc[:, ["obj"]]
        df_obj["id"] = df.index

        df_pred = df.loc[:, ["pred"]]
        df_pred["sub_id"] = df.index
        df_pred["obj_id"] = df.index

        df.to_csv(os.path.join(self.dir, "s.csv"), index=False)
        df_pred.to_csv(os.path.join(self.dir, "p.csv"), index=False)
        df_obj.to_csv(os.path.join(self.dir, "o.csv"), index=False)
