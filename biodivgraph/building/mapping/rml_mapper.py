import logging
import subprocess
import os
import sys
import shutil
from biodivgraph.utils.file_helper import copy_file_to_dir


class RMLMappingEngine:
    def __init__(self, config, working_dir, jar_location):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.path_to_yaml_rules = self.config.ontological_mapping_file
        self.path_to_rml_rules = None
        self.rml_filename = None
        self.working_dir = working_dir
        self.jar_location = os.path.join(os.getcwd(), jar_location)

    def rules_to_rml(self):
        rulefile = (
            os.path.splitext(os.path.basename(self.path_to_yaml_rules))[0] + ".rml"
        )
        self.path_to_rml_rules = os.path.join(self.working_dir, rulefile)
        self.to_rml(self.path_to_yaml_rules, self.path_to_rml_rules)
        print(self.path_to_yaml_rules, self.path_to_rml_rules)
        if os.path.exists(self.path_to_rml_rules):
            self.logger.info(
                "Conversion of {} to RML successful".format(
                    os.path.basename(self.path_to_yaml_rules)
                )
            )
            self.rml_filename = rulefile
        else:
            self.path_to_rml_rules = None
            raise RuntimeError(
                "Conversion of {} to RML failed".format(
                    os.path.basename(self.path_to_yaml_rules)
                )
            )

    def to_rml(self, yaml_file, rml_file):
        cmd = "yarrrml-parser -i {} -o {}".format(yaml_file, rml_file)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=None, shell=True)
        exit_code = p.wait()  # TODO : Find a way to detect when conversion failed
        result = p.communicate()[0].decode("utf8")
        return exit_code

    def run_mapping(self, df, wdir, f_out):
        self.df_to_csv(df, wdir)
        if self.path_to_rml_rules == None:
            self.rules_to_rml()
            if self.path_to_rml_rules == None:
                raise ValueError("The mapping engine needs a valid RML rules file")
        if os.path.exists(self.path_to_rml_rules):
            copy_file_to_dir(self.path_to_rml_rules, wdir)
            if not os.path.exists(
                "{}/rmlmapper-4.6.0-r147.jar".format(self.jar_location)
            ):
                raise FileNotFoundError(
                    "{}/rmlmapper-4.6.0-r147.jar".format(self.jar_location)
                )
            cmd = "cd {} ; java -jar {}/rmlmapper-4.6.0-r147.jar -m {} -s nquads -o {}".format(
                wdir, self.jar_location, self.rml_filename, os.path.basename(f_out)
            )
            self.logger.info("Run command {}".format(cmd))
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=None, shell=True)
            exit_code = p.wait()
            result = p.communicate()[0].decode("utf8")
        else:
            raise FileNotFoundError(self.path_to_rml_rules)

    def df_to_csv(self, df, dst):
        os.makedirs(dst, exist_ok=True)

        df = df.rename(
            columns={
                self.config.subject_column_name: "sub",
                self.config.predicate_column_name: "pred",
                self.config.object_column_name: "obj",
                self.config.references_column_name: "references",
            }
        )

        df["id"] = df.index

        df_obj = df.loc[:, ["obj"]]
        df_obj["id"] = df.index

        df_pred = df.loc[:, ["pred"]]
        df_pred["id"] = df.index

        df.to_csv(os.path.join(dst, "s.csv"), index=False)
        df_pred.to_csv(os.path.join(dst, "p.csv"), index=False)
        df_obj.to_csv(os.path.join(dst, "o.csv"), index=False)

        df_sub_taxon = df[
            [
                "sub",
                self.config.subject_name_column_name,
                self.config.subject_rank_column_name,
            ]
        ].rename(
            columns={
                "sub": "iri",
                self.config.subject_name_column_name: "scientific_name",
                self.config.subject_rank_column_name: "taxon_rank",
            }
        )
        df_obj_taxon = df[
            [
                "obj",
                self.config.object_name_column_name,
                self.config.object_rank_column_name,
            ]
        ].rename(
            columns={
                "obj": "iri",
                self.config.object_name_column_name: "scientific_name",
                self.config.object_rank_column_name: "taxon_rank",
            }
        )

        df_taxon = df_sub_taxon.append(df_obj_taxon, ignore_index=True).drop_duplicates(
            subset="iri"
        )

        df_taxon.to_csv(os.path.join(dst, "taxon.csv"), index=False)
