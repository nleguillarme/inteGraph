from ..core import Service

# from ..linking import TripleLinkingEngine
from ..mapping.rml_mapper import RMLMappingEngine
from ..mapping.taxonomic_mapper import TaxonomicMapper
from ..mapping.vocabulary_mapper import VocabularyMapper

from ...utils.csv_helper import *
from ...utils.config_helper import read_config
from ...utils.file_helper import move_file_to_dir, clean_dir

import argparse
import logging
import os
import multiprocessing
from math import ceil

from rdflib import Graph
from rdflib.term import BNode, URIRef


class CSV2RDF(Service):
    def __init__(self, config):
        Service.__init__(self, config.internal_id)
        self.logger = logging.getLogger(__name__)
        self.config = config

        # self.rootDir = cfg["rootDir"]
        self.config.rdf_extension = ".nt"
        self.config.output_dir = os.path.join(self.config.source_root_dir, "output")
        self.config.output_chunks_dir = os.path.join(self.config.output_dir, "chunks")
        self.config.output_triples_dir = os.path.join(self.config.output_dir, "triples")
        self.config.output_unreasoned_dir = os.path.join(
            self.config.output_dir,
            "graph"
            # self.config.rootDir, cfg["dumpDirectory"]
        )  # TODO : change this if adding reasoner in the pipeline

        property_file = os.path.join(
            self.config.source_root_dir, "config", self.config.properties
        )
        self.properties = read_config(property_file)
        self.logger.info("Properties {}".format(self.properties))

        # self.properties["rootDir"] = self.rootDir
        # self.linker = TripleLinkingEngine().init_from_dict(self.properties)

        self.properties.taxonomic_mapper_conf.run_on_localhost = (
            self.config.run_on_localhost
        )
        self.taxonomic_mapper = TaxonomicMapper(self.properties.taxonomic_mapper_conf)

        self.interaction_mapper = None
        if "interaction_mapping_file" in self.properties.triplifier_conf:
            self.properties.triplifier_conf.interaction_mapping_file = os.path.join(
                self.config.source_root_dir,
                self.properties.triplifier_conf.interaction_mapping_file,
            )
            self.interaction_mapper = VocabularyMapper(self.properties.triplifier_conf)

        self.properties.triplifier_conf.ontological_mapping_file = os.path.join(
            self.config.source_root_dir,
            self.properties.triplifier_conf.ontological_mapping_file,
        )
        self.triplifier = RMLMappingEngine(
            self.properties.triplifier_conf,
            self.config.output_triples_dir,
            self.config.jars_location,
        )

        self.logger.info("New CSV2RDF service with id {}".format(self.get_id()))

    def run(self):
        self.clean_output_dir()
        self.split_in_chunks()
        triples_files = []
        for chunk_num in range(self.get_nb_chunks()):
            f_in = self.get_chunk_filename(chunk_num)
            f_out = self.get_mapped_chunk_filename(chunk_num)
            self.map_taxonomic_entities(f_in, f_out)

            if self.with_interactions_mapping():
                f_in = f_out
                self.map_interactions(f_in, f_out)

            f_in = f_out
            f_out = self.get_triples_filename(chunk_num)
            wdir = self.get_onto_mapping_working_dir_template(chunk_num)
            self.triplify_chunk(f_in, f_out, wdir)

            triples_files.append(f_out)
        f_output, f_graph = self.get_output_filenames()
        self.merge_chunks(triples_files, f_output)
        self.set_graph_dst(f_graph)

    def clean_output_dir(self, **kwargs):
        clean_dir(self.config.output_dir)
        clean_dir(self.config.output_chunks_dir)
        clean_dir(self.config.output_triples_dir)
        clean_dir(self.config.output_unreasoned_dir)

    def get_chunk_reader(self):
        # columns = [
        #     self.properties.triplifier_conf.subject_column_name,
        #     self.properties.triplifier_conf.predicate_column_name,
        #     self.properties.triplifier_conf.object_column_name,
        #     self.properties.triplifier_conf.references_column_name,
        # ]
        columns = None
        filepath = os.path.join(self.config.source_root_dir, self.properties.data_file)
        nb_records = get_nb_records_in_csv(filepath)
        return get_csv_file_reader(
            csv_file=filepath,
            columns=columns,
            delimiter=self.properties["delimiter"],
            chunksize=ceil(nb_records / self.config.num_processes),
        )

    # Split a CSV file into a number of chunks = number of workers
    # and write the chunks to dedicated files
    def split_in_chunks(self, **kwargs):
        chunk_reader = self.get_chunk_reader()
        chunk_count = 0
        for df in chunk_reader:
            chunk_filename = self.get_chunk_filename(chunk_count)
            write(df, chunk_filename)
            chunk_count += 1
        if chunk_count != self.get_nb_chunks():
            raise AssertionError(
                "Expected {} chunks, got {}".format(self.get_nb_chunks(), chunk_count)
            )

    def map_taxonomic_entities(self, f_in, f_out, **kwargs):
        df = read(f_in)
        df = self.taxonomic_mapper.map(df)
        write(df, f_out)

    def map_interactions(self, f_in, f_out, **kwargs):
        df = read(f_in)
        df = self.interaction_mapper.map(df)
        write(df, f_out)

    # Validate data and generate RDF triples from each observation
    def triplify_chunk(self, f_in, f_out, wdir, **kwargs):
        # TODO : consider reusing triplifier from ontology-data-pipeline when
        # interactions will be supported (see https://github.com/biocodellc/ontology-data-pipeline/issues/60)
        # In that case, entity linking will have to be performed as a previous step (outside the pipeline preferably)
        df = read(f_in)  # .dropna()
        self.triplifier.run_mapping(df, wdir, f_out)
        move_file_to_dir(
            os.path.join(wdir, os.path.basename(f_out)), self.config.output_triples_dir
        )
        # move_file_to_dir(f_in, os.path.join(self.tmp_dir, "complete"))

    # Merge RDF graphs
    def merge_chunks(self, f_list_in, f_out, **kwargs):
        g = Graph()
        n = 0
        for f_in in f_list_in:
            self.logger.info("Merge graph {}".format(f_in))
            tmp_g = Graph()
            tmp_g.parse(f_in, format="nquads")
            # A workaround for blank nodes collisions during graph merging
            # Prefix blank nodes with the name of the graph and the number of the chunk
            # See : https://rdflib.readthedocs.io/en/stable/merging.html
            for s, p, o in tmp_g:
                new_s = s
                new_o = o
                if isinstance(s, BNode):
                    new_s = BNode(value="{}_g{}_{}".format(self.get_id(), n, s))
                if isinstance(o, BNode):
                    new_o = BNode(value="{}_g{}_{}".format(self.get_id(), n, o))
                # if isinstance(s, URIRef):
                #     new_s = URIRef(value="{}_g{}_{}".format(self.get_id(), n, s))
                # if isinstance(o, URIRef):
                #     new_o = URIRef(value="{}_g{}_{}".format(self.get_id(), n, o))
                g.add((new_s, p, new_o))
            n += 1
        g.serialize(destination=f_out, format="nt")

    def set_graph_dst(self, f_name, **kwargs):
        with open(f_name, "wt") as f:
            f.write(self.get_id())
            f.close()

    # def create_report(self, df_ref, df):
    #     null_rows = df.isnull().any(axis=1)
    #     null_df = df_ref[null_rows]
    #     with open(os.path.join(self.reports_dir, str(self.job_id) + ".rep"), "wt") as f:
    #         null_df.to_string(f)
    #         f.close()

    def get_chunk_filename(self, num_chunk):
        return os.path.join(
            self.config.output_chunks_dir,
            self.get_id() + "_{}".format(num_chunk) + self.properties.data_extension,
        )

    def get_mapped_chunk_filename(self, num_chunk):
        return os.path.join(
            self.config.output_chunks_dir,
            self.get_id()
            + "_mapped_{}".format(num_chunk)
            + self.properties.data_extension,
        )

    def get_triples_filename(self, num_chunk):
        return os.path.join(
            self.config.output_triples_dir,
            self.get_id() + "_{}".format(num_chunk) + self.config.rdf_extension,
        )

    def get_onto_mapping_working_dir_template(self, num_chunk):
        return os.path.join(
            self.config.output_triples_dir, self.get_id() + "_{}".format(num_chunk)
        )

    def get_output_filenames(self):
        basename = os.path.join(self.config.output_unreasoned_dir, self.get_id())
        return basename + self.config.rdf_extension, basename + ".graph"

    def get_nb_chunks(self):
        return self.config.num_processes

    def with_interactions_mapping(self):
        return self.interaction_mapper != None


def main():
    parser = argparse.ArgumentParser(
        description="csv2rdf pipeline command line interface."
    )

    parser.add_argument("cfg_file", help="YAML configuration file.")
    parser.add_argument(
        "--log_file",
        help="log all output to a log.txt file in the output_dir. default is to log output to the console",
        action="store_true",
    )
    parser.add_argument(
        "-v", "--verbose", help="verbose logging output", action="store_true"
    )
    parser.add_argument(
        "--num_processes",
        help="number of process to use for parallel processing of data. Defaults to cpu_count of the machine",
        type=int,
        default=multiprocessing.cpu_count(),
    )
    args = parser.parse_args()

    config = read_config(args.cfg_file)
    config.jars_location = "jars"
    config.run_on_localhost = True
    process = CSV2RDF(config)
    process.run()


if __name__ == "__main__":
    main()
