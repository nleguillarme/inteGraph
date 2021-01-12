from ..core import Transformer

# from ..linking import TripleLinkingEngine
from ..mapping.rml_mapper import RMLMappingEngine
from ..mapping.taxonomic_mapper import TaxonomicMapper
from ..mapping.vocabulary_mapper import VocabularyMapper

from ...utils.csv_helper import *
from ...utils.config_helper import read_config
from ...utils.file_helper import move_file_to_dir, clean_dir, get_basename_and_extension

import argparse
import logging
import os
import multiprocessing
from math import ceil
from os import listdir

from rdflib import Graph
from rdflib.term import BNode, URIRef


class CSV2RDF(Transformer):
    def __init__(self, config):
        Transformer.__init__(self, "csv2rdf_transformer")
        self.logger = logging.getLogger(__name__)
        self.config = config

        self.config.rdf_extension = ".nt"
        self.config.extracted_data_dir = os.path.join(
            self.config.output_dir, "extracted"
        )
        self.config.output_chunks_dir = os.path.join(self.config.output_dir, "chunks")
        self.config.output_triples_dir = os.path.join(self.config.output_dir, "triples")
        self.config.output_unreasoned_dir = os.path.join(
            self.config.output_dir, "graph"
        )  # TODO : change this if adding reasoner in the pipeline

        property_file = os.path.join(
            self.config.source_root_dir, "config", self.config.properties
        )
        self.properties = read_config(property_file)
        self.logger.info("Properties {}".format(self.properties))

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

        self.logger.info("New CSV2RDF Transformer with id {}".format(self.get_id()))

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
        f_output = self.get_output_filenames()
        self.merge_chunks(triples_files, f_output)

    def clean_output_dir(self, **kwargs):
        clean_dir(self.config.output_chunks_dir)
        clean_dir(self.config.output_triples_dir)
        clean_dir(self.config.output_unreasoned_dir)

    def get_chunk_reader(self):
        columns = None
        # columns = [
        #     self.properties.triplifier_conf.subject_column_name,
        #     self.properties.triplifier_conf.predicate_column_name,
        #     self.properties.triplifier_conf.object_column_name,
        #     self.properties.triplifier_conf.references_column_name,
        # ]
        filepath = self.get_input_filename()
        if filepath:
            self.logger.info("Fetched file {}".format(filepath))
            nb_records = get_nb_records_in_csv(filepath)
            if nb_records < self.config.num_processes:
                self.config.num_processes = 1
            dtype = str
            return get_csv_file_reader(
                csv_file=filepath,
                columns=columns,
                dtype=dtype,
                delimiter=self.properties.delimiter,
                chunksize=ceil(nb_records / self.config.num_processes),
            )
        else:
            raise ValueError(filepath)

    # Split a CSV file into a number of chunks = number of workers
    # and write the chunks to dedicated files
    def split_in_chunks(self, **kwargs):
        chunk_reader = self.get_chunk_reader()
        chunk_count = 0
        for df in chunk_reader:
            chunk_filename = self.get_chunk_filename(chunk_count)
            write(df, chunk_filename, sep=self.properties.delimiter)
            chunk_count += 1
        if chunk_count != self.get_nb_chunks():
            raise AssertionError(
                "Expected {} chunks, got {}".format(self.get_nb_chunks(), chunk_count)
            )
        filepath = self.get_input_filename()
        basename, ext = get_basename_and_extension(filepath)
        processed_dir = os.path.join(self.config.output_dir, "processed")
        clean_dir(processed_dir)
        os.rename(filepath, os.path.join(processed_dir, basename + ext))

    def map_taxonomic_entities(self, f_in, f_out, **kwargs):
        df = read(f_in, sep=self.properties.delimiter)
        matched_df, not_matched_df = self.taxonomic_mapper.map(df)
        basename, ext = get_basename_and_extension(f_in)
        invalid_data_filepath = os.path.join(
            self.config.output_chunks_dir, basename + "_invalid" + ext
        )
        write(matched_df, f_out, sep=self.properties.delimiter)
        if not_matched_df.shape[0]:
            write(not_matched_df, invalid_data_filepath, sep=self.properties.delimiter)

    def map_interactions(self, f_in, f_out, **kwargs):
        df = read(f_in, sep=self.properties.delimiter)
        df = self.interaction_mapper.map(df)
        write(df, f_out, sep=self.properties.delimiter)

    # Validate data and generate RDF triples from each observation
    def triplify_chunk(self, f_in, f_out, wdir, **kwargs):
        # TODO : consider reusing triplifier from ontology-data-pipeline when
        # interactions will be supported (see https://github.com/biocodellc/ontology-data-pipeline/issues/60)
        # In that case, entity linking will have to be performed as a previous step (outside the pipeline preferably)
        df = read(f_in, sep=self.properties.delimiter)
        self.triplifier.run_mapping(df, wdir, f_out)
        move_file_to_dir(
            os.path.join(wdir, os.path.basename(f_out)), self.config.output_triples_dir
        )

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
                    new_s = BNode(
                        value="{}_{}_{}".format(self.config.internal_id, n, s)
                    )
                if isinstance(o, BNode):
                    new_o = BNode(
                        value="{}_{}_{}".format(self.config.internal_id, n, o)
                    )
                g.add((new_s, p, new_o))
            n += 1
        g.serialize(destination=f_out, format="nt")

    def get_extracted_data_dir(self):
        return self.config.extracted_data_dir

    def get_chunk_filename(self, num_chunk):
        return os.path.join(
            self.config.output_chunks_dir,
            self.config.internal_id
            + "_{}".format(num_chunk)
            + self.properties.data_extension,
        )

    def get_mapped_chunk_filename(self, num_chunk):
        return os.path.join(
            self.config.output_chunks_dir,
            self.config.internal_id
            + "_{}_mapped".format(num_chunk)
            + self.properties.data_extension,
        )

    def get_triples_filename(self, num_chunk):
        return os.path.join(
            self.config.output_triples_dir,
            self.config.internal_id
            + "_{}".format(num_chunk)
            + self.config.rdf_extension,
        )

    def get_onto_mapping_working_dir_template(self, num_chunk):
        return os.path.join(
            self.config.output_triples_dir,
            self.config.internal_id + "_{}".format(num_chunk),
        )

    def get_output_filenames(self):
        basename = os.path.join(
            self.config.output_unreasoned_dir, self.config.internal_id
        )
        return basename + self.config.rdf_extension  # , basename + ".graph"

    def get_nb_chunks(self):
        return self.config.num_processes

    def get_input_filename(self):
        dir = self.get_extracted_data_dir()
        files = [
            f
            for f in listdir(dir)
            if os.path.isfile(os.path.join(dir, f))
            and "_processed" not in f
            and f[0] != "."
        ]
        if len(files) == 1:
            return os.path.join(self.config.extracted_data_dir, files[0])
        else:
            return None

    def with_interactions_mapping(self):
        return self.interaction_mapper != None


def main():
    parser = argparse.ArgumentParser(
        description="csv2rdf transformer command line interface."
    )

    parser.add_argument("cfg_file", help="YAML configuration file.")
    args = parser.parse_args()

    config = read_config(args.cfg_file)
    config.jars_location = "jars"
    config.run_on_localhost = True
    config.output_dir = os.path.join(config.source_root_dir, "output")
    process = CSV2RDF(config)
    process.run()


if __name__ == "__main__":
    main()
