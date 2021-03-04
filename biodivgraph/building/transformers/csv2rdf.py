from ..core import Transformer

# from ..linking import TripleLinkingEngine
from ..mapping.rml_mapper import RMLMappingEngine
from ..mapping.taxonomic_mapper import TaxonomicMapper
from ..mapping.ontology_mapper import OntologyMapper

from ...utils.csv_helper import *
from ...utils.config_helper import read_config
from ...utils.file_helper import move_file_to_dir, clean_dir, get_basename_and_extension

import argparse
import logging
import os
import multiprocessing
from math import ceil
from os import listdir

from rdflib import Graph, ConjunctiveGraph
from rdflib.term import BNode, URIRef


class CSV2RDF(Transformer):
    def __init__(self, config):
        Transformer.__init__(self, "csv2rdf_transformer")
        self.logger = logging.getLogger(__name__)
        self.cfg = config

        self.cfg.rdf_format = "nq"  # f".{self.cfg.rdf_format}"

        # Input directory
        self.cfg.fetched_data_dir = os.path.join(self.cfg.output_dir, "fetched")

        # Output directories
        self.cfg.processed_dir = os.path.join(self.cfg.output_dir, "processed")
        self.cfg.chunks_dir = os.path.join(self.cfg.output_dir, "chunks")
        self.cfg.triples_dir = os.path.join(self.cfg.output_dir, "triples")
        self.cfg.unreasoned_dir = os.path.join(
            self.cfg.output_dir, "graph"
        )  # TODO : change this if adding reasoner in the pipeline

        # Read transformer properties
        self.properties = read_config(
            os.path.join(self.cfg.source_root_dir, "config", self.cfg.properties)
        )
        self.logger.debug(f"{self.get_id()} : properties = {self.properties}")

        # Create taxonomic mapper
        self.properties.taxonomic_mapper_conf.run_on_localhost = (
            self.cfg.run_on_localhost
        )
        self.taxonomic_mapper = None
        if "taxonomic_mapper_conf" in self.properties:
            self.taxonomic_mapper = TaxonomicMapper(
                self.properties.taxonomic_mapper_conf
            )

        # Create interaction mapper
        self.entity_mapper = None
        if (
            "entity_mapper_conf" in self.properties
            and "ontology_file" in self.properties.entity_mapper_conf
        ):
            self.properties.entity_mapper_conf.ontology_file = os.path.join(
                self.cfg.source_root_dir,
                self.properties.entity_mapper_conf.ontology_file,
            )
            # for column_config in self.properties.entity_mapper_conf.columns:
            #     column_config.mapping_file = os.path.join(
            #         self.cfg.source_root_dir, column_config.mapping_file
            #     )
            self.entity_mapper = OntologyMapper(self.properties.entity_mapper_conf)
            self.entity_mapper.load_ontology()
            # VocabularyMapper(self.properties.entity_mapper_conf)

        # Create triplifier
        self.properties.triplifier_conf.ontological_mapping_file = os.path.join(
            self.cfg.source_root_dir,
            self.properties.triplifier_conf.ontological_mapping_file,
        )
        self.triplifier = RMLMappingEngine(self.properties.triplifier_conf)

        self.logger.info("New CSV2RDF Transformer with id {}".format(self.get_id()))

    def run(self):
        self.clean_output_dir()
        self.split_in_chunks()
        triples_files = []
        for chunk_num in range(self.get_nb_chunks()):
            f_in = self.get_path_to_chunk(chunk_num)
            f_out = self.get_path_to_mapped_chunk(chunk_num)
            f_not_matched = self.get_path_to_invalid_data(chunk_num)
            drop_not_matched = not self.with_other_entities_mapping()

            self.map_taxonomic_entities(
                f_in,
                f_out,
                drop_not_matched=drop_not_matched,
                f_not_matched=f_not_matched,
            )
            if self.map_other_entities():
                f_in = f_out
                self.map_other(
                    f_in, f_out, drop_not_matched=True, f_not_matched=f_not_matched
                )

            f_in = f_out
            f_out = self.get_path_to_triples(chunk_num)
            wdir = self.get_mapping_working_dir(chunk_num)
            self.triplify_chunk(f_in, f_out, wdir)
            triples_files.append(f_out)

        f_out = self.get_path_to_graph()
        self.merge_chunks(triples_files, f_out)

    def clean_output_dir(self, **kwargs):
        clean_dir(self.cfg.chunks_dir)
        clean_dir(self.cfg.triples_dir)
        clean_dir(self.cfg.unreasoned_dir)

    def get_chunk_reader(self):
        columns = None
        # columns = [
        #     self.properties.triplifier_conf.subject_column_name,
        #     self.properties.triplifier_conf.predicate_column_name,
        #     self.properties.triplifier_conf.object_column_name,
        #     self.properties.triplifier_conf.references_column_name,
        # ]
        filepath = self.get_path_to_input_data()
        if filepath:
            self.logger.info(f"Start transformer with file {filepath}")
            nb_records = get_nb_records_in_csv(filepath)
            if nb_records < self.cfg.num_processes:
                self.cfg.num_processes = 1
            chunksize = ceil(nb_records / self.cfg.num_processes)
            self.logger.debug(
                f"Create {self.cfg.num_processes} chunks of size {chunksize}"
            )
            dtype = str
            return get_csv_file_reader(
                csv_file=filepath,
                columns=columns,
                dtype=dtype,
                delimiter=self.properties.delimiter,
                chunksize=chunksize,
            )
        else:
            raise ValueError(filepath)

    # Split a CSV file into a number of chunks = number of workers
    # and write the chunks to dedicated files
    def split_in_chunks(self, **kwargs):
        chunk_reader = self.get_chunk_reader()
        chunk_count = 0
        # Write chunks to chunks directory
        for df in chunk_reader:
            chunk_filepath = self.get_path_to_chunk(chunk_count)
            write(df, chunk_filepath, sep=self.properties.delimiter)
            chunk_count += 1
        if chunk_count != self.get_nb_chunks():
            raise AssertionError(
                f"Expected {self.get_nb_chunks()} chunks, got {chunk_count}"
            )
        # Move fetched data to processed directory
        filepath = self.get_path_to_input_data()
        filename = os.path.basename(filepath)
        clean_dir(self.cfg.processed_dir)
        os.rename(filepath, os.path.join(self.cfg.processed_dir, filename))

    def map_taxonomic_entities(self, f_in, f_out, **kwargs):
        df = read(f_in, sep=self.properties.delimiter)
        df, uri_colnames = self.taxonomic_mapper.map(df)
        if "drop_not_matched" in kwargs and kwargs["drop_not_matched"]:
            matched_df = df.dropna(subset=uri_colnames)
            write(matched_df, f_out, sep=self.properties.delimiter)
            if "f_not_matched" in kwargs:
                not_matched_df = df[df[uri_colnames].isnull().any(axis=1)]
                write(
                    not_matched_df,
                    kwargs["f_not_matched"],
                    sep=self.properties.delimiter,
                )
        else:
            write(df, f_out, sep=self.properties.delimiter)

    def map_other_entities(self, f_in, f_out, **kwargs):
        df = read(f_in, sep=self.properties.delimiter)
        df, uri_colnames = self.entity_mapper.map(df)
        if "drop_not_matched" in kwargs and kwargs["drop_not_matched"]:
            matched_df = df.dropna(subset=uri_colnames)
            write(matched_df, f_out, sep=self.properties.delimiter)
            if "f_not_matched" in kwargs:
                not_matched_df = df[df[uri_colnames].isnull().any(axis=1)]
                write(
                    not_matched_df,
                    kwargs["f_not_matched"],
                    sep=self.properties.delimiter,
                )
        else:
            write(df, f_out, sep=self.properties.delimiter)

    # Validate data and generate RDF triples from each observation
    def triplify(self, f_in, f_out, wdir, **kwargs):
        # TODO : consider reusing triplifier from ontology-data-pipeline when
        # interactions will be supported (see https://github.com/biocodellc/ontology-data-pipeline/issues/60)
        # In that case, entity linking will have to be performed as a previous step (outside the pipeline preferably)
        df = read(f_in, sep=self.properties.delimiter)
        self.triplifier.run_mapping(df, wdir, f_out)
        move_file_to_dir(
            os.path.join(wdir, os.path.basename(f_out)), self.cfg.triples_dir
        )

    # Merge RDF graphs
    def merge(self, f_in_list, f_out, **kwargs):
        g = Graph()  # ConjunctiveGraph(identifier=self.cfg.graph_uri)
        n = 0
        for f_in in f_in_list:
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
                    new_s = BNode(value="{}_{}_{}".format(self.cfg.internal_id, n, s))
                if isinstance(o, BNode):
                    new_o = BNode(value="{}_{}_{}".format(self.cfg.internal_id, n, o))
                g.add((new_s, p, new_o))
            n += 1
        g.serialize(destination=f_out, format="ntriples")

    def get_path_to_input_data(self):
        dir = self.get_fetched_data_dir()
        files = [
            f
            for f in listdir(dir)
            if os.path.isfile(os.path.join(dir, f))
            and "_processed" not in f
            and not f.startswith(".")
        ]
        if len(files) != 1:
            raise FileNotFoundError(f"No or multiple files found in {dir} : {files}")
        return os.path.join(dir, files[0])

    def get_path_to_chunk(self, num_chunk):
        return os.path.join(
            self.cfg.chunks_dir,
            self.cfg.internal_id
            + "_{}".format(num_chunk)
            + self.properties.data_extension,
        )

    def get_path_to_mapped_chunk(self, num_chunk):
        return os.path.join(
            self.cfg.chunks_dir,
            self.cfg.internal_id
            + "_{}_mapped".format(num_chunk)
            + self.properties.data_extension,
        )

    def get_path_to_invalid_data(self, num_chunk):
        return os.path.join(
            self.cfg.chunks_dir,
            self.cfg.internal_id
            + f"_{num_chunk}_invalid"
            + self.properties.data_extension,
        )

    def get_path_to_triples(self, num_chunk):
        return os.path.join(
            self.cfg.triples_dir,
            self.cfg.internal_id + f"_{num_chunk}.{self.cfg.rdf_format}",
        )

    def get_mapping_working_dir(self, num_chunk):
        return os.path.join(
            self.cfg.triples_dir, self.cfg.internal_id + f"_{num_chunk}"
        )

    def get_path_to_graph(self):
        basename = os.path.join(self.cfg.unreasoned_dir, self.cfg.internal_id)
        return os.path.join(
            self.cfg.unreasoned_dir, self.cfg.internal_id + f".{self.cfg.rdf_format}"
        )

    def get_fetched_data_dir(self):
        return self.cfg.fetched_data_dir

    def get_nb_chunks(self):
        return self.cfg.num_processes

    def with_taxonomic_entities_mapping(self):
        return self.taxonomic_mapper != None

    def with_other_entities_mapping(self):
        return self.entity_mapper != None


def main():
    parser = argparse.ArgumentParser(
        description="csv2rdf transformer command line interface."
    )

    parser.add_argument("cfg_file", help="YAML configuration file.")
    args = parser.parse_args()

    config = read_config(args.cfg_file)
    # config.jars_location = "jars"
    config.run_on_localhost = True
    config.output_dir = os.path.join(config.source_root_dir, "output")
    process = CSV2RDF(config)
    process.run()


if __name__ == "__main__":
    main()
