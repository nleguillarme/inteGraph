# import bonobo
# from bonobo.config import use, Service, Configurable

from bkgb.utils.file_manager import decompress_file, delete_file
import yaml
import argparse
import os
import requests
import shutil
import magic
import logging

from rdflib import ConjunctiveGraph, URIRef, plugin, Graph
from rdflib.util import guess_format
from rdflib.store import Store

logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(filename='DumpImport.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

"""

The Triple/Quad Dump Importer is used to locally replicate dataset dumps from
the Web. The importer supports the following RDF serialization formats:
RDF/XML, N-Triples, N-Quads and Turtle.

"""

# def get_graph(job):#, **options):
#     graph = bonobo.Graph()
#     graph.add_chain(
#         job.extract,
#         job.transform,
#         job.load,ConjunctiveGraph
#         bonobo.Limit(10),
#         bonobo.PrettyPrinter(),
#     )
#     return graph


# def get_services():#**options):
#     http = requests.Session()
#     http.headers = {'User-Agent': 'Monkeys!'}
#     return {
#         'http': http
#     }

# Example of dump_import config file
# GloBiDumpImport.yml
#   internal_id : globi.1
#   data_source : GloBi
#   job_type : dump_import
#   dump_location : https://depot.globalbioticinteractions.org/snapshot/target/data/interactions.nq.gz

class DumpImport(): #Configurable):

    def __init__(self, cfg):
        self.output_dir = "./dumps/"
        self.prefix = cfg['prefix']
        self.internal_id = cfg['internal_id']
        self.data_source = cfg['data_source']
        self.dump_url = cfg['dump_location']
        self.output_file = self.internal_id+".nq"
        self.provenance_uri = self.prefix+"/graph/"+self.internal_id

    def download_dump(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        dump_file = self.dump_url.split('/')[-1]
        dump_file = os.path.join(self.output_dir, local_filename)
        with requests.get(self.dump_url, allow_redirects=True, stream=True) as r:
            with open(dump_file, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        return dump_file

    def decompress_archive(self, dump_file):
        m_res = magic.detect_from_filename(dump_file)
        if ("archive" in m_res.name) or ("compressed" in m_res.name):
            dump_file = decompress_file(dump_file)
        return dump_file

    def convert_to_nquads(self, dump_file):
        logging.debug('Convert {} to nquads'.format(dump_file))
        format = guess_format(dump_file)
        g = ConjunctiveGraph()
        g.parse(dump_file, format=format, publicID=URIRef(self.provenance_uri))
        f_out = os.path.join(self.output_dir, self.output_file)
        g.serialize(destination=f_out, format='nquads')

    def run(self):
        # dump_file = self.download_dump()
        # dump_file = self.decompress_archive(dump_file)
        logging.debug('Enter DumpImport.run')
        dump_file = "/home/leguilln/workspace/biodiv-kg-builder/dumps/interactions.nq"
        self.convert_to_nquads(dump_file)
        logging.debug('Leave DumpImport.run')




if __name__ == '__main__':

    parser = argparse.ArgumentParser("dump_import")
    parser.add_argument("config", help="Path to YAML configuration file.", type=str)
    args = parser.parse_args()

    # Parse configuration file
    with open(args.config, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        job = DumpImport(cfg)
        job.run()

        #parser = bonobo.get_argument_parser()
        #with bonobo.parse_args(parser) as options:
        # bonobo.run(
        #     get_graph(job),# **options),
        #     services=get_services()#**options)
        # )
