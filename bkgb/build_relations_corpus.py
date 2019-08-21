import bonobo
import os

from modules.access.sparql_endpoint_query import SPARQLEndpointQuery
from modules.access.pubmed_query import PubMedQuery
from modules.access.bhl_query import BHLQuery
from modules.access.semantic_scholar_query import SemanticScolarQuery
from modules.access.globi_query import GlobiQuery
from modules.access.crossref_query import CrossRefQuery

from utils.name_resolver import uri_2_name

class RECorpusBuilder():

    def __init__(self):
        endpoint_cfg = {'endpoint':'http://localhost:3030/interactions/sparql'}
        self.endpoint = SPARQLEndpointQuery(endpoint_cfg)

        pubmed_cfg = {'output_dir':'./corpus/pubmed/', 'max_results':'50'}
        self.pubmed = PubMedQuery(pubmed_cfg)

        bhl_cfg = {'output_dir':'./corpus/bhl/', 'max_results':'1000'}
        self.bhl = BHLQuery(bhl_cfg)

        sscholar_cfg = {'output_dir':'./corpus/scholar/'}
        self.sscholar = SemanticScolarQuery(sscholar_cfg)

        globi_cfg = {}
        self.globi = GlobiQuery(globi_cfg)

        crossref_cfg = {}
        self.crossref = CrossRefQuery(crossref_cfg)

        self.urls = set([])
        self.output_dir = "./corpus/re"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def extract_relation_uri(self):
        yield "<http://purl.obolibrary.org/obo/RO_0002470>" # "eats"

    def uris_2_names(self, s, p, o):
        s_name = uri_2_name(s)
        o_name = uri_2_name(o)
        return s_name, p, o_name

    def save_urls(self, url, query):
        rel_uri = query[1]
        rel_id = os.path.basename(rel_uri[:-1])
        filename = os.path.join(self.output_dir, rel_id)

        tuple = (url, query[0], query[2])
        if tuple not in self.urls:
            self.urls.add(tuple)
            with open(filename, 'a+') as f:
                print("Write", tuple)
                f.write(url+","+query[0]+","+query[2]+"\n")

    def get_graph(self):
        graph = bonobo.Graph()
        graph.add_chain(
            self.extract_relation_uri, # Get relation URI
            self.endpoint.query_endpoint, # Get all entities involved in the relation
            self.globi.get_interaction_sources, # Get source paper for this interaction
            self.crossref.get_full_text_url,
            self.save_urls
        )
        return graph

if __name__ == '__main__':
    pipe = RECorpusBuilder()
    bonobo.run(pipe.get_graph())
