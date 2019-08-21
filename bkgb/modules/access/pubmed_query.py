import bonobo
from bonobo.constants import NOT_MODIFIED
import os
from pymed import PubMed

class PubMedQuery():

    def __init__(self, cfg):
        self.uri_prefix = "https://www.ncbi.nlm.nih.gov/pubmed/"
        self.output_dir = cfg['output_dir']
        self.max_results = int(cfg['max_results'])
        self.pubmed = PubMed(tool="BKGB", \
            email="nicolas.leguillarme@univ-grenoble-alpes.fr")

    def build_query(self, search_terms):
        query = ''
        nb_terms = len(search_terms)
        for j in range(0,nb_terms):
            alternatives = search_terms[j]
            query_part = '('
            nb_alternatives = len(alternatives)
            for i in range(0, nb_alternatives):
                query_part += '({}[Title/Abstract])'.format(alternatives[i])
                if i != nb_alternatives-1:
                    query_part += ' OR '
                else:
                    query_part += ')'
            query += query_part
            if j != nb_terms-1:
                query += ' AND '
        return query

    def search_in_abstract(self, search_terms):

        query = self.build_query(search_terms)
        # Execute the query against the API
        results = self.pubmed.query(query, max_results=self.max_results)

        # Loop over the retrieved articles
        for article in results:
            article_id = article.pubmed_id
            abstract = article.abstract
            if ("\n" not in article_id): # Workaround for id parsing bug in pymed
                yield article_id, abstract

    def save_text(self, id, text):
        uri = self.uri_prefix+str(id)
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        filename = os.path.join(self.output_dir, str(id)+'.txt')
        with open(filename, 'w+') as f:
            f.write(uri+"\n")
            f.write(text)
            f.close()
        return NOT_MODIFIED

    def triple_2_search(self, s, p, o):
        return [[s], [o]]

    def get_graph(self):
        graph = bonobo.Graph()
        graph.add_chain(
            self.search_in_abstract,
            self.save_text,
            bonobo.PrettyPrinter(),
        )
        return graph

if __name__ == '__main__':
    pubmed_cfg = {'output_dir':'./corpus/pubmed/', 'max_results':'10'}
    pipe = PubMedQuery(cfg)
    # cfg = {'search_terms':[['Haliaeetus leucocephalus'], ['prey', 'diet']], \
    #     'max_results':'10'}
    # bonobo.run(pipe.get_graph())
