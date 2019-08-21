from SPARQLWrapper import SPARQLWrapper, JSON

class SPARQLEndpointQuery():

    def __init__(self, cfg):
        self.endpoint = cfg['endpoint']
        self.sparql = SPARQLWrapper(self.endpoint)
        self.sparql.setReturnFormat(JSON)

    def build_query(self, rel_uri):
        query = """
                SELECT ?taxid1 ?taxid2
                WHERE {
                    ?s %s ?o .
                    ?s <http://purl.obolibrary.org/obo/RO_0002350> ?taxon1 .
                    ?o <http://purl.obolibrary.org/obo/RO_0002350> ?taxon2 .
                    ?taxon1 <http://www.w3.org/2002/07/owl#sameAs> ?taxid1 .
                    ?taxon2 <http://www.w3.org/2002/07/owl#sameAs> ?taxid2 .
                    FILTER regex(str(?taxid1), "http://www.gbif.org/species/") .
                    FILTER regex(str(?taxid2), "http://www.gbif.org/species/") .
                }
            """ % rel_uri
        return query

    def query_endpoint(self, rel_uri):
        query = self.build_query(rel_uri)
        self.sparql.setQuery(query)

        # Execute the query against the SQPARQL endpoint
        results = self.sparql.query().convert()

        # Loop over the results
        for result in results["results"]["bindings"]:
            s = '<{}>'.format(result['taxid1']['value'])
            o = '<{}>'.format(result['taxid2']['value'])
            yield s, rel_uri, o

if __name__ == '__main__':
    cfg = {'endpoint': 'http://localhost:3030/interactions/sparql'}
    request = SPARQLEndpointQuery(cfg)
    for res in request.query_endpoint("<http://purl.obolibrary.org/obo/RO_0002470>"):
        print(res)
