import requests

from utils.name_resolver import uri_2_name

class GlobiQuery():

    def __init__(self, cfg):
        self.api_prefix = "https://api.globalbioticinteractions.org/"
        # self.inter_types = {}
        # self.get_supported_interaction_types()

        # self.output_dir = cfg['output_dir']
        # self.archive = cfg['archive_path']
        # self.max_results = int(cfg['max_results'])

    # def triple_2_search(self, s, p, o):
    #     return s, p, o

    # def get_supported_interaction_types(self):
    #     url = self.api_prefix+'interactionTypes'
    #     resp = requests.get(url=url)
    #     data = resp.json()
    #     for type in data:
    #         self.inter_types[data[type]['termIRI']] = type

    def build_query(self, s, p, o):
        rel_type = p[1:-1]
        query = self.api_prefix + 'interaction?includeObservations=true'
        query += '&field=study_doi'
        query += '&interactionType={}'.format(rel_type)
        query += '&sourceTaxon={}&targetTaxon={}'.format(s, o)
        return query

    def get_interaction_sources(self, s, p, o):
        s_name = uri_2_name(s)
        o_name = uri_2_name(o)
        url = self.build_query(s_name, p, o_name)
        resp = requests.get(url=url)
        data = resp.json()
        dois = set([item for sublist in data['data'] for item in sublist if item != None])
        for doi in dois:
            yield doi, (s,p,o)

    def get_common_names(self, sci_name):
        url = self.api_prefix+'findCloseMatchesForTaxon/'+sci_name
        resp = requests.get(url=url)
        data = resp.json()
        names_dict = {}
        if data != {}:
            if 'data' in data:
                for taxon in data['data']:
                    taxon_name = taxon[0]
                    taxon_common_names = taxon[1]
                    if taxon_name == sci_name and taxon_common_names != None:
                        names = taxon_common_names.split('|')
                        for name in names:
                            split_name = name.strip().split('@')
                            if len(split_name) == 2:
                                names_dict[split_name[1]] = split_name[0].strip()
        return names_dict

if __name__ == '__main__':
    globi_cfg = {}
    # globi_cfg = {'output_dir':'./corpus/sscholar/', \
    #     'max_results':'10', 'archive_path':'/home/leguilln/wor(Enhydra lutris)kspace/biodiv-kg-builder/dumps/sample-S2-records'}
    pipe = GlobiQuery(globi_cfg)
    # pipe.get_supported_interaction_types()
    # pipe.get_interaction_sources('Enhydra lutris', 'http://purl.obolibrary.org/obo/RO_0002470', 'Strongylocentrotus')
    pipe.get_common_names('Enhydra lutris')
