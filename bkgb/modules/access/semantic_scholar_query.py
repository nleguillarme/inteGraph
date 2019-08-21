from bonobo.constants import NOT_MODIFIED
import os
import bonobo
import json
import time

import requests
from bs4 import BeautifulSoup
import os,sys

class SemanticScolarQuery():

    def __init__(self, cfg):
        self.uri_prefix = "https://www.semanticscholar.org/paper/"
        self.output_dir = cfg['output_dir']
        self.api_prefix = "https://api.semanticscholar.org/v1/paper/"

    def build_query(self, doi):
        return self.api_prefix + doi

    def get_paper_data(self, doi, query):
        url = self.build_query(doi)
        resp = requests.get(url=url)
        if resp.status_code == 200:
            data = resp.json()
            if 'title' in data:
                title = data['title']
            elif 'paperTitle' in data:
                title = data['paperTitle']
            if 'abstract' in data:
                abstract = data['abstract']
            elif 'paperAbstract' in data:
                abstract = data['paperAbstract']
            if abstract != None:
                yield doi, title, abstract, query
        elif resp.status_code == 429: #The API is freely available, but enforces a rate limit.
            time.sleep(1) # Please reduce your call rate and retry if you recieve HTTP status 429 responses.
            get_paper_data(self, doi, query)

    def save_text(self, id, *args):
        id = id.replace('/', '_')
        uri = self.uri_prefix+str(id)
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        filename = os.path.join(self.output_dir, str(id)+'.txt')
        with open(filename, 'w+') as f:
            f.write(uri+"\n")
            for arg in args:
                f.write(str(arg)+"\n")
            f.close()
        return NOT_MODIFIED

if __name__ == '__main__':
    pass
    # sscholar_cfg = {'output_dir':'./corpus/sscholar/', \
    #     'max_results':'10', 'archive_path':'/home/leguilln/workspace/biodiv-kg-builder/dumps/sample-S2-records'}
    # pipe = SemanticScolarQuery(sscholar_cfg)
    # pipe.extract_paper_data()
