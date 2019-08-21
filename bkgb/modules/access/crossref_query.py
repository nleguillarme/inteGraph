import requests

class CrossRefQuery():

    def __init__(self, cfg):
        self.api_prefix = "http://api.crossref.org/works/"
        self.api_suffix = "?has-full-text:true"
        self.cache = {}

    def build_query(self, doi):
        return self.api_prefix + doi + self.api_suffix

    def get_full_text_url(self, doi, query=()):
        if doi in self.cache:
            yield self.cache[doi], query
        else:
            doi = doi.replace("_","/")
            url = self.build_query(doi)
            resp = requests.get(url=url)
            if resp.status_code == 200:
                data = resp.json()
                if 'message' in data and 'link' in data['message']:
                    links = data['message']['link']
                    for link in links:
                        if link['content-type'] == 'application/pdf':
                            resp = requests.get(link['URL'])
                            if resp.status_code == 200 and resp.headers['content-type'] == 'application/pdf':
                                self.cache[doi] = link['URL']
                                yield link['URL'], query
            else:
                print("DOI", doi, "not found")

if __name__ == '__main__':
    cfg = {}
    req = CrossRefQuery(cfg)
    req.get_paper_full_text(doi="10.1155/2014/413629")
