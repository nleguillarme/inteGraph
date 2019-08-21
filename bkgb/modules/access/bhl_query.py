import bonobo
import os
from .pybhl.response import BHLResponse
from .pybhl.request import BHLRequest

class BHLQuery():

    def __init__(self, cfg):
        self.uri_prefix = "https://www.biodiversitylibrary.org/page/"
        self.output_dir = cfg['output_dir']
        self.max_results = int(cfg['max_results'])
        self.search_type = 'all' # search for all specified words
        self.bhl_type_map = {'Part':'PartID', 'Item':'ItemID'}
        self.bhl_type_op_map = {
            'Part':'GetPartMetadata',
            'Item':'GetItemMetadata',
        }

    def triple_2_search(self, s, p, o):
        """ First array contains terms to search for in the title.
            Second array contains terms to search for in the full text.
            TODO : create all possible combinations of s and o.
        """
        return [s, s+' '+o]

    def search_in_publications(self, search_terms):
        for page_id in self.search_for_name_in_publications(search_terms[0]):
            for page_id, text in self.search_for_name_in_page(search_terms[1], page_id):
                yield page_id, text


    def search_for_name_in_publications(self, sci_name):
        req = BHLRequest(op='GetNameMetadata', name=sci_name)
        res = req.get_response()
        cnt = 0
        for result in res.data['Result']:
            for title in result['Titles']:
                for item in title['Items']:
                    for page in item['Pages']:
                        if cnt < self.max_results:
                            cnt += 1
                            yield page['PageID']

    def search_for_name_in_page(self, sci_name, page_id):
        req = BHLRequest(op='GetPageMetadata', pageid=page_id, \
            ocr='t', names='t')
        res = req.get_response()
        for result in res.data['Result']:
            for name in result['Names']:
                if sci_name in name['NameConfirmed']:
                    yield page_id, result['OcrText']

    # def search_in_publications(self, search_terms):
    #     nb_pages = self.max_results // 200 # BHL API returns 200 results per page
    #     if self.max_results % 200 != 0:
    #         nb_pages += 1
    #     for title_search_term in search_terms[0]:
    #         for text_search_term in search_terms[1]:
    #             cnt = 0
    #             print("Search for {} in title, {} in text".format(title_search_term, text_search_term))
    #             for i in range(1, nb_pages+1):
    #                 req = BHLRequest(op='PublicationSearchAdvanced', \
    #                     title=title_search_term, titleop=self.search_type, \
    #                     text=text_search_term, textop=self.search_type, \
    #                     page=str(i))
    #                 res = req.get_response()
    #                 for result in res.data['Result']:
    #                     if cnt < self.max_results:
    #                         cnt += 1
    #                         print(result)

    # def search_in_title(self, search['Haliaeetus leucocephalus','Fulica americana']_terms):
    #     nb_pages = self.max_results // 200 # BHL API returns 200 results per page
    #     if self.max_results % 200 != 0:'Haliaeetus leucocephalus'
    #         nb_pages += 1
    #     for search_term in search_terms:
    #         cnt = 0
    #         for i in range(1, nb_pages+1):
    #             req = BHLRequest(op='PublicationSearchAdvanced', \
    #                 title=search_term, titleop=self.search_type, page=str(i))
    #             res = req.get_response()
    #             for result in res.data['Result']:
    #                 if cnt < self.max_results:
    #                     cnt += 1
    #                     print(result)
    #                     # return result
    #
    # def search_in_full_text(self, search_terms):
    #     nb_pages = self.max_results // 200 # BHL API returns 200 results per page
    #     if self.max_results % 200 != 0:
    #         nb_pages += 1
    #     for search_term in search_terms:
    #         cnt = 0
    #         for i in range(1, nb_pages+1):
    #             req = BHLRequest(op='PublicationSearch', searchterm=search_term, \
    #                 searchtype='F', page=str(i))
    #             res = req.get_response()
    #             for result in res.data['Result']:
    #                 if cnt < self.max_results:
    #                     cnt += 1
    #                     yield result
    #
    # def get_part_text(self, part):
    #     if part['BHLType'] == 'Part':
    #         args = {'id':part['PartID'], 'pages':'t'}
    #         req = BHLRequest(op='GetPartMetadata', **args)
    #         res = req.get_response()
    #         for page in res.data['Result'][0]['Pages']:
    #             args = {'pageid':page['PageID'], 'ocr':'t'}
    #             req = BHLRequest(op='GetPageMetadata', **args)
    #             text = req.get_response().data['Result'][0]['OcrText']
    #             yield page['PageUrl'], page['PageID'], text
    #
    # def get_item_text(self, item):
    #     if item['BHLType'] == 'Item':
    #         for search_term in self.search_terms:
    #             req = BHLRequest(op='PageSearch', itemid=item['ItemID'], text=search_term)
    #             res = req.get_response()
    #             for page in res.data['Result']:
    #                 text = page['OcrText']
    #                 yield page['PageUrl'], page['PageID'], text    res = req.get_response()
    #             for result in res.data['Result']:
    #                 if cnt < self.max_results:
    #                     cnt += 1
    #                     print(result)
    #                     # return result
    #
    # def search_in_full_text(self, search_terms):
    #     nb_pages = self.max_results // 200 # BHL API returns 200 results per page
    #     if self.max_results % 200 != 0:
    #         nb_pages += 1
    #     for search_term in search_terms:
    #         cnt = 0
    #         for i in range(1, nb_pages+1):
    #             req = BHLRequest(op='PublicationSearch', searchterm=search_term, \
    #                 searchtype='F', page=str(i))
    #             res = req.get_response()
    #             for result in res.data['Result']:
    #                 if cnt < self.max_results:
    #                     cnt += 1
    #                     yield result

    # def get_part_text(self, part):
    #     if part['BHLType'] == 'Part':
    #         args = {'id':part['PartID'], 'pages':'t'}
    #         req = BHLRequest(op='GetPartMetadata', **args)
    #         res = req.get_response()
    #         for page in res.data['Result'][0]['Pages']:
    #             args = {'pageid':page['PageID'], 'ocr':'t'}
    #             req = BHLRequest(op='GetPageMetadata', **args)
    #             text = req.get_response().data['Result'][0]['OcrText']
    #             yield page['PageUrl'], page['PageID'], textsearch_for_name_in_publications
    #
    # def get_item_text(self, item):
    #     if item['BHLType'] == 'Item':
    #         for search_term in self.search_terms:
    #             req = BHLRequest(op='PageSearch', itemid=item['ItemID'], text=search_term)
    #             res = req.get_response()
    #             for page in res.data['Result']:
    #                 text = page['OcrText']
    #                 yield page['PageUrl'], page['PageID'], text

    def save_text(self, id, text):
        uri = self.uri_prefix+str(id)
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        filename = os.path.join(self.output_dir, str(id)+'.txt')
        with open(filename, 'w') as f:
            f.write(uri+"\n")
            f.write(text)
            f.close()
            return NOT_MODIFIED

    def get_graph(self):#, **options):
        graph = bonobo.Graph()
        graph.add_chain(
            self.search_in_title,
            self.get_part_text,
            self.save_text,
            bonobo.PrettyPrinter(),
        )
        graph.add_chain(
            self.get_item_text,
            self.save_text,
            bonobo.PrettyPrinter(),
            _input=self.search_in_title,
        )
        return graph

if __name__ == '__main__':
    bhl_cfg = {'output_dir':'./corpus/bhl/', 'max_results':'100'}
    pipe = BHLQuery(bhl_cfg)
    search_terms = pipe.triple_2_search('Haliaeetus leucocephalus', None, 'Fulica americana')
    pipe.search_in_publications(search_terms)
    # bonobo.run(pipe.get_graph())
