import bonobo
from bonobo.constants import NOT_MODIFIED
import os
import re
import requests
import string
from tika import parser
from pygbif import species
from ete3 import NCBITaxa
from ...utils.name_resolver import name_2_taxid
from ..access.globi_query import GlobiQuery

def left_strip(token):
    """Takes a token and strips non alpha characters off the left. It
    returns the stripped string and the number of characters it stripped.

    Arguments:
    token -- a one word token which might have trailing non alpha characters

    """
    i = 0
    while(i < len(token)):
        if(not token[i].isalpha()):
            i = i + 1
        else:
            break
    if(i == len(token)):
        return('', 0)
    return(token[i:], i)

class NamedEntityRecognition():
    """
        Input : raw text
        Output : a list of named entities, including :
            - their accepted scientific name
            - their position in the text
            - the verbatim text
            - the corresponding entry in the taxonomy/ontology
    """

    def __init__(self, cfg):
        self.neti_endpoint = "http://localhost:6384"
        self.gnrd_endpoint = "https://gnrd.globalnames.org/name_finder.json"
        # self.ncbi.update_taxonomy_database()

        self.preproc_replace = {'-\n' : '', '\n' : ' '}
        self.preproc_regex = re.compile("(%s)" % "|".join(map(re.escape, \
            self.preproc_replace.keys())))

        self.gnrd_replace = {'null' : 'None', 'false' : 'False', 'true' : 'True'}
        self.gnrd_regex = re.compile("(%s)" % "|".join(map(re.escape, \
            self.gnrd_replace.keys())))

        self.globi = GlobiQuery({})

    def get_raw_text(self):
        with open("test_data/processed_text.txt") as f:
            yield f.read()

    def text_preprocessing(self, raw_text):
        """
            Remove newlines from text
        """
        text = self.preproc_regex.sub(
            lambda mo: self.preproc_replace[mo.string[mo.start():mo.end()]],
            raw_text)
        text = " ".join(text.split())
        # with open("./preprocessed_text.txt","w") as f:
        #     f.write(text)
        #     f.close()
        return text

    def scientific_names_detection_neti(self, text):
        """
            Detect scientific names in text using NetiNeti
        """
        r = requests.post(self.neti_endpoint, data={'data' : text})
        resolved_names, names_verbatim, offsets = eval(r.text)
        resolved_names = resolved_names.split("\n")
        nbNames = len(resolved_names)
        entities = []
        for i in range(0, nbNames):
            entity = {'sci_name':resolved_names[i],
                'offset':int(offsets[i][0]) + left_strip(names_verbatim[i])[1],
                'verbatim':re.sub(r"^\W+|\W+$", "", names_verbatim[i])}
            entities.append(entity)
        return entities, text

    def scientific_names_detection_gnrd(self, text):
        """
            Detect scientific names in text using NetiNeti and TaxonFinder
        """
        r = requests.post(self.gnrd_endpoint, \
            data={'file' : "", 'text' : text, 'engines' : ['TaxonFinder', 'NetiNeti']})
        req_result_str = self.gnrd_regex.sub(
            lambda mo: self.gnrd_replace[mo.string[mo.start():mo.end()]], r.text)
        names = eval(req_result_str)["names"]
        entities = [{'sci_name':name["scientificName"],
            'offset':int(name["offsetStart"]) + left_strip(name["verbatim"])[1],
            'verbatim':re.sub(r"^\W+|\W+$", "", name["verbatim"])}
            # 'verbatim':names_verbatim[i].strip([' ()'])}
            for name in names]
        return entities, text


    def scientific_names_postprocessing(self, entities, text):
        """
            Prefix matching against the scientific name of detected entities
            for correction of incomplete names
            Example : "Enhydra" detected -> look for "Enhydra lutris" in text
        """
        for entity in entities:
            names = species.name_suggest(entity['sci_name'])
            for name in names:
                canonical = name['canonicalName'] if 'canonicalName' in name else name['scientificName']
                l_canonical = canonical.lower()
                if entity['sci_name'].lower() != l_canonical:
                    offsets = [m.start() for m in re.finditer(entity['sci_name'], l_canonical, re.IGNORECASE)]
                    for offset in offsets:
                        text_offset = entity['offset']-offset
                        in_text = text[text_offset:text_offset+len(l_canonical)]
                        if in_text.lower() == l_canonical:
                            entity['sci_name'] = canonical
                            entity['offset'] = text_offset
                            entity['verbatim'] = in_text
        return entities, text

    def entity_normalization(self, entities, text):
        """
            Link detected entities with corresponding entities in the KB
            (here we assign their GBIF or NCBI TaxId)
        """
        norm_entities = []
        for entity in entities:
            dict = name_2_taxid(entity['sci_name'])
            if dict['taxid'] != None:
                entity['taxid'] = dict['taxid']
                entity['uri'] = dict['uri']
                entity['db'] = dict['db']
                entity['sci_name'] = dict['canonical']
                norm_entities.append(entity)
        return norm_entities, text

    def common_names_detection(self, entities, text):
        unique_entities = list({entity['taxid']:entity for entity in entities}.values())
        common_names = {}
        for entity in unique_entities:

            names = self.globi.get_common_names(entity['sci_name']) # GloBi API
            if 'en' in names:
                common_names[names['en'].lower()] = {k:entity[k] for k in ['sci_name', 'uri', 'taxid', 'db']}

            if entity['db'] == 'gbif': # GBIF API
                usages = species.name_usage(key=entity['taxid'], data='vernacularNames')
                for usage in usages['results']:
                    if usage['language'] == 'eng':
                        common_names[usage['vernacularName'].lower()] = {k:entity[k] for k in ['sci_name', 'uri', 'taxid', 'db']}

        new_entities = []
        for name in common_names: # TODO : start with longest names
            offsets = [m.start() for m in re.finditer(name, text, re.IGNORECASE)]
            for offset in offsets:
                entity = dict(common_names[name])
                entity['offset'] = offset
                entity['verbatim'] = name
                new_entities.append(entity)

        return entities+new_entities, text

    def synonyms_detection(self, text, entities):
        unique_entities = list({entity['taxid']:entity for entity in entities}.values())
        synonym_names = {}
        for entity in unique_entities:
            if entity['db'] == 'gbif':
                usages = species.name_usage(key=entity['taxid'], data='synonyms')
                for usage in usages['results']:
                    synonym_names[usage['canonicalName']] =
                        {k:entity[k] for k in ['sci_name', 'uri', 'taxid', 'db']}

        new_entities = []
        for name in synonym_names:
            offsets = [m.start() for m in re.finditer(name, text)]
            for offset in offsets:
                entity = dict(synonym_names[name])
                entity['offset'] = offset
                entity['verbatim'] = name
                new_entities.append(entity)

        return text, entities+new_entities

    def end(self, text, entities):
        return NOT_MODIFIED

    def get_test_graph(self):
        graph = bonobo.Graph()
        graph.add_chain(
            self.get_raw_text,
            self.text_preprocessing,
            self.scientific_names_detection_gnrd,
            self.entity_normalization,
            self.common_names_detection,
            self.end,
        )
        return graph

if __name__ == '__main__':
    cfg = {}
    pipe = NamedEntityRecognition(cfg)
    bonobo.run(pipe.get_test_graph())
