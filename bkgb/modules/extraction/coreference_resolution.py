import bonobo
from bonobo.constants import NOT_MODIFIED
import os
import re
import requests
import string

import spacy
import en_coref_lg

from pygbif import species
from ete3 import NCBITaxa
from ...utils.name_resolver import name_2_taxid
from ..access.globi_query import GlobiQuery

from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.tokenize.punkt import PunktSentenceTokenizer
from numpy import searchsorted

# from stanfordcorenlp import StanfordCoreNLP
# import json

class CoreferenceResolution():
    """
        Input : raw text, a list of named entities
        Output : a list of named entities, which includes:
            - the entities in the input list
            - coreferences referring to these entities
    """

    def __init__(self, cfg):
        self.globi = GlobiQuery({})
        self.nlp = en_coref_lg.load()
        self.context_size = 1
        # self.corenlp = StanfordCoreNLP(r'/home/leguilln/apps/stanford-corenlp-full-2018-10-05', lang='en', memory='8g')

    def get_sentence(self):
        return "Haliaeetus leucocephalus population increased at an annual rate of 10. Over the same time period, foraging activity of Bald Eagles at marine bird breeding colonies also increased"

    def resolve_coref(self, entities, text):
        entities_by_sentence, sent_tokens = self.group_entities_by_sentence(entities, text)
        for sent_entities, context in self.get_entities_context(text, entities_by_sentence, sent_tokens):
            offsets = [e['offset'] for e in sent_entities]
            for entity, reference in self.get_coref_clusters(sent_entities, context):
                norm_entity = self.normalize_coref(sent_entities, context, entity, reference)
                if norm_entity != None and (norm_entity['offset'] not in offsets):
                    entities.append(norm_entity)
        return entities, text

    def group_entities_by_sentence(self, entities, text):
        sent_tokens = list(PunktSentenceTokenizer().span_tokenize(text))
        sent_offsets = [start for start, _end in sent_tokens]
        entities_by_sentence = {}
        for entity in entities:
            index = searchsorted(sent_offsets, entity['offset'])-1
            if entity['offset'] > sent_tokens[index][1]:
                index += 1
            entities_by_sentence.setdefault(index, []).append(entity)
        return entities_by_sentence, sent_tokens

    def get_entities_context(self, text, entities_by_sentence, sent_tokens):
        for sentence in entities_by_sentence:
            if sentence < len(sent_tokens)-self.context_size:
                context_start = sent_tokens[sentence][0]
                context_end = sent_tokens[sentence+self.context_size][1]
                context = {'offset':context_start, 'text':text[context_start:context_end]}
                yield entities_by_sentence[sentence], context

    def get_coref_clusters(self, entities, context):
        # props={'annotators': 'dcoref','pipelineLanguage':'en','outputFormat':'json'}
        # result = self.corenlp.annotate(context['text'], properties=props)
        # print(result)
        doc = self.nlp(context['text'])
        if doc._.has_coref:
            for cluster in doc._.coref_clusters:
                entity = cluster.mentions[0]
                reference = cluster.mentions[1]
                yield entity, reference

    def normalize_coref(self, entities, context, entity, reference):
        for e in entities:
            if e['verbatim'].lower() in entity.text.lower():
                norm_entity = dict(e)
                norm_entity['offset'] = context['offset']+reference.start_char
                norm_entity['verbatim'] = reference.text
                return norm_entity
        return None

    def end(self, text, entities):
        return NOT_MODIFIED

    def get_test_graph(self):#, **options):
        graph = bonobo.Graph()
        graph.add_chain(
            self.get_sentence,
            self.get_coref_clusters,
            self.end,
        )
        return graph



if __name__ == '__main__':
    cfg = {}
    pipe = CoreferenceResolution(cfg)
    bonobo.run(pipe.get_test_graph())
