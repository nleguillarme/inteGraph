import bonobo
from bonobo.constants import NOT_MODIFIED
import os
import re
from tika import parser
from .named_entity_recognition import NamedEntityRecognition
from .coreference_resolution import CoreferenceResolution

class InformationExtraction():

    def __init__(self, cfg):
        self.input_dir = cfg['input_dir']
        self.ner = NamedEntityRecognition(cfg={})
        self.coref = CoreferenceResolution(cfg={})

    def get_files(self):
        """
        Generate all (PDF) file names in the input directory
        """
        for entry in os.listdir(self.input_dir):
            file_path = os.path.join(self.input_dir, entry)
            if os.path.isfile(file_path):
                yield file_path

    def pdf_2_text(self, file_path):
        print(file_path)
        file_data = parser.from_file(file_path)
        raw_text = file_data['content'] # Get files text content
        yield raw_text

    def parse_ground_truth(self, filepath):
        from lxml import etree
        tree = etree.parse(filepath)
        annotations = [{'type':node.get("Type"), 'start':int(node.get("StartNode")), 'end':int(node.get("EndNode"))} \
            for node in tree.xpath("/GateDocument/AnnotationSet/Annotation")]
        return annotations

    def get_ground_truth(self, text, annotations):
        verbatim = [text[ann['start']:ann['end']] for ann in annotations]
        unique = list(set(verbatim))
        return unique, verbatim, annotations

    def evaluate_method(self, entities, text):
        annotations = self.parse_ground_truth("/home/leguilln/workspace/biodiv-kg-builder/preprocessed_text.xml")
        unique, verbatim, annotations = self.get_ground_truth(text, annotations)
        full_offsets_gt = [ann['start'] for ann in annotations]
        start_offsets_gt = [ann['start'] for ann in annotations if (ann['type'] in 'accepted' or ann['type'] in 'common' or ann['type'] in 'ref')]
        print(len(entities),"/",len(start_offsets_gt))
        tp = 0

        # entity_offsets = [entity['offset'] for entity in entities]
        # for offset in start_offsets_gt:
        #     if offset not in entity_offsets:
        #         print(verbatim[full_offsets_gt.index(offset)])

        for entity in entities:
            if entity['offset'] in start_offsets_gt:
                # print(entity['offset'], entity['verbatim'].lower())
                if entity['verbatim'].lower() in verbatim[full_offsets_gt.index(entity['offset'])].lower():
                    tp += 1
            #     else:
            #         print(entity['verbatim'], ":", verbatim[full_offsets_gt.index(entity['offset'])])
            # else:
            #     print(entity['offset'], entity['verbatim'], ":", "?")
        fp = len(entities)-tp
        print("True Positive {}, False Positive {}".format(tp, fp))
        print("Precision = {}, Recall = {}".format(tp/(tp+fp), tp/len(start_offsets_gt)))
        return NOT_MODIFIED

    def get_test_graph(self):
        graph = bonobo.Graph()
        graph.add_chain(
            self.get_files,
            self.pdf_2_text,
            self.ner.text_preprocessing,
            self.ner.scientific_names_detection_gnrd,
            self.ner.scientific_names_postprocessing,
            self.ner.entity_normalization,
            self.ner.common_names_detection,
            self.coref.resolve_coref,
            self.evaluate_method
        )
        return graph

if __name__ == '__main__':
    cfg = {'input_dir' : '/home/leguilln/workspace/biodiv-kg-builder/test_data/pdf'}
    pipe = InformationExtraction(cfg)
    bonobo.run(pipe.get_test_graph())
