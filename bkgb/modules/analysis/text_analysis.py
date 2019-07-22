import bonobo
import os
import re
from tika import parser
from nltk.tokenize import word_tokenize
import requests

class TextAnalysis():

    def __init__(self, cfg):
        self.input_dir = cfg['input_dir']

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
        # Get files text content
        raw_text = file_data['content']
        yield raw_text

    def text_preprocessing(self, raw_text):
        replace_dict = {'-\n' : '', '\n' : ' '}
        regex = re.compile("(%s)" % "|".join(map(re.escape, replace_dict.keys())))
        text = regex.sub(lambda mo: replace_dict[mo.string[mo.start():mo.end()]], raw_text)
        # tokens = word_tokenize(text)
        # print(tokens[:100])
        yield text

    def find_names(self, text):
        r = requests.post("http://localhost:6384", data={'data' : text})
        resolved_names, names_verbatim, offsets = eval(r.text)
        resolved_names = resolved_names.split("\n")
        print("Found {} names".format(len(resolved_names)))

    def get_graph(self):#, **options):
        graph = bonobo.Graph()
        graph.add_chain(
            self.get_files,
            self.pdf_2_text,
            self.text_preprocessing,
            self.find_names,
            bonobo.Limit(10),
            bonobo.PrettyPrinter(),
        )
        return graph

if __name__ == '__main__':
    cfg = {'input_dir' : '/home/leguilln/workspace/biodiv-kg-builder/test_data/pdf'}
    pipe = TextAnalysis(cfg)
    bonobo.run(pipe.get_graph())
