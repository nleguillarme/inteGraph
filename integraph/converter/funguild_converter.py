import os
import logging
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from .converter import Converter
from ..util.csv_helper import *


class FUNGuildConverter(Converter):
    def __init__(self, config):
        Converter.__init__(self)
        self.logger = logging.getLogger(__name__)
        self.cfg = config

    def convert(self, f_in, out_dir, **kwargs):
        with open(f_in, "r") as f:
            html_content = f.read()
            df = self.parse_html_content(html_content)
            filename = os.path.splitext(os.path.basename(f_in))[0]
            f_out = os.path.join(out_dir, filename + f".{self.cfg.standard_format}")
            write(df, f_out, sep=self.cfg.delimiter)

    def parse_html_content(self, html_content):
        soup = BeautifulSoup(html_content, "html.parser")
        json_content = soup.body.get_text()
        return pd.read_json(StringIO(json_content), orient="records")
