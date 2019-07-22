import requests

class SPARQLStore():

    def __init__(self, cfg):
        self.input_file = "./dumps/globi.1.nq"
        self.data_endpoint = "http://localhost:3030/interactions/data"

    def run(self):
        with open(self.input_file) as f:
            headers = {'Content-Type': 'application/n-quads'}
            r = requests.post(self.data_endpoint, data=f, headers=headers)
