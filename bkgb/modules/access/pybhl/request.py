# Copyright (C) 2009 Mark A. Matienzo
#
# This file is part of pybhl, the Python Biodiversity Heritage Library module.
#
# pybhl is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# pybhl is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with pybhl.  If not, see <http://www.gnu.org/licenses/>.

# request.py

# TODO - ADD OPENURL 1.0 SUPPORT

# from urllib import urlencode
from urllib.parse import urlencode
from urllib.request import urlopen
from .response import BHLResponse

VALID_PARAMS = {
    '0.1': ['title', 'au', 'aulast', 'aufirst', 'publisher', 'date', 'isbn',
            'issn', 'coden', 'stitle', 'volume', 'issue', 'spage', 'pid',
            'genre'],
}

class BHLRequest(object):
    # Class to request things via OpenURL using BHL
    # TODO - Implement a separate module for OpenURL creating/parsing ala
    # ropenurl for Ruby

    def __init__(self, format='json', op='PublicationSearchAdvanced', **kwargs):
        """Constructor method"""
        self.baseurl = 'https://www.biodiversitylibrary.org/api3?'
        self.format = format
        if self.format == 'json':
            self.parsing = True
        else:
            self.parsing = False
        self.op = op
        self.params = kwargs
        self.apikey = '33adf7b0-7fde-4e10-8387-80b549637523'

    # def validate(self):
    #     """Simple validator method"""
    #     for param in self.params:
    #         if param not in VALID_PARAMS[self.openurl_version]:
    #             raise ValueError("Improper parameter for OpenURL version %s"\
    #             % self.openurl_version)

    # def create_url(self):
    #     """Generates URL for request to BHL's API"""
    #     return self.baseurl + '?format=' + self.format + '&' \
    #         + urlencode(self.params)

    def create_url(self):
        return self.baseurl + 'op=' + self.op + '&apikey=' + self.apikey + '&'\
            + urlencode(self.params) + '&format=' + self.format

    def get_response(self):
        """Get a BHLResponse object based on a created request"""
        url = self.create_url()
        rsp = urlopen(url).read()
        return BHLResponse(self, rsp)

    # def get_response(self):
    #     """Get a BHLResponse object based on a created request"""
    #     url = self.create_url()
    #     rsp = urlopen(url).read()
    #     return BHLResponse(self, rsp)

class BHLOpenURLRequest(object):
    # Class to request things via OpenURL using BHL
    # TODO - Implement a separate module for OpenURL creating/parsing ala
    # ropenurl for Ruby

    def __init__(self, format='json', **kwargs):
        """Constructor method"""
        self.baseurl = 'http://www.biodiversitylibrary.org/openurl'
        self.format = format
        if self.format == 'json':
            self.parsing = True
        else:
            self.parsing = False
        self.params = kwargs
        self.openurl_version = '0.1'

    def validate(self):
        """Simple validator method"""
        for param in self.params:
            if param not in VALID_PARAMS[self.openurl_version]:
                raise ValueError("Improper parameter for OpenURL version %s"\
                % self.openurl_version)

    def create_url(self):
        """Generates URL for request to BHL's API"""
        return self.baseurl + '?format=' + self.format + '&' \
            + urlencode(self.params)

    def get_response(self):
        """Get a BHLResponse object based on a created request"""
        url = self.create_url()
        rsp = urlopen(url).read()
        return BHLResponse(self, rsp)
