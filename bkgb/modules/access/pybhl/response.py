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

# response.py

try:
    import json                 # Python >= 2.6
except ImportError:
    import simplejson as json   # otherwise require simplejson

from bs4 import BeautifulSoup

OCRID = 'ctl00_toolbarContentPlaceHolder_titleVolumeSelectionControl_linkOCR'

class BHLResponse(object):
    """Response class for BHLOpenURLRequests"""
    def __init__(self, request, data):
        """Constructor method"""
        super(BHLResponse, self).__init__()
        self.format = request.format
        if request.parsing is True and request.format == 'json':
            self.data = json.loads(data)
            self.parsed = True
        else:
            self.data = data
            self.parsed = False

    def get_ocr_url(self, index):
        """Get the URL containing the OCRed text from archive.org"""
        if (self.format == 'json') and self.parsed:
            pageurl = self.data['citations'][index]['Url']
            soup = BeautifulSoup(urllib.request.urlopen(pageurl).read())
            return soup.find('a', id=OCRID)['href']
        else:
            raise NotImplementedError
