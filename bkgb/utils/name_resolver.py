from os.path import basename, dirname
from pygbif import species
from ete3 import NCBITaxa
import re

def name_2_taxid(sci_name, db=None):
    if db == None or db == 'gbif':
        out = species.name_backbone(name=sci_name)
        if out["matchType"] == 'EXACT':
            taxid = out['usageKey']
            uri = 'https://www.gbif.org/species/{}'.format(taxid)
            return {'db':'gbif', 'taxid':taxid, 'uri':uri, 'canonical':out["canonicalName"]}
    if db == None or db == 'ncbi':
        name2taxid = NCBITaxa().get_name_translator([sci_name])
        if name2taxid != {}:
            taxid = name2taxid[sci_name][0]
            uri = 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id={}'.format(taxid)
            return {'db':'ncbi', 'taxid':taxid, 'uri':uri, 'canonical':sci_name}
    # else: # TODO : Raise error
    return {'db':None, 'taxid':None, 'uri':None}

def taxid_2_name(taxid, db='gbif'):
    if db == 'gbif':
        return species.name_usage(key=taxid, data='name')['canonicalName']
    elif db == 'ncbi':
        taxid2name = NCBITaxa().get_taxid_translator([taxid])
        if taxid2name != {}:
            return taxid2name[taxid]
    return None # TODO : Raise error

def uri_2_name(uri):
    taxid = int(re.search(r'\d+', uri).group())
    if 'gbif' in uri:
        return taxid_2_name(taxid, db='gbif')
    elif 'ncbi' in uri:
        return taxid_2_name(taxid, db='ncbi')
    else: # TODO : Raise error
        return None
