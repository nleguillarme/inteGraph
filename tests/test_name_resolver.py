import pytest

# from bkgb.utils.name_resolver import *

# def test_name_2_taxid_gbif():
#     sci_name = 'Helianthus annuus'
#     db = 'gbif'
#     res = name_2_taxid(sci_name, db=db)
#     assert res['taxid'] == 9206251
#     assert res['uri'] == '<https://www.gbif.org/species/9206251>'
#
# def test_name_2_taxid_ncbi():
#     sci_name = 'Helianthus annuus'
#     db = 'ncbi'
#     res = name_2_taxid(sci_name, db=db)
#     assert res['taxid'] == 4232
#     assert res['uri'] == '<https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=4232>'
#
# def test_taxid_2_name_gbif():
#     db = 'gbif'
#     res = taxid_2_name(9206251, db=db)
#     assert res == 'Helianthus annuus'
#
# def test_taxid_2_name_ncbi():
#     db = 'ncbi'
#     res = taxid_2_name(4232, db=db)
#     assert res == 'Helianthus annuus'
#
# def test_uri_2_name_gbif():
#     uri = '<https://www.gbif.org/species/9206251>'
#     res = uri_2_name(uri)
#     assert res == 'Helianthus annuus'
#
# def test_uri_2_name_ncbi():
#     uri = '<https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=4232>'
#     res = uri_2_name(uri)
#     assert res == 'Helianthus annuus'
