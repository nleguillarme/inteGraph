isql-v -Udba -Pdba exec="ld_dir ('/data', 'test_globi_16042020_merged.nt', NULL);"
isql-v -Udba -Pdba exec="rdf_loader_run();" &
isql-v -Udba -Pdba exec="rdf_loader_run();" &
isql-v -Udba -Pdba exec="rdf_loader_run();" &
isql-v -Udba -Pdba exec="rdf_loader_run();" &
isql-v -Udba -Pdba exec="rdf_loader_run();" &
wait
isql-v -Udba -Pdba exec="checkpoint;"
isql-v -Udba -Pdba exec="DELETE FROM DB.DBA.load_list;"
