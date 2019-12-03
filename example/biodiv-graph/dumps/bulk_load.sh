isql -Udba -Pleca2019 exec="ld_dir ('/data', '*', NULL);"
isql -Udba -Pleca2019 exec="rdf_loader_run();" &
isql -Udba -Pleca2019 exec="rdf_loader_run();" &
isql -Udba -Pleca2019 exec="rdf_loader_run();" &
isql -Udba -Pleca2019 exec="rdf_loader_run();" &
isql -Udba -Pleca2019 exec="rdf_loader_run();" &
wait
isql -Udba -Pleca2019 exec="checkpoint;"
isql -Udba -Pleca2019 exec="DELETE FROM DB.DBA.load_list;"