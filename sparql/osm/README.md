# Reusable SPARQL shards for Points Of Interest

This directory contains universally reusable SPARQL shards to be used with `compose_spatial.py`, which select certain points of interest (POIs) from RDF OpenStreetMap data (see `osm2rdf`). The file names should be self-explanatory. Some require a `VALUES ?poi_predicates` clause, which is not included. It should be provided via the configuration JSON, analogous to the example configuration JSONs in the parent directory.
