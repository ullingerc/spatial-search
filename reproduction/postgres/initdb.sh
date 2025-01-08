#!/bin/bash
# from https://osm2pgsql.org/doc/manual.html#creating-a-database
sudo -u postgres createuser osmuser
sudo -u postgres createdb --encoding=UTF8 --owner=osmuser osm
sudo -u postgres psql osm --command='CREATE EXTENSION postgis;'
sudo -u postgres psql osm --command='CREATE EXTENSION hstore;'
