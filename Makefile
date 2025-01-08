.PHONY: all test checkstyle help coverage btw21 btw21_compose ew24 ew24_compose delfi21 delfi24 vag24 fintraffic24 help_election help_gtfs help_compose help_kml help_csv help_reproduction export_reproduction license data_licenses

ifndef VERBOSE
.SILENT:
endif

.ONESHELL:

all: checkstyle test

checkstyle:
	python3 -m flake8 *.py

test:
	python3 -m unittest test_*.py

coverage:
	python3-coverage run --source=. -m unittest test_*.py
	python3-coverage report
	python3-coverage html

btw21:
	mkdir -p /data/election && cd /data/election
	cp /app/election/btw21-manifesto-replacement.csv .
	election2rdf.py --config /app/election/btw21.json --output /output/btw21.ttl.bz2 --output-aux-geo /output/btw21-aux-geo.tsv

btw21_compose:
	compose_spatial.py --main-config btw21_compose.json --output /output/btw21.rq /app/sparql

ew24:
	mkdir -p /data/election && cd /data/election
	cp /app/election/ew24-manifesto-replacement.csv .
	election2rdf.py --config /app/election/ew24.json --output /output/ew24.ttl.bz2

ew24_compose:
	compose_spatial.py --main-config ew24_compose.json --output /output/ew24.rq /app/sparql

serve_compose:
	compose_spatial.py --main-config btw21_compose.json --serve serve.json /app/sparql

delfi21:
	set -e
	mkdir -p /data/gtfs/delfi21 && cd /data/gtfs/delfi21
	curl --fail -o delfi_20210924.zip "https://web.archive.org/web/20241009072240/https://archiv.opendata-oepnv.de/DELFI/Soll-Fahrplandaten%20(GTFS)/2021/20210924_fahrplaene_gesamtdeutschland_gtfs.zip"
	gtfs2rdf.py --feed delfi_20210924 --input delfi_20210924.zip --output /output/delfi21.ttl.bz2 --add-linestrings

delfi24:
	set -e
	mkdir -p /data/gtfs/delfi24 && cd /data/gtfs/delfi24
	# The download via opendata-oepnv.de requires an account, however we can freely redistribute the data without this restriction after one download because of the CC-BY license
	curl --fail -o delfi_20240603.zip "https://ullinger.info/bachelor-thesis/data/20240603_fahrplaene_gesamtdeutschland_gtfs.zip"
	gtfs2rdf.py --feed delfi_20240603 --input delfi_20240603.zip --output /output/delfi24.ttl.bz2 --add-linestrings

vag24:
	set -e
	mkdir -p /data/gtfs/vag24 && cd /data/gtfs/vag24
	curl --fail -o vag_2024.zip https://web.archive.org/web/20241108074231/https://www.vag-freiburg.de/fileadmin/gtfs/VAGFR.zip
	gtfs2rdf.py --feed vag_2024 --input vag_2024.zip --output /output/vag24.ttl.bz2 --add-linestrings

fintraffic24:
	set -e
	mkdir -p /data/gtfs/fintraffic24 && cd /data/gtfs/fintraffic24
	curl --fail -o fintraffic_2024.zip https://web.archive.org/web/20241127154525/https://rata.digitraffic.fi/api/v1/trains/gtfs-all.zip
	gtfs2rdf.py --feed fintraffic_2024 --input fintraffic_2024.zip --output /output/fintraffic24.ttl.bz2 --add-linestrings

export_reproduction:
	set -e
	mkdir -p /data/export_reproduction
	cd /data/export_reproduction
	cp -r /app/. . # Important: this has to be exactly like it is in order to copy dotfiles
	cp reproduction.py __main__.py
	zip -r ../reproduction.zip .
	mv ../reproduction.zip .
	echo '#!/bin/env python3' > repro_head
	cat repro_head reproduction.zip > /output/run_reproduction
	chmod +x /output/run_reproduction
	echo "Reproduction script exported to '/output/run_reproduction'. You can now run this program on your host system."

help_election:
	election2rdf.py  --help
	echo ""
	echo "For example configuration files, see 'election/btw21.json' and 'election/ew24.json'"

help_gtfs:
	gtfs2rdf.py --help

help_compose:
	compose_spatial.py --help

help_kml:
	kml2rdf.py --help

help_csv:
	csv2rdf.py --help

help_reproduction:
	reproduction.py --help
	echo ""
	echo "List of steps:"
	reproduction.py --list-steps
	echo ""
	echo "Please run this program outside of the container, as it needs to start multiple containers."

help:
	more docs/help.txt

license:
	more LICENSE

data_licenses:
	more docs/LICENSE_data
