#!/bin/bash
set -euo pipefail
cd /data

# Get data
if [ ! -f btw21-manifesto.csv ]; then
    echo "Downloading data"
    curl --silent --fail https://web.archive.org/web/20240926200854/https://manifesto-project.wzb.eu/down/data/2024a/datasets/MPDataset_MPDS2024a.csv | sed 's/\([[:digit:]][[:digit:]]\)\/\([[:digit:]][[:digit:]]\)\/\([[:digit:]][[:digit:]][[:digit:]][[:digit:]]\)/\1\.\2\.\3/g' > btw21-manifesto.csv
    # Note: the sed command is used to make dates less ambigous,
    # in the manifesto-replacement CSVs the DD.MM.YYYY format is already applied 
fi
if [ ! -f btw21-manifesto-replacement.csv ]; then
    cp -v /app/btw21-manifesto-replacement.csv .
fi

# Reproduce result using R
/usr/bin/time -p Rscript /app/manifesto-replacement.R |& tee R-output-manifesto.txt
