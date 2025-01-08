#!/bin/bash
set -euo pipefail
cd /data

# Ensure data
for f in btw21.tsv ew24.tsv; do
    if [ ! -f $f ]; then
        echo "$f missing"
        exit 1
    fi
done

# Reproduce results using R
/usr/bin/time -p Rscript /app/case_study.R |& tee R-output.txt

/app/manifesto-replacement.sh
