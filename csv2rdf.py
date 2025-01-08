#!/bin/env python3
"""
create rdf turtle from comma-separated values (csv) data

Copyright (c) 2024 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""


import csv
import re
import json
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence
from dataset import PREFIXES, Dataset, Prefix, Triple, next_id, TYPE, MEMBER, \
    UNPROBLEMATIC_PREDICATE, add_datatype
import argparse
import logging

PROGRAM_DESCRIPTION = """
    csv2rdf - create RDF turtle from CSV data.

    Convert a comma-separated values file (CSV) to an RDF knowledge-graph
    in turtle format. The program supports renaming columns, regex replacing
    of values, custom prefixes, etc.

    The program is licensed under the GNU General Public License version 3,
    see LICENSE for details.
"""

logger = logging.getLogger(__name__)


WARN_MISSING_COL_MAPPING = False


def set_warn_missing_col_mapping(value: bool):
    global WARN_MISSING_COL_MAPPING
    WARN_MISSING_COL_MAPPING = value


ExtraTripleCallback = Optional[Callable[[
    'CSVDataset', str, dict[str, str]], Iterator[Triple]]
]  # Dataset, Subject, Row
CSVColumnMapping = dict[str, Optional[str]]
CSVValuesMappingRule = tuple[str, str | Callable[[re.Match], str]]
CSVValuesMappingRules = Sequence[CSVValuesMappingRule]
CSVValuesMapping = dict[
    str, tuple[CSVValuesMappingRules, bool]
]  # Col after Mapping -> ([(ReSearch, ReReplace),...], AddDatatype)


@Dataset.register
@dataclass
class CSVDataset(Dataset):
    """
    Represents a CSV file to be converted to RDF.

    Fields:

    - `_dataset`: A name for the dataset
    - `_command`:               Shell command to download or generat the data
                                (optional)
    - `_store_filename`:        Filename of the data set. If this file does not
                                exist, `_command` will be run and its stdout
                                will be written to this file.
    - `_primary_prefix`:        The prefix to be used by default for entities
                                from this data set.
    - `parent`:                 A fully qualified RDF entity to be used to emit
                                a `rdfs:member` triple for each row. (optional)
    - `csv_separator`:          CSV separator character (default: `,`)
    - `csv_quote`:              CSV quoting character (default: `"`)
    - `column_mapping`:         A dictionary string to string, which will map
                                column names to predicates. If the target
                                predicate has no prefix, `_primary_prefix` will
                                be used. (default: `{}`)
    - `primary_col`:            The name of a column (after applying
                                `column_mapping`) which contains a unique key.
                                It will be used as the entity id (together with
                                `_primary_prefix`). (optional)
    - `values_mapping`:         A dictionary which will be used to modify cell
                                values before they are emitted as objects in
                                RDF. The string keys are column names after
                                applying `column_mapping`. The value is a tuple
                                of two elements. The first element is a list of
                                replace rules: each a 2-tuple (`pattern`,
                                `repl`) suitable for `re.sub`. The second
                                element is a boolean, whether the result should
                                be treated as a literal (True) or an IRI
                                (False). (default: `{}`)
    - `extra_triple_callback`:  A callback that will be called once for every
                                row in the input CSV. It will be called with
                                this `CSVDataset` object, the RDF subject for
                                the current row as well as the row's raw data
                                without any mappings applied as arguments. The
                                row is given as a dictionary from column name
                                to cell value. It should return an Iterable of
                                triples. (optional)

    Field names prefixed with an underscore are immutable after creation.
    The other fields may be changed between multiple calls to the rdf()
    function.
    """

    csv_separator: str = ","
    csv_quote: str = "\""
    column_mapping: CSVColumnMapping = field(default_factory=dict)
    primary_col: Optional[str] = None
    # col after mapping -> Regex Find, Regex Replace, Apply datatype?
    values_mapping: CSVValuesMapping = field(default_factory=dict)
    extra_triple_callback: ExtraTripleCallback = None

    def rdf(self) -> Iterator[Triple]:
        """
        Converts a CSV file to RDF triples. See: Dataset.rdf
        """
        content_iter = self.content()
        reader = csv.DictReader(content_iter,
                                delimiter=self.csv_separator,
                                quotechar=self.csv_quote)

        # Check column mappings
        if WARN_MISSING_COL_MAPPING and reader.fieldnames:
            for col in reader.fieldnames:
                if col not in self.column_mapping:
                    logger.warning(
                        "Dataset %s. Missing column mapping for %s",
                        self.dataset, col)

        # Process entries of file
        for row in reader:
            # Each row corresponds to one RDF subject
            subj = ""
            if self.primary_col:
                # Also apply col + values mapping to primary_col if applicable
                primary_col = self.primary_col
                if primary_col in self.column_mapping:
                    primary_col = self.column_mapping[primary_col]

                primary_col_val = row[self.primary_col]
                if primary_col in self.values_mapping:
                    for search, replace in \
                            self.values_mapping[primary_col][0]:
                        primary_col_val = re.sub(
                            search, replace, primary_col_val)

                # Use as subject
                subj = f"{self.primary_prefix}{primary_col_val}"
            else:
                # Generate subject because the data does not have a primary
                subj = f"{self.primary_prefix}_{next_id()}"

            # Emit general triples on the subject (type and parent)
            yield (subj, TYPE, f"{self.type_str}")
            if self.parent:
                yield (subj, MEMBER, self.parent)

            for col, obj in row.items():
                # Do not emit triples with empty object
                if obj == "" or obj is None:
                    continue

                # Predicate name / Column mapping
                pred = col
                if col in self.column_mapping:
                    pred = self.column_mapping[col]
                    if pred is None:
                        continue

                # Apply regular expression replace to values
                datatype = True
                if pred in self.values_mapping:
                    pairs, datatype = self.values_mapping[pred]
                    for search, replace in pairs:
                        obj = re.sub(search, replace, obj)

                # Clean up predicates
                if ":" not in pred:
                    pred = f"{self.clean_prefix}{pred}"
                if not UNPROBLEMATIC_PREDICATE.match(pred):
                    pred = f"\"{pred}\""

                # Encode object. Emit triple
                yield (subj, pred, add_datatype(obj) if datatype else obj)

            if self.extra_triple_callback:
                yield from self.extra_triple_callback(self, subj, row)


def parse_arguments(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    """
    Parse the program's command-line arguments
    """
    parser = argparse.ArgumentParser(
        description=PROGRAM_DESCRIPTION)

    # Required arguments
    parser.add_argument(
        '--input', '-i', nargs=1, type=str, required=True,
        help='input CSV file')
    parser.add_argument(
        '--dataset', '-d', nargs=1, type=str, required=True,
        help='alphanumeric dataset name for entity ids (e.g. mydata)')
    parser.add_argument(
        '--output', '-o', nargs=1, type=str, required=True,
        help='filename for compressed output turtle file ' +
        '(e.g. mydata.ttl.bz2)')
    parser.add_argument(
        '--prefix', '-p', nargs=1, type=str, required=True,
        help='primary prefix to be used (short form, e.g. "ex:")')
    parser.add_argument(
        '--iri', '-r', nargs=1, type=str, required=True,
        help='primary prefix to be used (full IRI form, ' +
        'e.g "http://example.com/schema")')

    # # Optional arguments
    parser.add_argument(
        '--parent', '-e', nargs=1, type=str,
        help='optional IRI for rdfs:member statements')
    parser.add_argument(
        '--separator', nargs=1, type=str, default=',',
        help='separator character (default: ,)')
    parser.add_argument(
        '--quote', nargs=1, type=str, default='"',
        help='quote character (default: ")')
    parser.add_argument(
        '--primary-col', '-c', nargs=1, type=str,
        help='a column which contains the "primary key" to be used for ' +
        'entity IRIs')

    parser.add_argument(
        '--column-mapping', '-cm', nargs=1, type=str,
        help='''
        To map column names, give a valid json dictionary mapping
        column names in the dataset to predicates. Example:
        {"*!Some Very Weird Col Name!*": "rdfs:label"}
        ''')
    parser.add_argument(
        '--values-mapping', '-vm', nargs=1, type=str,
        help='''
        To map cell values, give a valid json dictionary where the keys are
        the column names after applying column mapping and the values are
        lists of two elements. The first element is a list of replace rules
        (each list of two: regex search, regex replace) and the second is "lit"
        to treat the value after mapping as a literal or "iri" to treat it as
        an IRI. Example: {"mycol": [[[".*/(.*)", "\\\\1"]], "lit"]}
        ''')
    parser.add_argument(
        '--additional-prefixes', '-a', nargs=1, type=str,
        help='''
        to add additional prefixes, a valid json dictionary mapping
        the short prefix to the full IRI. Example:
        {"abc": "http://example.com/#"}
        ''')
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None):
    """
    The procedure to be run when the program is called directly.
    """

    args = parse_arguments(argv)
    logger.info("csv2rdf - arguments: %s", args)

    # Required Arguments
    input_ = args.input[0]
    dataset = args.dataset[0]
    output = args.output[0]
    prefix = args.prefix[0]
    iri = args.iri[0]
    assert ":" in prefix
    PREFIXES.add(Prefix(prefix.split(":")[0], iri))

    # Optional Arguments
    parent = args.parent[0] if args.parent else None
    separator = args.separator[0] if args.separator else None
    quote = args.quote[0] if args.quote else None
    primary_col = args.primary_col[0] if args.primary_col else None

    column_mapping = {}
    if args.column_mapping:
        column_mapping = json.loads(args.column_mapping[0])
    values_mapping = {}
    if args.values_mapping:
        for key, val in json.loads(args.values_mapping[0]).items():
            add_type = val[1] == "lit"
            if not add_type:
                assert val[1] == "iri"
            values_mapping[key] = (val[0], add_type)
    if args.additional_prefixes:
        for prefix_, iri_ in json.loads(args.additional_prefixes[0]).items():
            PREFIXES.add(Prefix(prefix_, iri_))

    # Build dataset and write triples to .ttl.bz2 file
    d = CSVDataset(
        _dataset=dataset,
        _command=None,
        _store_filename=input_,
        _primary_prefix=prefix,
        parent=parent,
        csv_separator=separator or ",",
        csv_quote=quote or "\"",
        column_mapping=column_mapping,
        primary_col=primary_col,
        values_mapping=values_mapping,
        extra_triple_callback=None
    )
    count = d.to_file(output)
    logger.info("Emitted %d triples", count)


if __name__ == "__main__":
    main()
