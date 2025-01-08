#!/bin/env python3
"""
election2rdf - create rdf turtle from election data

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

import json
import argparse
import logging
from dataclasses import dataclass, field, InitVar
from typing import Iterator, Optional, Sequence
from datetime import datetime
from contextlib import nullcontext
import bz2
from io import TextIOWrapper
from dataset import LABEL, TYPE, PREFIXES, Dataset, Prefix, all_prefixes, \
    next_id, Triple, add_datatype, set_get_data_env
from csv2rdf import CSVDataset, set_warn_missing_col_mapping
from kml2rdf import KMLDataset, AuxGeoCallback

PROGRAM_DESCRIPTION = """
    This program creates an RDF knowledge graph in turtle format by combining
    multiple publicly available datasets (.csv and .kml). It is mainly
    tailored to German elections and the peculiarities of the Federal Returning
    Officer\'s (Bundeswahlleiter:in) datasets. The data can be augmented by
    further external datasets by user configuration.
"""

ELECTION = "election"

WD = Prefix("wd", "http://www.wikidata.org/entity/")
OSMREL = Prefix("osmrel", "https://www.openstreetmap.org/relation/")
PREFIXES.update({WD, OSMREL})

logger = logging.getLogger(__name__)


@dataclass
class Election:
    """
    Represents a collection of datasets belonging to one election that will
    be transformed into one combined RDF data set.

    Fields:

    - `_label`:             A human-readable name for this election. This
                            should usually include the elected body and year.
    - `_datasets`:          A list of `Dataset` objects, which will be
                            converted to RDF.
    - `_countryname`:       The English name for the country or area the
                            election took place. Useful for combining data.
    - `_wikidata`:          A Wikidata entity identifier "Q..." referring to
                            this election. For convenience, the `wd:` prefix
                            as well as its declaration is added automatically.
                            (optional)
    - `_osm`:               An OpenStreetMap relation id referring to the area
                            where this election took place. For convenience,
                            the `osmrel:` prefix as well as its declaration is
                            added automatically. (optional)
    - `_year`:              The year the election took place. (optional)
    - `_date`:              The date the election took place as YYYY/MM/DD.
                            (optional)
    - `_id_prefix`:         A string to be prefixed to the identifiers of the
                            entities produced. Helpful to combine the output
                            of multiple `Election`s in one RDF data set without
                            unwanted side effects.
    - `aux_geo_callback`:   A callback to export all geometries from KML.
                            Useful for generating aux-geo files for osm2rdf.

    Field names prefixed with underscore are immutable after creation.
    The other fields may be changed safely between multiple calls to the
    `Election.rdf()` function.
    """

    # Immutable fields
    _label: InitVar[str]
    _datasets: InitVar[list[Dataset]]
    _countryname: InitVar[Optional[str]]
    _wikidata: InitVar[Optional[str]]
    _osm: InitVar[Optional[str]]
    _year: InitVar[Optional[int]]
    _date: InitVar[Optional[str]]
    _id_prefix: InitVar[str]

    # The Aux Geo Callback is not declared InitVar because it may safely be
    # updated after the object has be initialized
    aux_geo_callback: AuxGeoCallback = None

    __id: str = field(init=False)

    def __post_init__(self, _label: str, _datasets: list[Dataset],
                      _countryname: Optional[str], _wikidata: Optional[str],
                      _osm: Optional[str], _year: Optional[int],
                      _date: Optional[str], _id_prefix: str):
        # Invariant
        assert _label

        # Attributes
        self.__label = _label
        self.__datasets = _datasets
        self.__countryname = _countryname
        self.__wikidata = _wikidata
        self.__osm = _osm
        self.__year = _year
        self.__date = _date
        self.__id_prefix = _id_prefix

        # Config
        self.__id = f"{ELECTION}:{self.__id_prefix}{next_id()}"

    # Getters
    @property
    def label(self) -> str:
        return self.__label

    @property
    def datasets(self) -> list[Dataset]:
        return self.__datasets

    @property
    def countryname(self) -> Optional[str]:
        return self.__countryname

    @property
    def wikidata(self) -> Optional[str]:
        return self.__wikidata

    @property
    def osm(self) -> Optional[str]:
        return self.__osm

    @property
    def year(self) -> Optional[int]:
        return self.__year

    @property
    def date(self) -> Optional[str]:
        return self.__date

    @property
    def id_prefix(self) -> str:
        return self.__id_prefix

    def get_all_data(self):
        """
        For all datasets fetch the data.
        """

        # Environment variables for Dateset.command
        set_get_data_env("ELECTION_LABEL", self.label)
        set_get_data_env("ELECTION_COUNTRY", self.countryname or "")
        set_get_data_env("ELECTION_DATE", self.date or "")

        # Run commands
        for d in self.datasets:
            d.get_data()

    def rdf(self) -> Iterator[Triple]:
        """
        Emit all triples for this election (metadata and all datasets).
        """
        # Global triples on the election
        logger.info("Emitting general info")
        yield (self.__id, TYPE, f"{ELECTION}:election")
        yield (self.__id, LABEL, add_datatype(self.label))
        if self.wikidata:
            yield (self.__id, f"{ELECTION}:wikidata",
                   f"{WD.prefix}:{self.wikidata}")
        if self.osm:
            yield (self.__id, f"{ELECTION}:osm",
                   f"{OSMREL.prefix}:{self.osm}")
        if self.countryname:
            yield (self.__id, f"{ELECTION}:countryname",
                   add_datatype(self.countryname))
        if self.date:
            yield (self.__id, f"{ELECTION}:date",
                   add_datatype(self.date))
        if self.year:
            yield (self.__id, f"{ELECTION}:year",
                   add_datatype(str(self.year)))

        # Main election results
        logger.info("Emitting datasets...")
        for d in self.datasets:
            logger.info(f"Emitting {d.dataset}")
            d.parent = self.__id
            match d:
                case KMLDataset():
                    d.aux_geo_callback = self.aux_geo_callback
            yield from d.rdf()

    def to_file(self, output: TextIOWrapper,
                aux_geo: Optional[TextIOWrapper]):
        """
        Takes two output files and produces TTL and aux-geo TSV for the given
        election
        """
        if aux_geo is not None:
            def ag_cb(_id: str, _wkt: str):
                print(_id + "\t" + _wkt, file=aux_geo)
            self.aux_geo_callback = ag_cb

        output.writelines(all_prefixes())

        logger.info("Starting triple generation...")

        start = datetime.now()
        count = 0
        for s in self.rdf():
            count += 1
            print(" ".join(s) + " .", file=output)
        logger.info("Took %s", str(datetime.now() - start))

        logger.info("Emitted %i triples", count)
        output.close()
        if aux_geo is not None:
            aux_geo.close()

    @staticmethod
    def load_from_config(filename: str) -> 'Election':
        """
        Construct an `Election` object from a given configuration json file.
        """
        config = {}

        def get(cnf: dict, key: str):
            assert key in cnf, \
                f"Required value missing {key}.\nInput: {repr(cnf)}"
            return cnf[key]

        with open(filename, "r") as f:
            config = json.load(f)

        election_prefix_seen = False
        for p in get(config, "prefixes"):
            prefix = get(p, "prefix")
            if prefix == ELECTION:
                election_prefix_seen = True
            iri = get(p, "iri")
            PREFIXES.add(Prefix(prefix, iri))
        assert election_prefix_seen, \
            f"A prefix definition for '{ELECTION}' is mandatory"

        datasets: list[Dataset] = []
        e = get(config, "election")
        for c in get(config, "csv"):
            datasets.append(CSVDataset(
                get(c, "dataset"),
                get(c, "command"),
                get(c, "store_filename"),
                # For ID uniqueness:
                get(c, "primary_prefix") + get(e, "id_prefix"),
                None,
                get(c, "csv_separator"),
                get(c, "csv_quote"),
                get(c, "column_mapping"),
                get(c, "primary_col")
            ))
        for k in get(config, "kml"):
            datasets.append(KMLDataset(
                get(k, "dataset"),
                get(k, "command"),
                get(k, "store_filename"),
                # For ID uniqueness:
                get(k, "primary_prefix") + get(e, "id_prefix"),
                None
            ))
        return Election(
            get(e, "label"),
            datasets,
            get(e, "countryname"),
            get(e, "wikidata"),
            get(e, "osm"),
            get(e, "year"),
            get(e, "date"),
            get(e, "id_prefix")
        )


def parse_arguments(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    """
    Parse the program's command-line arguments
    """
    parser = argparse.ArgumentParser(
        description=PROGRAM_DESCRIPTION)

    # Required arguments
    parser.add_argument(
        '--config', '-c', nargs=1, type=str, required=True,
        help='filename of a configuration json file - see the project ' +
        'readme, as well as the example files, for more details')
    parser.add_argument(
        '--output', '-o', nargs=1, type=str, required=True,
        help='filename for the output .ttl.bz2 file')

    # Optional arguments
    parser.add_argument(
        '--warn-missing-col-mapping', action='store_true',
        help='print a warning if a column from a CSV file has no mapping ' +
        'to a predicate in the config file')
    parser.add_argument(
        '--output-aux-geo', '-x', nargs=1, type=str,
        help='filename for the optional output of an aux-geo file for ' +
        'osm2rdf: this file will contain the RDF subjects and raw well-' +
        'known text strings separated by tab. (Note: for a valid ttl file ' +
        'from osm2rdf you may need to add a @prefix statement for these ' +
        'entities to its output)')

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None):
    """
    The procedure to be run when the program is called directly.
    """

    args = parse_arguments(argv)

    set_warn_missing_col_mapping(args.warn_missing_col_mapping)

    election = Election.load_from_config(args.config[0])
    output: str = args.output[0]
    aux_geo = args.output_aux_geo[0] if args.output_aux_geo else None

    logger.info("election2rdf, arguments: %s", repr(args))

    logger.info("Downloading datasets...")
    start = datetime.now()
    election.get_all_data()
    logger.info("Took %s", str(datetime.now() - start))

    aux_geo_file = open(aux_geo, "w") if aux_geo is not None else nullcontext()
    if not output.endswith(".ttl.bz2"):
        logger.warning("Output filename does not end in '.ttl.bz2'")
    with bz2.open(output, "wb") as f, aux_geo_file as agf:
        election.to_file(TextIOWrapper(f), agf)


if __name__ == "__main__":
    main()
