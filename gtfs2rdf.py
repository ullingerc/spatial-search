#!/bin/env python3
"""
gtfs2rdf - create RDF turtle from a GTFS zip file

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

import re
import argparse
import logging
import bz2
from io import TextIOWrapper
from typing import Iterator, NotRequired, Optional, TypedDict, Sequence
from dataclasses import dataclass, field, InitVar
from dataset import DATE_LITERAL_TYPE, TYPE, all_prefixes, Prefix, PREFIXES, \
    Triple, triple, HAS_GEOMETRY, AS_WKT, HAS_CENTROID, \
    WKT_LITERAL_TYPE
from csv2rdf import CSVColumnMapping, CSVDataset, CSVValuesMapping, \
    CSVValuesMappingRules, ExtraTripleCallback
from kml2rdf import AuxGeoCallback
from contextlib import nullcontext
from zipfile import ZipFile

PROGRAM_DESCRIPTION = """
    gtfs2rdf - create RDF turtle from a GTFS zip file.

    The output of gtfs2rdf is compatible with the "Linked GTFS" standard.
    (See https://github.com/OpenTransport/linked-gtfs/blob/master/spec.md)

    The program augments the data with additional GeoSPARQL triples.

    The program is licensed under the GNU General Public License version 3,
    see LICENSE for details.
"""


logger = logging.getLogger(__name__)

# Prefixes from Linked GTFS standard
GTFS = Prefix("gtfs", "http://vocab.gtfs.org/terms#")
DCT = Prefix("dct", "http://purl.org/dc/terms/")
DCAT = Prefix("dcat", "http://www.w3.org/ns/dcat#")
FOAF = Prefix("foaf", "http://xmlns.com/foaf/0.1/")
SCHEMA = Prefix("schema", "http://schema.org/")

PREFIXES.update({GTFS, DCT, DCAT, FOAF, SCHEMA})

# Type annotations for internal configuration objects
GtfsFileCsvConfig = TypedDict("GtfsFileCsvConfig", {
    "dataset": str,
    "primary_prefix": str,
    "primary_col": Optional[str],
    "column_mapping": CSVColumnMapping,
    "values_mapping": CSVValuesMapping,
    "extra_triple_callback": NotRequired[ExtraTripleCallback]
})
GtfsCsvConfig = dict[str, GtfsFileCsvConfig]
# Shape cache: Subject => List of (Sequence Number, Longitude, Latitude)
GtfsShapeCache = dict[str, list[tuple[int, str, str]]]


@dataclass
class GTFSFeed:
    """
    A `GTFSFeed` object represents an entire GTFS .zip file which will be
    converted to RDF. The result is Linked GTFS compliant but provides
    additional triples.

    Fields:

    - `_feed`:              an identifier for this feed (alphanumerics and
                            underscore), will be used to make IRIs unique,
                            such that multiple outputs on different feeds
                            can be joined in a single RDF data set.
    - `_filename`:          the input zip file's path
    - `_excludes`:          a list of zip member filenames to ignore
    - `_add_linestrings`:   a boolean, whether the shapes found in the feed
                            should be emitted also as well-known text line
                            string geometries in addition to the standardized
                            shape points; requires memory for remembering
                            shapes during computation; does nothing if
                            shapes.txt is excluded.
    - `aux_geo_callback`:   A callback to emit well-known text geometries for
                            an osm2rdf aux-geo file, see kml2rdf (optional)

    The parameters prefixed with an underscore are immutable after creation and
    may not be safely changed.
    """

    # User input: Immutable from outside via InitVar after initialization
    _feed: InitVar[str]
    _filename: InitVar[str]
    _excludes: InitVar[list[str]]
    _add_linestrings: InitVar[bool]
    aux_geo_callback: AuxGeoCallback = None

    # Internal configuration attributes
    __datasets: list[CSVDataset] = field(default_factory=list, init=False)
    __config: GtfsCsvConfig = field(default_factory=dict, init=False)
    __members: list[str] = field(default_factory=list, init=False)
    __shapes: GtfsShapeCache = field(
        default_factory=dict, init=False)

    def __post_init__(self, _feed: str, _filename: str, _excludes: list[str],
                      _add_linestrings: bool):
        # Invariant
        assert re.match(r"^\w+$", _feed), "Feed name must be alphanum."
        # Properties
        self.__feed = _feed
        self.__filename = _filename
        self.__excludes = _excludes
        self.__add_linestrings = _add_linestrings
        # Prepare
        self.__make_datasets()

    # Attribute getters
    @property
    def feed(self) -> str:
        return self.__feed

    @property
    def filename(self) -> str:
        return self.__filename

    @property
    def excludes(self) -> list[str]:
        return self.__excludes

    @property
    def add_linestrings(self) -> bool:
        return self.__add_linestrings

    # Initialization and internal helper functions
    def __make_config(self):
        """
        Prepare the `__config` dictionary containing the individual settings
        for constructing one CSVDataset per file for all of the different
        supported GTFS tables.
        """
        if self.__config != {}:
            return

        int2bool = ([("^0$", "\"false\"^^xsd:boolean"), (
                    "^1$", "\"true\"^^xsd:boolean")], False)

        dateAddition2bool = ([("^2$", "\"false\"^^xsd:boolean"), (
            "^1$", "\"true\"^^xsd:boolean")], False)

        replace_nalph = ("(?P<m>\\W)", lambda m: format(
            ord(m.group("m")), '#08x'))

        def foreign(table: str) -> CSVValuesMappingRules:
            return [replace_nalph,
                    ("^(?P<content>.*)$",
                     f"gtfs:{table}_{self.feed}_\\g<content>")]

        pickUp_dropOff = ([
            ("^$", "gtfs:Regular"),
            ("^0$", "gtfs:Regular"),
            ("^1$", "gtfs:NotAvailable"),
            ("^2$", "gtfs:MustPhone"),
            ("^3$", "gtfs:MustCoordinateWithDriver"),
        ], False)

        def date(s: str) -> str:
            if len(s) != 8:
                return f'"{s}"'
            return f'"{s[0:4]}/{s[4:6]}/{s[6:8]}"^^{DATE_LITERAL_TYPE}'

        def temporal(subj: str, start: str, end: str) -> Iterator[Triple]:
            yield (subj, "dct:temporal", subj + "_temporal")
            yield (subj + "_temporal", "schema:startDate", date(start))
            yield (subj + "_temporal", "schema:endDate", date(end))

        def calendar_etc(_: CSVDataset, subj: str, row: dict[str, str]) \
                -> Iterator[Triple]:
            yield from temporal(subj, row["start_date"], row["end_date"])

        def feed_info_etc(_: CSVDataset, subj: str, row: dict[str, str]) \
                -> Iterator[Triple]:
            yield (subj, TYPE, "dcat:Dataset")
            yield from temporal(subj, row["feed_start_date"],
                                row["feed_end_date"])

        def point(subj: str, lon: str, lat: str) -> Iterator[Triple]:
            yield (subj, HAS_GEOMETRY, subj + "_geo")
            yield (subj, HAS_CENTROID, subj + "_geo")
            yield (subj + "_geo", AS_WKT,
                   f"\"POINT({lon} {lat})\"^^{WKT_LITERAL_TYPE}")
            if self.aux_geo_callback:
                self.aux_geo_callback(subj, f"POINT({lon} {lat})")

        def shapes_etc(_: CSVDataset, subj: str, row: dict[str, str]) \
                -> Iterator[Triple]:
            shape_subj = "gtfs:shape_" + self.feed + "_" + \
                re.sub(*replace_nalph, row["shape_id"])
            yield (shape_subj, TYPE, "gtfs:Shape")
            yield (shape_subj, "gtfs:shapePoint", subj)
            yield from point(subj, row['shape_pt_lon'], row['shape_pt_lat'])
            if self.add_linestrings:
                if shape_subj not in self.__shapes:
                    self.__shapes[shape_subj] = []
                shape_point = (
                    int(row['shape_pt_sequence']),
                    row['shape_pt_lon'],
                    row['shape_pt_lat']
                )
                self.__shapes[shape_subj].append(shape_point)

        def stops_etc(_: CSVDataset, subj: str, row: dict[str, str]) \
                -> Iterator[Triple]:
            yield from point(subj, row['stop_lon'], row['stop_lat'])
            if "parent_station" in row and row["parent_station"]:
                yield (f"gtfs:station_{self.feed}_" +
                       re.sub(*replace_nalph, row['parent_station']),
                       TYPE, "gtfs:Station")

        self.__config = {
            "agency.txt": {
                "dataset": "Agency",
                "primary_prefix": "gtfs:agency_" + self.feed + "_",
                "primary_col": "agency_id",
                "column_mapping": {
                    "agency_id": "dct:identifier",
                    "agency_name": "foaf:name",
                    "agency_url": "foaf:page",
                    "agency_timezone": "gtfs:timeZone",
                    "agency_lang": "dct:language",
                    "agency_phone": "foaf:phone"
                },
                "values_mapping": {
                    "dct:identifier": ([replace_nalph], True)
                }
            },
            "calendar.txt": {
                "dataset": "CalendarRule",
                "primary_prefix": "gtfs:calendar_" + self.feed + "_",
                "primary_col": None,
                "column_mapping": {
                    "service_id": "gtfs:service",
                    "monday": "gtfs:monday",
                    "tuesday": "gtfs:tuesday",
                    "wednesday": "gtfs:wednesday",
                    "thursday": "gtfs:thursday",
                    "friday": "gtfs:friday",
                    "saturday": "gtfs:saturday",
                    "sunday": "gtfs:sunday",
                    # Additional
                    "start_date": "schema:startDate",
                    "end_date": "schema:endDate"
                },
                "values_mapping": {
                    "gtfs:service": (foreign("service"), False),
                    "gtfs:monday": int2bool,
                    "gtfs:tuesday": int2bool,
                    "gtfs:wednesday": int2bool,
                    "gtfs:thursday": int2bool,
                    "gtfs:friday": int2bool,
                    "gtfs:saturday": int2bool,
                    "gtfs:sunday": int2bool,
                },
                # Provides dct:temporal
                "extra_triple_callback": calendar_etc
            },
            "calendar_dates.txt": {
                "dataset": "CalendarDateRule",
                "primary_prefix": "gtfs:calendar_date_" + self.feed + "_",
                "primary_col": None,
                "column_mapping": {
                    "service_id": "gtfs:service",
                    "date": "dct:date",
                    "exception_type": "gtfs:dateAddition"
                },
                "values_mapping": {
                    "gtfs:service": (foreign("service"), False),
                    "gtfs:dateAddition": dateAddition2bool
                }
            },
            "feed_info.txt": {
                "dataset": "Feed",
                "primary_prefix": "gtfs:feed_" + self.feed + "_",
                "primary_col": None,
                "column_mapping": {
                    "feed_publisher_url": "dct:publisher",
                    "feed_lang": "dct:language",
                    "feed_version": "schema:version",
                    "feed_contact_mail": "dcat:contactPoint",
                    # Additional
                    "feed_publisher_name": "rdfs:label",
                    "feed_start_date": "schema:startDate",
                    "feed_end_date": "schema:endDate",
                },
                "values_mapping": {},
                # Provides a dcat:Dataset and dct:temporal
                "extra_triple_callback": feed_info_etc
            },
            "frequencies.txt": {
                "dataset": "Frequency",
                "primary_prefix": "gtfs:frequency_" + self.feed + "_",
                "primary_col": None,
                "column_mapping": {
                    "trip_id": "gtfs:trip",
                    "start_time": "gtfs:startTime",
                    "end_time": "gtfs:endTime",
                    "headway_secs": "gtfs:headwaySeconds",
                    "exact_times": "gtfs:exactTimes"
                },
                "values_mapping": {
                    "gtfs:trip": (foreign("trip"), False)
                }
            },
            # Not contained in Linked GTFS standard...
            # "levels.txt": {
            #     "dataset": "",
            #     "primary_prefix": "" + self.feed + "_",
            #     "primary_col": "",
            #     "column_mapping": {},
            #     "values_mapping": {}
            # },
            # "pathways.txt": {
            #     "dataset": "",
            #     "primary_prefix": "" + self.feed + "_",
            #     "primary_col": "",
            #     "column_mapping": {"pathway_id", "from_stop_id","to_stop_id",
            #                        "pathway_mode", "is_bidirectional",
            #                        "traversal_time", "length",
            #                        "stair_count", "max_slope", "min_width",
            #                        "signposted_as"},
            #     "values_mapping": {}
            # },
            "routes.txt": {
                "dataset": "Route",
                "primary_prefix": "gtfs:route_" + self.feed + "_",
                "primary_col": "route_id",
                "column_mapping": {
                    "route_id": "dct:identifier",
                    "agency_id": "gtfs:agency",
                    "route_short_name": "gtfs:shortName",
                    "route_long_name": "gtfs:longName",
                    "route_type": "gtfs:routeType",
                    "route_color": "gtfs:color",
                    "route_text_color": "gtfs:textColor",
                    "route_desc": "dct:description"
                },
                "values_mapping": {
                    "dct:identifier": ([replace_nalph], True),
                    "gtfs:agency": ([
                        replace_nalph,
                        ("^(?P<content>.*)$",
                            f"gtfs:agency_{self.feed}_\\g<content>")
                    ], False),
                    "gtfs:routeType": ([
                        ("^0$", "gtfs:LightRail"),
                        ("^1$", "gtfs:Subway"),
                        ("^2$", "gtfs:Rail"),
                        ("^3$", "gtfs:Bus"),
                        ("^4$", "gtfs:Ferry"),
                        ("^5$", "gtfs:CableCar"),
                        ("^6$", "gtfs:Gondola"),
                        ("^7$", "gtfs:Funicular"),
                        ("^(?P<v>\\d+)$", "\"\\g<v>\"")
                    ], False)
                }
            },
            "shapes.txt": {
                "dataset": "ShapePoint",
                "primary_prefix": "gtfs:shapepoint_" + self.feed + "_",
                "primary_col": None,
                "column_mapping": {
                    "shape_id": None,
                    "shape_pt_lat": "geo:lat",
                    "shape_pt_lon": "geo:long",
                    "shape_pt_sequence": "gtfs:pointSequence"
                },
                "values_mapping": {},
                # Provides: geo:hasGeometry/geo:asWKT and
                # "gtfs:Shape gtfs:shapePoint x"
                # If add_linestrings is True, also prepares line string
                # points
                "extra_triple_callback": shapes_etc
            },
            "stops.txt": {
                "dataset": "Stop",
                "primary_prefix": "gtfs:stop_" + self.feed + "_",
                "primary_col": "stop_id",
                "column_mapping": {
                    "stop_id": "dct:identifier",
                    "stop_code": "gtfs:code",
                    "stop_name": "foaf:name",
                    "stop_desc": "dct:description",
                    "stop_lat": "geo:lat",
                    "stop_lon": "geo:long",
                    "parent_station": "gtfs:parentStation",
                    "wheelchair_boarding": "gtfs:wheelchairAccessible",
                    # inofficial
                    "platform_code": "gtfs:platform",
                    # "location_type": "",
                    # "level_id": ""
                },
                "values_mapping": {
                    "dct:identifier": ([replace_nalph], True),
                    "gtfs:parentStation": (foreign("station"), False)
                },
                "extra_triple_callback": stops_etc
            },
            "stop_times.txt": {
                "dataset": "StopTime",
                "primary_prefix": "gtfs:stop_time_" + self.feed + "_",
                "primary_col": None,
                "column_mapping": {
                    "trip_id": "gtfs:trip",
                    "arrival_time": "gtfs:arrivalTime",
                    "departure_time": "gtfs:departureTime",
                    "stop_id": "gtfs:stop",
                    "stop_sequence": "gtfs:stopSequence",
                    "pickup_type": "gtfs:pickupType",
                    "drop_off_type": "gtfs:dropOffType",
                    "stop_headsign": "gtfs:headsign"
                },
                "values_mapping": {
                    "gtfs:trip": (foreign("trip"), False),
                    "gtfs:stop": (foreign("stop"), False),
                    "gtfs:pickupType": pickUp_dropOff,
                    "gtfs:dropOffType": pickUp_dropOff,
                }
            },
            "transfers.txt": {
                "dataset": "TransferRule",
                "primary_prefix": "gtfs:transfer_rule_" + self.feed + "_",
                "primary_col": None,
                "column_mapping": {
                    "from_stop_id": "gtfs:originStop",
                    "to_stop_id": "gtfs:destinationStop",
                    "transfer_type": "gtfs:transferType",
                    "min_transfer_time": "gtfs:minimumTransferTime",
                    # Additional
                    "from_route_id": "gtfs:originRoute",
                    "to_route_id": "gtfs:destinationRoute",
                    "from_trip_id": "gtfs:originTrip",
                    "to_trip_id": "gtfs:destinationTrip"
                },
                "values_mapping": {
                    "gtfs:transferType": ([
                        ("0", "gtfs:RecommendedTransfer"),
                        ("1", "gtfs:EnsuredTransfer"),
                        ("2", "gtfs:MinimumTimeTransfer"),
                        ("3", "gtfs:NoTransfer"),
                        # Additional
                        ("4", "gtfs:InSeatTransfer"),
                        ("5", "gtfs:NoInSeatTransfer")
                    ], False),
                    "gtfs:originStop": (foreign("stop"), False),
                    "gtfs:destinationStop": (foreign("stop"), False),
                    "gtfs:originRoute": (foreign("route"), False),
                    "gtfs:destinationRoute": (foreign("route"), False),
                    "gtfs:originTrip": (foreign("trip"), False),
                    "gtfs:destinationTrip": (foreign("trip"), False)
                }
            },
            "trips.txt": {
                "dataset": "Trip",
                "primary_prefix": "gtfs:trip_" + self.feed + "_",
                "primary_col": "trip_id",
                "column_mapping": {
                    "route_id": "gtfs:route",
                    "service_id": "gtfs:service",
                    "trip_id": "dct:identifier",
                    "trip_headsign": "gtfs:headsign",
                    "trip_short_name": "gtfs:shortName",
                    "direction_id": "gtfs:direction",
                    "block_id": "gtfs:block",
                    "shape_id": "gtfs:shape",
                    "wheelchair_accessible": "gtfs:wheelchairAccessible",
                    "bikes_allowed": "gtfs:bikesAllowed"
                },
                "values_mapping": {
                    "dct:identifier": ([replace_nalph], True),
                    "gtfs:service": (foreign("service"), False),
                    "gtfs:route": (foreign("route"), False),
                    "gtfs:shape": (foreign("shape"), False),
                    "gtfs:direction": int2bool,
                    "gtfs:wheelchairAccessible": int2bool,
                    "gtfs:bikesAllowed": int2bool
                }
            }
        }

    def __get_members(self):
        """
        Retrieves the list of member files of the input zip.
        """
        if self.__members != []:
            return
        logger.info("Reading input file's list of members: %s", self.filename)
        with ZipFile(self.filename, "r") as zf:
            self.__members = zf.namelist()
        logger.info("Input file contains: %s", ", ".join(self.__members))

    def __make_datasets(self):
        """
        Populates the `__dataset` list with the `CSVDataset` objects required
        for the GTFS to RDF conversion. Will call the required other
        preparation steps implicitly.
        """
        if self.__datasets != []:
            return

        logger.info("Prepare configuration")
        self.__get_members()
        self.__make_config()

        additional = set(self.__members) - set(self.__config.keys())
        if len(additional) > 0:
            logger.warning(
                "Additional files were found in the input zip " +
                "that are currently not supported by gtfs2rdf: %s",
                ", ".join(additional))

        for table, config in self.__config.items():
            if table in self.excludes:
                logger.info("Skipping %s because it is excluded", table)
                continue
            if table not in self.__members:
                logger.info(
                    "Skipping %s because it is not present in input zip",
                    table)
                continue

            d = CSVDataset(
                _dataset=config["dataset"],
                _command=None,  # f"unzip -qq -c $GTFS_FILE {table}"
                _store_filename=table,
                _primary_prefix=config["primary_prefix"],
                parent="gtfs:feed_" + self.feed,
                csv_separator=",",
                csv_quote="\"",
                column_mapping=config["column_mapping"],
                primary_col=config["primary_col"],
                values_mapping=config["values_mapping"],
                extra_triple_callback=config.get("extra_triple_callback", None)
            )
            self.__datasets.append(d)

        assert self.__datasets != [], "No supported files found in input zip"

    # Public user functions
    def get_all_data(self):
        """
        Extracts all supported, not-excluded files from the given input zip
        into the current working directory.
        """
        assert self.__datasets != [], \
            "Did not call __make_datasets() before calling get_all_data()"
        logger.info("Extracting data from %s...", self.filename)
        with ZipFile(self.filename, "r") as zf:
            for d in self.__datasets:
                logger.info("Extracting %s", d.store_filename)
                zf.extract(d.store_filename)
                # set_get_data_env("GTFS_FILE", self.filename)
                d.get_data()

    def rdf(self) -> Iterator[Triple]:
        assert self.__datasets != [], \
            "Please call get_all_data() first"

        count = 0
        dataset = ""

        def _count():
            nonlocal count
            count += 1
            if count % 100000 == 0:
                logger.info(
                    "%d triples emitted in total, " +
                    "currently processing %s", count, dataset)

        # Reset shapes cache for multiple runs of rdf()
        self.__shapes = {}

        for d in self.__datasets:
            logger.info("Emitting triples for %s", d.dataset)
            dataset = d.dataset
            for t in d.rdf():
                _count()
                yield t

        if self.add_linestrings and "shapes.txt" not in self.excludes:
            logger.info("Emitting geometry triples for shapes")
            dataset = "Shapes to LineStrings"
            for subj, points in self.__shapes.items():
                _count()
                yield (subj, HAS_GEOMETRY, subj + "_geo")
                _count()
                points_str = ", ".join(
                    p[1] + " " + p[2] for p in sorted(points)
                )
                yield (subj + "_geo", AS_WKT,
                       f"\"LINESTRING({points_str})\"^^{WKT_LITERAL_TYPE}")
                if self.aux_geo_callback:
                    self.aux_geo_callback(subj, f"LINESTRING({points_str})")
        elif self.add_linestrings:
            logger.warning(
                "'add_linestrings' is true but 'shapes.txt' is excluded.")

        logger.info("Complete. %d triples emitted.", count)

    def to_file(self, output: TextIOWrapper, emit_prefixes: bool = True):
        """
        Takes two output files and produces TTL and aux-geo TSV for the given
        election
        """
        if emit_prefixes:
            logger.info("Emitting prefixes")
            output.writelines(all_prefixes())
        logger.info("Emitting data:")
        for t in self.rdf():
            output.write(triple(t))
        output.close()


def parse_arguments(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    """
    Parse the program's command-line arguments
    """
    parser = argparse.ArgumentParser(
        description=PROGRAM_DESCRIPTION)

    # Required arguments
    parser.add_argument(
        '--feed', '-f', nargs=1, type=str, required=True,
        help='identifier for the feed (alphanumeric chars and underscores)')
    parser.add_argument(
        '--input', '-i', nargs=1, type=str, required=True,
        help='filename of the input GTFS .zip file')
    parser.add_argument(
        '--output', '-o', nargs=1, type=str, required=True,
        help='filename for the bzip2 compressed output turtle file (.ttl.bz2)')

    # Optional arguments
    parser.add_argument(
        '--output-aux-geo', '-x', nargs=1, type=str,
        help='filename for the optional output of an aux-geo file for ' +
        'osm2rdf: this file will contain the RDF subjects and raw well-' +
        'known text strings separated by tab. (Note: for a valid ttl file ' +
        'from osm2rdf you may need to add a @prefix statement for these ' +
        'entities to its output)')
    parser.add_argument(
        '--exclude', '-e', nargs='*', type=str,
        help='names of member files from GTFS .zip to exclude from conversion')
    parser.add_argument(
        '--add-linestrings', '-l', action='store_true',
        help='translate GTFS shapes.txt to WKT LINESTRING geometries ' +
        '(requires that shapes.txt fits into memory)')
    parser.add_argument(
        '--skip-prefixes', '-n', action='store_true',
        help='do not output @prefix declarations')

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None):
    """
    The procedure to be run when the program is called directly.
    """

    args = parse_arguments(argv)
    logger.info("gtfs2rdf: arguments: %s", repr(args))
    _feed, _input, _output, _exclude, _add_linestrings, _aux_geo = \
        args.feed[0], args.input[0], str(args.output[0]), \
        args.exclude or [], args.add_linestrings, \
        args.output_aux_geo[0] if args.output_aux_geo else None

    gtfs = GTFSFeed(_feed, _input, _exclude, _add_linestrings)
    gtfs.get_all_data()

    if not _output.endswith(".ttl.bz2"):
        logger.warning("Output filename does not end in '.ttl.bz2'")
    _agf = open(_aux_geo, "w") if _aux_geo is not None else nullcontext()
    with bz2.open(_output, "wb") as f, _agf as agf:
        def agc(subj: str, wkt: str):
            if _aux_geo is not None:
                print(subj + "\t" + wkt, file=agf)
        gtfs.aux_geo_callback = agc
        gtfs.to_file(TextIOWrapper(f), not args.skip_prefixes)


if __name__ == "__main__":
    main()
