#!/bin/env python3
"""
create rdf turtle from various datasets: this module provides utilities and an
abstract base class

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
import re
import subprocess
import logging
import bz2
from pathlib import Path
from dataclasses import dataclass, field, InitVar
from typing import Iterator, Optional
from abc import ABC, abstractmethod
from urllib.parse import urlparse
from io import TextIOWrapper

Triple = tuple[str, str, str]

# Collection of regexes to detect datatypes etc.
UNPROBLEMATIC_PREDICATE = re.compile(r"^(\w+:)?\w+$")
UNPROBLEMATIC_PREFIX = re.compile(r"^\w+:\w*$")
UNPROBLEMATIC_DATASET = re.compile(r"^\w+$")
INT_REGEX = re.compile(r'^-?\d+$')
FLOAT_REGEX = re.compile(r'^-?\d+(\.\d+)?$')
DATE_REGEX_DD_MM_YYYY = re.compile(
    r'^(?P<day>\d{2})\.(?P<month>\d{2})\.(?P<year>\d{4})$')
DATE_REGEX_YYYY_MM_DD = re.compile(
    r'^(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2})$')
DATETIME_ISO_REGEX = re.compile(
    r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z'
)
POINT_REGEX = re.compile(
    r'\s*[Pp][Oo][Ii][Nn][Tt]\s*\(\s*(-)?\d+(\.\d+)?\s+(-)?\d+(\.\d+)?\s*\)\s*'
)


@dataclass(frozen=True)
class Prefix:
    """
    Class representing an RDF prefix declaration.
    """

    # Note: this is an immutable / frozen object to avoid problems with set
    # and/or manually defining __hash__.

    prefix: str
    iri: str

    def __post_init__(self):
        # Use only alphanumeric chars for prefix
        assert re.match(r'^\w+$', self.prefix), \
            f"Prefixes must be alphanumeric, given '{self.prefix}'"

        # Use only valid http/https URLs as IRIs
        parsed = urlparse(self.iri)
        assert parsed.scheme in ("http", "https"), \
            f"IRI should have http:// or https:// protocol, given '{self.iri}'"
        assert parsed.netloc, \
            "IRI should be a valid URL with a hostname segment, " + \
            f"given '{self.iri}'"
        assert parsed.path.startswith("/"), \
            "IRI should be a valid URL with at least '/' " + \
            f"as a path segment, given '{self.iri}'"

    def __str__(self) -> str:
        return f"@prefix {self.prefix}: <{self.iri}> .\n"


# Predefined prefixes, this set can be updated by all modules using
# this utility program. All prefixes should be added before producing any
# triples. Then all_prefixes() can be used to get the prefix declarations in
# turtle.
RDFS = Prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
GEO = Prefix("geo", "http://www.opengis.net/ont/geosparql#")
XSD = Prefix("xsd", "http://www.w3.org/2001/XMLSchema#")
DCT = Prefix("dct", "http://purl.org/dc/terms/")
PREFIXES = {RDFS, GEO, XSD, DCT}

# Predefined predicates for consistency across modules
TYPE = "a"
MEMBER = f"{RDFS.prefix}:member"
LABEL = f"{RDFS.prefix}:label"
COMMENT = f"{RDFS.prefix}:comment"
HAS_GEOMETRY = f"{GEO.prefix}:hasGeometry"
HAS_CENTROID = f"{GEO.prefix}:hasCentroid"
AS_WKT = f"{GEO.prefix}:asWKT"
IDENTIFIER = f"{DCT.prefix}:identifier"

# Predefined RDF datatypes
INT_LITERAL_TYPE = f"{XSD.prefix}:integer"
FLOAT_LITERAL_TYPE = f"{XSD.prefix}:decimal"
DATE_LITERAL_TYPE = f"{XSD.prefix}:date"
DATETIME_LITERAL_TYPE = f"{XSD.prefix}:dateTime"
WKT_LITERAL_TYPE = f"{GEO.prefix}:wktLiteral"

# This counter is used to give unique numbers to entities who don't have
# a unique identifier in the origin dataset
GLOBAL_COUNTER = 0

# Using this dictionary global environment variables can be defined that
# will be passed to all programs started for retrieving dataset content
GET_DATA_ENV: dict[str, str] = {}

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def all_prefixes() -> Iterator[str]:
    assert len(PREFIXES) == len(set(p.prefix for p in PREFIXES)), \
        "there may not be multiple IRIs for the same prefix"

    # set() may be unsorted in older Python versions
    # but we want deterministic output
    for prefix in sorted(str(p) for p in PREFIXES):
        yield prefix


def next_id() -> str:
    global GLOBAL_COUNTER
    GLOBAL_COUNTER += 1
    return str(GLOBAL_COUNTER)


def set_get_data_env(key: str, value: str):
    GET_DATA_ENV[key] = value


def triple(t: Triple) -> str:
    return " ".join(t) + " .\n"


def add_datatype(obj: str) -> str:
    """
    Tries to guess the datatype by the content of the string.
    Will encode the string and, if detected, append datatype.
    """
    def encode(obj_type: str = "") -> str:
        if obj_type:
            obj_type = '^^' + obj_type
        return f"{json.dumps(str(obj).strip())}{obj_type}"

    if type(obj) is not str:
        return '""'

    # Is it an int?
    try:
        if INT_REGEX.match(obj):
            int(obj)  # Try conversion
            return encode(INT_LITERAL_TYPE)
    except ValueError:
        pass

    # Is it a float?
    try:
        if FLOAT_REGEX.match(obj):
            float(obj)  # Try conversion
            return encode(FLOAT_LITERAL_TYPE)
    except ValueError:
        pass

    # Is it a date?
    m = DATE_REGEX_DD_MM_YYYY.match(obj)
    if m:
        r = m.groupdict()
        # Needs rewriting to fit into rdf schema
        obj = f"{r['year']}/{r['month']}/{r['day']}"
        return encode(DATE_LITERAL_TYPE)

    if DATE_REGEX_YYYY_MM_DD.match(obj):
        return encode(DATE_LITERAL_TYPE)

    if DATETIME_ISO_REGEX.match(obj):
        return encode(DATETIME_LITERAL_TYPE)

    # Is it a WKT point?
    if POINT_REGEX.match(obj):
        return encode(WKT_LITERAL_TYPE)

    # We don't know, so we keep it a string
    return encode()


@dataclass
class Dataset(ABC):
    """
    Abstract Base Class for Conversion of External Data Sets to RDF.
    """

    # Immutable attributes
    _dataset: InitVar[str]
    _command: InitVar[Optional[str]]
    _store_filename: InitVar[str]
    _primary_prefix: InitVar[str]  # shall include ":"

    # Mutable. Is only used "on the fly" by rdf()
    parent: Optional[str] = None  # fully qualified, for rdfs:member

    __get_data_done: bool = field(init=False, default=False)

    def __post_init__(self, _dataset: str, _command: Optional[str],
                      _store_filename: str, _primary_prefix: str):
        # Invariants
        assert ":" in _primary_prefix \
            and UNPROBLEMATIC_PREFIX.match(_primary_prefix), \
            "primary_prefix should be alphanumeric and contain ':', but " + \
            f"'{_primary_prefix}' given"
        assert _store_filename
        assert UNPROBLEMATIC_DATASET.match(_dataset), \
            f"Please chose an alphanumeric dataset name for '{_dataset}'"

        # Attributes
        self.__dataset = _dataset
        self.__command = _command
        self.__store_filename = _store_filename
        self.__primary_prefix = _primary_prefix

    # Getters
    @property
    def dataset(self) -> str:
        return self.__dataset

    @property
    def command(self) -> Optional[str]:
        return self.__command

    @property
    def store_filename(self) -> str:
        return self.__store_filename

    @property
    def primary_prefix(self) -> str:
        return self.__primary_prefix

    def get_data(self):
        """
        Check if the data has already been downloaded. Otherwise run
        the command in shell and store the stdout to file.
        """
        if Path(self.store_filename).exists():
            if self.command:
                logger.info("%s already exists. skipping.",
                            self.store_filename)
            self.__get_data_done = True
            return
        assert self.command, "No command provided but file not present"
        logger.info("%s: running %s", self.store_filename, self.command)
        with subprocess.Popen(self.command,
                              stdout=subprocess.PIPE,
                              shell=True,
                              env=GET_DATA_ENV,
                              text=True) as proc, \
                open(self.store_filename, "w") as f:
            if proc.stdout:
                for line in proc.stdout:
                    print(line, file=f, end='')
        self.__get_data_done = True

    def content(self) -> Iterator[str]:
        """
        Checks if `get_data()` has been run, then yields data line by line
        """
        assert self.__get_data_done, f"Dataset {self.dataset} not loaded"
        # Some text files may come with BOM which disturbs
        # for ex. csv.DictReader
        with open(self.store_filename, "r", encoding="utf-8-sig") as f:
            yield from f

    @property
    def clean_prefix(self) -> str:
        return self.primary_prefix.split(":")[0] + ":"

    @property
    def type_str(self) -> str:
        return f"{self.clean_prefix}{self.dataset}"

    @abstractmethod
    def rdf(self) -> Iterator[Triple]:
        """
        Emits turtle triples as 3-tuple of strings. One per iteration.
        """
        raise NotImplementedError

    def to_file(self, filename: str) -> int:
        """
        Writes all prefixes and the triples produced by `Dataset.rdf()` to a
        bzip2 compressed file. Returns the number of triples written,
        excluding prefixes.
        """
        counter = 0
        if not filename.endswith(".ttl.bz2"):
            logger.warning(
                "Output filename is expected to end with '.ttl.bz2'")
        self.get_data()
        with bz2.open(filename, "wb") as zf:
            f = TextIOWrapper(zf)
            f.writelines(all_prefixes())
            for t in self.rdf():
                f.write(triple(t))
                counter += 1
            f.close()
        return counter
