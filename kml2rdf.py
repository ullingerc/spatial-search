#!/bin/env python3
"""
kml2rdf - create RDF turtle from a KML or KMZ file

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

from zipfile import ZipFile
from dataset import PREFIXES, Prefix, WKT_LITERAL_TYPE, \
    Dataset, Triple, next_id, TYPE, MEMBER, LABEL, COMMENT, IDENTIFIER, \
    HAS_GEOMETRY, AS_WKT, add_datatype
from contextlib import nullcontext
import logging
import argparse
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Any, Sequence
from abc import ABC, abstractmethod
from io import StringIO

PROGRAM_DESCRIPTION = """
    kml2rdf - create RDF turtle from a KML or KMZ file.

    Convert a file in Keyhole Markup Language (KML) to an RDF knowledge-graph
    in turtle format. This program supports Placemarks (with id, name and
    description) containing points, line strings, polygons and collections
    thereof.

    The program is licensed under the GNU General Public License version 3,
    see LICENSE for details.
"""

KML_PARSING_VERBOSE = False

AuxGeoCallback = Optional[Callable[[str, str], Any]]  # Id, unescaped-WKT

logger = logging.getLogger(__name__)


@dataclass
class KMLXPathHelper:
    """
    This helper class provides XPath queries for extracting geometries from KML
    """
    schema: str = "http://www.opengis.net/kml/2.2"
    schema_gx: str = "http://www.google.com/kml/ext/2.2"

    @staticmethod
    def from_kml_source(source: str) -> 'KMLXPathHelper':
        """
        Create the XPath helper from a KML source string (detect namespace)
        """

        # Extract namespaces from KML source
        # Source: https://stackoverflow.com/a/42372404
        iterp = ET.iterparse(StringIO(source), events=['start-ns'])
        namespaces = dict(node for _, node in iterp)

        # We require the main namespace from <kml xmlns=...>
        assert '' in namespaces
        assert '/kml/' in namespaces['']

        # Get namespace URLs
        main = namespaces['']
        gx = namespaces.get("gx", None)

        # Construct xpath helper
        if gx:
            return KMLXPathHelper(main, gx)
        else:
            return KMLXPathHelper(main)

    @property
    def KML_SCHEMA(self) -> str:
        return f"{{{self.schema}}}"

    @property
    def KML_GX_SCHEMA(self) -> str:
        return f"{{{self.schema_gx}}}"

    # The following XPaths are based on the helpful KML code examples from
    # https://developers.google.com/kml/documentation/kml_tut

    @property
    def KML_PLACEMARK_XPATH(self) -> str:
        return f".//{self.KML_SCHEMA}Placemark"

    @property
    def KML_NAME_XPATH(self) -> str:
        return f"./{self.KML_SCHEMA}name"

    @property
    def KML_DESCRIPTION_XPATH(self) -> str:
        return f"./{self.KML_SCHEMA}description"

    @property
    def KML_MULTI_XPATH(self) -> str:
        return f"./{self.KML_SCHEMA}MultiGeometry"

    @property
    def KML_POLYGON_XPATH(self) -> str:
        return f"./{self.KML_SCHEMA}Polygon"

    @property
    def KML_POLYGON_OUTER_XPATH(self) -> str:
        return f"./{self.KML_SCHEMA}outerBoundaryIs/" + \
            f"{self.KML_SCHEMA}LinearRing/{self.KML_SCHEMA}coordinates"

    @property
    def KML_POLYGON_INNER_XPATH(self) -> str:
        return f"./{self.KML_SCHEMA}innerBoundaryIs/" + \
            f"{self.KML_SCHEMA}LinearRing/{self.KML_SCHEMA}coordinates"

    @property
    def KML_POINT_XPATH(self) -> str:
        return f"./{self.KML_SCHEMA}Point/{self.KML_SCHEMA}coordinates"

    @property
    def KML_LINE_XPATH(self) -> str:
        return f"./{self.KML_SCHEMA}LineString/{self.KML_SCHEMA}coordinates"

    # Extended syntax xpaths

    @property
    def KML_GX_TRACK_XPATH(self) -> str:
        return f"./{self.KML_GX_SCHEMA}Track"

    @property
    def KML_GX_COORDS_XPATH(self) -> str:
        return f"./{self.KML_GX_SCHEMA}coord"


@dataclass
class Geometry(ABC):
    """
    Abstract Base Class for any supported geometric object.

    Provides virtual/abstract methods for loading from a python ElementTree
    containing a KML Placemark, as well as exporting the geometry as
    well-known text representation.
    """

    @property
    @abstractmethod
    def wkt_type(self) -> str:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def from_kml(node: ET.Element, xpath: KMLXPathHelper) \
            -> Iterator['Geometry']:
        raise NotImplementedError

    @abstractmethod
    def to_wkt(self, geometry_type: bool = True) -> str:
        raise NotImplementedError

    def wkt_literal(self, datatype: bool = True) -> str:
        wkt = self.to_wkt()
        if datatype:
            return f"\"{wkt}\"^^{WKT_LITERAL_TYPE}"
        else:
            return wkt


@Geometry.register
@dataclass
class Point(Geometry):
    """
    A simple 2D Point.
    """

    lat: float
    lng: float

    def __post_init__(self):
        assert -90.0 <= self.lat <= 90.0
        assert -180.0 <= self.lng <= 180.0

    @property
    def wkt_type(self) -> str:
        return "POINT"

    @staticmethod
    def from_kml_coords(coords_str: str) -> 'Point':
        coords = coords_str.rstrip().split(",")
        lng, lat = float(coords[0]), float(coords[1])
        return Point(lat, lng)

    @staticmethod
    def from_kml(node: ET.Element, xpath: KMLXPathHelper) \
            -> Iterator['Point']:
        for el in node.iterfind(xpath.KML_POINT_XPATH):
            yield Point.from_kml_coords(el.text or "")

    def to_wkt(self, geometry_type: bool = True) -> str:
        return (self.wkt_type if geometry_type else '') + \
            f"({self.lng} {self.lat})"


@Geometry.register
@dataclass
class LineString(Geometry):
    """
    A 2D line with with inflection points given by a list of points.
    """

    members: list[Point]

    def __post_init__(self):
        assert len(self.members) >= 2

    @property
    def wkt_type(self) -> str:
        return "LINESTRING"

    @staticmethod
    def from_kml_coords(coords_str: str) -> Optional['LineString']:
        members: list[Point] = [
            Point.from_kml_coords(point)
            for point in coords_str.split()
            if point
        ]
        if len(members) >= 2:
            return LineString(members)

    @staticmethod
    def from_kml(node: ET.Element, xpath: KMLXPathHelper) \
            -> Iterator['LineString']:
        for el in node.iterfind(xpath.KML_LINE_XPATH):
            line = LineString.from_kml_coords(el.text or "")
            if line:
                yield line

        # Also try the extended track syntax
        for track_el in node.iterfind(xpath.KML_GX_TRACK_XPATH):
            members: list[Point] = []
            for el in track_el.iterfind(xpath.KML_GX_COORDS_XPATH):
                if el.text:
                    parts = el.text.split()
                    members.append(
                        Point(lng=float(parts[0]), lat=float(parts[1])))
            if len(members) >= 2:
                yield LineString(members)

    def to_wkt(self, geometry_type: bool = True) -> str:
        coords = (f"{member.lng} {member.lat}" for member in self.members)
        return f"{self.wkt_type if geometry_type else ''}({', '.join(coords)})"


@Geometry.register
@dataclass
class Polygon(Geometry):
    """
    A 2D polygon given by an `outer` line and optionally any number of cut-outs
    given by `innner` lines.
    """

    outer: LineString
    inner: list[LineString]

    @property
    def wkt_type(self) -> str:
        return "POLYGON"

    @staticmethod
    def from_kml(node: ET.Element, xpath: KMLXPathHelper) \
            -> Iterator['Polygon']:
        # Outer
        for el in node.iterfind(xpath.KML_POLYGON_XPATH):
            el_outer = el.find(xpath.KML_POLYGON_OUTER_XPATH)
            if el_outer is None:
                continue
            outer = LineString.from_kml_coords(el_outer.text or "")

            # Inner (multiple holes possible)
            inner: list[LineString] = []
            for el_inner in el.iterfind(xpath.KML_POLYGON_INNER_XPATH):
                inner_ = LineString.from_kml_coords(
                    el_inner.text or "")
                if inner_:
                    inner.append(inner_)

            if outer:
                yield Polygon(outer, inner)

    def to_wkt(self, geometry_type: bool = True) -> str:
        items: list[str] = [self.outer.to_wkt(False)] + \
            [inner.to_wkt(False) for inner in self.inner]
        return (self.wkt_type if geometry_type else '') + \
            f"({', '.join(items)})"


@Geometry.register
@dataclass
class GeometryCollection(Geometry):
    """
    A collection of any of the defined `Geometry` classes.

    It is "homogeneous", iff all elements are geometries of the same type and
    that type is primitive (`Point`, `LineString`, `Polygon`). This will lead
    to a different geometry type in the well-known text representation:
    `MULTIPOINT`, `MULTILINESTRING` or `MULTIPOLYGON` instead of
    `GEOMETRYCOLLECTION`.
    """

    members: list[Polygon | LineString | Point]

    def __post_init__(self):
        assert len(self.members)

    def homogeneous(self) -> Optional[str]:
        """
        Checks if all members are geometries of the same primitive type,
        if yes, returns the type, otherwise returns None.
        """
        geometry_types: set[str] = set(
            member.wkt_type for member in self.members
        )
        if len(geometry_types) == 1:
            geometry_type = tuple(geometry_types)[0]
            if geometry_type in ("POINT", "LINESTRING", "POLYGON"):
                return geometry_type

    @property
    def wkt_type(self) -> str:
        homogeneous = self.homogeneous()
        if homogeneous:
            return "MULTI" + homogeneous
        else:
            return "GEOMETRYCOLLECTION"

    @staticmethod
    def from_kml(node: ET.Element, xpath: KMLXPathHelper) \
            -> Iterator[Geometry]:
        for el in node.iterfind(xpath.KML_MULTI_XPATH):
            members = list(Point.from_kml(el, xpath)) + \
                list(LineString.from_kml(el, xpath)) + \
                list(Polygon.from_kml(el, xpath))

            if len(members) > 0:
                yield GeometryCollection(members)

    def to_wkt(self, geometry_type: bool = True) -> str:
        heterogen = self.homogeneous() is None
        content = ', '.join(
            member.to_wkt(heterogen) for member in self.members
        )
        return f"{self.wkt_type if geometry_type else ''}({content})"


@dataclass
class KMLPlacemark:
    """
    Represents a processed <Placemark> subtree from KML.
    """

    geometry: Geometry
    placemark_id: Optional[str]
    name: Optional[str]
    description: Optional[str]

    @staticmethod
    def from_kml(node: ET.Element, xpath: KMLXPathHelper) \
            -> list['KMLPlacemark']:
        def get_text(_el: ET.Element, _xpath: str) -> Optional[str]:
            el_text = _el.find(_xpath)
            if type(el_text) is ET.Element and el_text.text:
                return el_text.text

        result: list['KMLPlacemark'] = []
        for el in node.iterfind(xpath.KML_PLACEMARK_XPATH):
            if type(el) is ET.Element:
                # Geometry
                geometry = list(Point.from_kml(el, xpath)) + \
                    list(LineString.from_kml(el, xpath)) + \
                    list(Polygon.from_kml(el, xpath)) + \
                    list(GeometryCollection.from_kml(el, xpath))
                assert len(geometry) <= 1, \
                    "The KML standard allows only one geometry per placemark."

                # Metadata
                placemark_id = el.attrib.get("id", None)
                name = get_text(el, xpath.KML_NAME_XPATH)
                description = get_text(el, xpath.KML_DESCRIPTION_XPATH)
                # ... could add "./TimeStamp/when"

                # Is valid?
                if geometry:
                    result.append(KMLPlacemark(
                        geometry[0], placemark_id, name, description
                    ))
        return result


@Dataset.register
@dataclass
class KMLDataset(Dataset):
    """
    Represents a KML file to be converted to RDF.

    Supported KML tags are:

    - `<Placemark>`
    - `<Placemark id="">`
    - `<name>`
    - `<description>`
    - `<Point>`
    - `<LineString>`
    - `<gx:Track>`
    - `<Polygon>`
    - `<MultiGeometry>`

    Fields:

    - `_dataset`:               A name for the dataset (alphanumeric characters
                                and underscores)
    - `_command`:               Shell command to download or generat the data
                                (optional)
    - `_store_filename`:        Filename of the data set. If this file does not
                                exist, `_command` will be run and its stdout
                                will be written to this file.
    - `_primary_prefix`:        The prefix to be used by default for entities
                                from this data set.
    - `parent`:                 A fully qualified RDF entity to be used to emit
                                a `rdfs:member` triple for each Placemark.
                                (optional)
    - `aux_geo_callback`:       A callback that will be called for every
                                geometry that is found. It will recieve the
                                current Placemark's RDF subject and a raw
                                WKT string as arguments. (optional)

    Fields prefixed with underscore are immutable after creation.
    The remaining fields may be changed inbetween calls to the rdf() function.
    """

    # Mutable. May be changed between rdf() calls
    aux_geo_callback: AuxGeoCallback = field(default=None)

    def rdf(self) -> Iterator[Triple]:
        """
        Converts a Keyhole-Markup-Language (KML) file containing <Placemark>s
        with supported geometries to RDF triples. See: `Dataset.rdf`
        """
        content = "\n".join(self.content())
        xpath = KMLXPathHelper.from_kml_source(content)

        root = ET.fromstring(content)
        placemarks = KMLPlacemark.from_kml(root, xpath)

        if KML_PARSING_VERBOSE:
            logger.info("Processed tree contains %d placemarks",
                        len(placemarks))

        for placemark in placemarks:
            if KML_PARSING_VERBOSE:
                logger.info("Parsed: %s", repr(placemark))

            subj = f"{self.primary_prefix}{next_id()}"
            subj_geo = f"{subj}_geo"

            yield (subj, TYPE, self.type_str)
            if self.parent:
                yield (subj, MEMBER, self.parent)
            if placemark.name:
                yield (subj, LABEL, add_datatype(placemark.name))
            if placemark.description:
                yield (subj, COMMENT, add_datatype(placemark.description))
            if placemark.placemark_id:
                yield (subj, IDENTIFIER, add_datatype(placemark.placemark_id))

            yield (subj, HAS_GEOMETRY, subj_geo)
            yield (subj_geo, AS_WKT, placemark.geometry.wkt_literal())

            if self.aux_geo_callback:
                self.aux_geo_callback(
                    subj,
                    placemark.geometry.wkt_literal(False)
                )


def parse_arguments(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    """
    Parse the program's command-line arguments
    """
    parser = argparse.ArgumentParser(
        description=PROGRAM_DESCRIPTION)

    # Required arguments
    parser.add_argument(
        '--input', '-i', nargs=1, type=str, required=True,
        help='input KML file (or KMZ file if used together with "--kmz")')
    parser.add_argument(
        '--dataset', '-d', nargs=1, type=str, required=True,
        help='dataset name for entity ids, must include only alphanumeric ' +
        'chars and underscores (e.g. "myplaces")')
    parser.add_argument(
        '--output', '-o', nargs=1, type=str, required=True,
        help='filename for compressed output turtle file ' +
        '(e.g. "myplaces.ttl.bz2")')
    parser.add_argument(
        '--prefix', '-p', nargs=1, type=str, required=True,
        help='prefix to be used for produced entities ' +
        '(short form, e.g. "ex:")')
    parser.add_argument(
        '--iri', '-r', nargs=1, type=str, required=True,
        help='prefix to be used for produced entities (full IRI form, ' +
        'e.g "http://example.com/schema")')

    # Optional arguments
    parser.add_argument(
        '--parent', '-e', nargs=1, type=str,
        help='optional IRI for "placemark rdfs:member parent" statements')
    parser.add_argument(
        '--aux-geo', '-x', nargs=1, type=str,
        help='optional filename for an aux-geo TSV that can be used with ' +
        'osm2rdf (contains two columns: entity RDF subject, WKT ' +
        'representation). note that the resulting ttl from osm2rdf may be ' +
        'missing the prefixes of your entities from kml2rdf - please add ' +
        'them to the osm2rdf output manually')
    parser.add_argument(
        '--kmz', '-z', action='store_true',
        help='file given as "--input" is a KMZ file (zipped KML): ' +
        'if this option is given, the file will be extracted next to ' +
        '"--input" with the same name and ".kml" extension.'
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None):
    """
    The procedure to be run when the program is called directly.
    """

    # Arguments
    args = parse_arguments(argv)
    logger.info("kml2rdf - arguments: %s", args)
    output: str = args.output[0]
    prefix: str = args.prefix[0]
    iri: str = args.iri[0]
    dataset: str = args.dataset[0]
    input_: str = args.input[0]
    parent: Optional[str] = None
    if args.parent:
        parent = args.parent[0]

    assert ":" in prefix
    PREFIXES.add(Prefix(prefix.split(":")[0], iri))

    # Extract .kmz files
    if args.kmz:
        logger.info("Extracting KMZ file '%s'", input_)
        if input_.endswith(".kml"):
            logger.warning("The filename ends .kml but is treated as .kmz")
        extracted = input_
        if extracted.endswith(".kmz"):
            extracted = extracted[:-4]
        extracted += ".kml"
        with ZipFile(input_, "r") as zf:
            kml_files = [fn for fn in zf.namelist() if fn.endswith(".kml")]
            assert len(kml_files) == 1, \
                "Valid KMZ must contain exactly one .kml file"
            with zf.open(kml_files[0], "r") as cf, open(extracted, "w") as ef:
                ef.write(cf.read().decode("utf-8"))
        logger.info("Extracted file written to '%s'", extracted)
        input_ = extracted
    elif input_.endswith(".kmz"):
        logger.warning("Input filename ends with .kmz, but option --kmz is " +
                       "not given. Treating as KML.")

    # Generate and emit triples
    with (open(args.aux_geo[0], "w") if args.aux_geo else nullcontext()) \
            as agf:
        aux_geo_count = 0

        def aux_geo_callback(_id: str, _wkt: str):
            nonlocal aux_geo_count
            if args.aux_geo:
                print(f"{_id}\t{_wkt}", file=agf)
                aux_geo_count += 1

        d = KMLDataset(dataset, "", input_,
                       prefix, parent, aux_geo_callback)
        count = d.to_file(str(output))
        logger.info("Emitted %d triples and %d aux geo entries",
                    count, aux_geo_count)


if __name__ == "__main__":
    main()
