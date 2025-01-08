#!/bin/env python3
"""
Unit tests for dataset to rdf conversion programs (dataset.py, csv2rdf.py,
kml2rdf.py, gtfs2rdf.py, election2rdf.py)

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

import bz2
from dataclasses import dataclass
from typing import Iterator, Optional, Any
import unittest
from unittest.mock import patch
from dataset import PREFIXES, Dataset, Prefix, Triple, all_prefixes, \
    add_datatype, next_id, TYPE, triple
from kml2rdf import KMLDataset, KMLPlacemark, KMLXPathHelper, \
    Geometry, Point, LineString, Polygon, GeometryCollection, \
    main as kml2rdf_main
from csv2rdf import CSVDataset, main as csv2rdf_main, \
    set_warn_missing_col_mapping
from gtfs2rdf import GTFSFeed, main as gtfs2rdf_main
from election2rdf import Election, main as election2rdf_main
from abc import ABC
from tempfile import TemporaryDirectory
import xml.etree.ElementTree as ET
from pathlib import Path
import os
import shutil
import json
import re

# Change working directory to this test program's directory
# This simplifies tests for reading/writing files
prog_dir = str(Path(__file__).parent.resolve())
os.chdir(prog_dir)


class TestUtils(unittest.TestCase):
    def test_add_datatype(self):
        self.assertEqual(add_datatype(""), '""')
        self.assertEqual(add_datatype("abc"), '"abc"')
        self.assertEqual(add_datatype("12b"), '"12b"')
        self.assertEqual(add_datatype("1234"), '"1234"^^xsd:integer')
        self.assertEqual(add_datatype("0"), '"0"^^xsd:integer')
        self.assertEqual(add_datatype("-0.42"), '"-0.42"^^xsd:decimal')
        self.assertEqual(add_datatype("03.09.1989"), '"1989/09/03"^^xsd:date')
        self.assertEqual(add_datatype("2024/09/25"), '"2024/09/25"^^xsd:date')
        self.assertEqual(add_datatype("2024-11-17T18:19:50Z"),
                         '"2024-11-17T18:19:50Z"^^xsd:dateTime')
        self.assertEqual(add_datatype("2024-11-17T18:19:50.999Z"),
                         '"2024-11-17T18:19:50.999Z"^^xsd:dateTime')
        self.assertEqual(add_datatype("POINT(7.9 49.7)"),
                         '"POINT(7.9 49.7)"^^geo:wktLiteral')
        self.assertEqual(add_datatype(None), '""')  # type: ignore

    def test_prefix(self):
        p = Prefix("abc", "http://example.com/abc#")
        p2 = Prefix("abc", "http://example.com/xyz#")
        p3 = Prefix("xyz", "http://example.com/xyz#")
        p4 = Prefix("xyz", "http://example.com/xyz#")

        p_expect = "@prefix abc: <http://example.com/abc#> .\n"
        p2_expect = "@prefix abc: <http://example.com/xyz#> .\n"
        p3_expect = "@prefix xyz: <http://example.com/xyz#> .\n"

        self.assertEqual(str(p), p_expect)
        self.assertEqual(str(p2), p2_expect)
        self.assertEqual(p3, p4)
        self.assertNotEqual(p, p2)

        self.assertEqual(len(set((p, p2, p3))), 3)

        with self.assertRaises(AssertionError):
            Prefix("", "")

        with self.assertRaises(AssertionError):
            Prefix("abc:", "http://example.com/")

        with self.assertRaises(AssertionError):
            Prefix("abc", "http://example.com")

        with self.assertRaises(AssertionError):
            Prefix("abc", "http://")

        with self.assertRaises(AssertionError):
            Prefix("abc", "ftp://example.com/")

        with self.assertRaises(AssertionError):
            Prefix("abc", "**!!%$")

        # Same prefix twice but with different IRI is not allowed
        PREFIXES.add(p)
        PREFIXES.add(p2)
        with self.assertRaises(AssertionError):
            list(all_prefixes())
        PREFIXES.remove(p2)

        PREFIXES.add(p3)
        ap = list(all_prefixes())
        self.assertIn(p_expect, ap)
        self.assertIn(p3_expect, ap)

        # Must be sorted for deterministic output
        self.assertLess(ap.index(p_expect), ap.index(p3_expect))

    def test_next_id(self):
        i = int(next_id())
        for inc in range(1, 10):
            n = next_id()
            self.assertEqual(n, str(i + inc))

    def test_triple(self):
        self.assertEqual(triple(("a", "b", "c")), "a b c .\n")


class TestDataset(unittest.TestCase):
    @patch("dataset.logger.info", lambda *_: None)
    def test_dataset(self):
        # Dataset is abstract
        self.assertTrue(issubclass(Dataset, ABC))

        @Dataset.register
        @dataclass
        class DummyDataset(Dataset):
            def rdf(self) -> Iterator[Triple]:
                yield ("dummy", "triple", "abstract")

        # Missing / invalid dataset
        with self.assertRaises(AssertionError):
            DummyDataset(_dataset="", _command="echo 'Hello World'",
                         _store_filename="xyz", _primary_prefix="rdf:",
                         parent="test")
        with self.assertRaises(AssertionError):
            DummyDataset(_dataset="abc#*!", _command="echo 'Hello World'",
                         _store_filename="xyz", _primary_prefix="rdf:",
                         parent="test")

        # Missing / invalid primary prefix
        with self.assertRaises(AssertionError):
            DummyDataset(_dataset="abc", _command="echo 'Hello World'",
                         _store_filename="xyz", _primary_prefix="r",
                         parent="test")
        with self.assertRaises(AssertionError):
            DummyDataset(_dataset="abc", _command="echo 'Hello World'",
                         _store_filename="xyz", _primary_prefix="",
                         parent="test")

        # No filename
        with self.assertRaises(AssertionError):
            DummyDataset(_dataset="abc", _command="echo 'Hello World'",
                         _store_filename="", _primary_prefix="rdf:",
                         parent="test")

        # get_data: non existing file, empty _command
        Path("test/hello.txt").unlink(True)
        with self.assertRaises(AssertionError):
            d = DummyDataset(_dataset="abc", _command="",
                             _store_filename="test/hello.txt",
                             _primary_prefix="rdf:", parent="test")
            d.get_data()

        # get_data: existing file, empty _command
        d = DummyDataset(_dataset="abc", _command="",
                         _store_filename="test/hello2.txt",
                         _primary_prefix="rdf:", parent="test")
        d.get_data()
        self.assertEqual(list(d.content()), ["Hello World"])
        d = DummyDataset(_dataset="abc", _command=None,
                         _store_filename="test/hello2.txt",
                         _primary_prefix="rdf:", parent="test")
        d.get_data()
        self.assertEqual(list(d.content()), ["Hello World"])

        # get_data: non-existing file, non-empty _command
        Path("test/hello3.txt").unlink(True)
        d = DummyDataset(_dataset="abc", _command="echo 'Hello World'",
                         _store_filename="test/hello3.txt",
                         _primary_prefix="rdf:", parent="test")
        d.get_data()
        self.assertEqual(list(d.content()), ["Hello World\n"])

        # clean_prefix and type_str
        d = DummyDataset(_dataset="abc", _command="echo 'Hello World'",
                         _store_filename="test/hello3.txt",
                         _primary_prefix="rdf:abc_def", parent="test")
        self.assertEqual(d.clean_prefix, "rdf:")
        self.assertEqual(d.type_str, "rdf:abc")
        d = DummyDataset(_dataset="abc", _command="echo 'Hello World'",
                         _store_filename="test/hello3.txt",
                         _primary_prefix="test:", parent="test")
        self.assertEqual(d.clean_prefix, "test:")
        self.assertEqual(d.type_str, "test:abc")

        # to_file
        d = DummyDataset(_dataset="abc", _command="echo 'Hello World'",
                         _store_filename="test/hello3.txt",
                         _primary_prefix="rdf:abc_def", parent="test")
        d.to_file("test/hello3.ttl.bz2")
        output = ""
        with bz2.open("test/hello3.ttl.bz2") as f:
            output = f.read().decode("utf-8")
        lines = output.splitlines()
        self.assertEqual([
            line for line in lines if not line.startswith("@prefix")
        ], ["dummy triple abstract ."])

        # Filename warning
        with self.assertLogs('dataset', level='WARN') as log:
            d.to_file("test/hello3.ttl.xyz")
            expect_warn = "WARNING:dataset:Output filename is expected " + \
                "to end with '.ttl.bz2'"
            self.assertEqual(log.output, [expect_warn])

    @patch("dataset.logger.info", lambda *_: None)
    def test_csv_dataset(self):
        # test general features and col mapping
        i = next_id()
        i1, i2 = str(int(i) + 1), str(int(i) + 2)
        c = CSVDataset(
            _dataset="xyz",
            _command="cat test/basic.csv",
            _store_filename="test/basic.csv",
            _primary_prefix="election:id_prefix_xyz",
            parent="parent",
            csv_separator=",",
            csv_quote="\"",
            column_mapping={
                TYPE: "_a",
                "b": None
            },
            primary_col="",
            values_mapping={},
            extra_triple_callback=None
        )
        c.get_data()
        subj1 = f"election:id_prefix_xyz_{i1}"
        subj2 = f"election:id_prefix_xyz_{i2}"
        self.assertListEqual(list(c.rdf()), [
            (subj1, "a", "election:xyz"),
            (subj1, "rdfs:member", "parent"),
            (subj1, "election:_a", "\"1\"^^xsd:integer"),
            (subj1, "election:c", "\"l\""),
            (subj2, "a", "election:xyz"),
            (subj2, "rdfs:member", "parent"),
            (subj2, "election:_a", "\"-4.5\"^^xsd:decimal"),
            (subj2, "election:c", "\"2024/01/01\"^^xsd:date")
        ])

        # test csv separator, csv quote, primary_col, values mapping,
        # extra triple callback and parent=None
        def et_cb(cc: CSVDataset, subj: str, row: dict[str, str]) \
                -> Iterator[Triple]:
            yield (subj, "test", row["a"])
            yield ("example", "test", subj)
            yield ("example", "test", cc.clean_prefix + "x")
        c2 = CSVDataset(
            _dataset="separator",
            _command="cat test/separator.csv",
            _store_filename="test/separator.csv",
            _primary_prefix="test:abc",
            parent=None,
            csv_separator="|",
            csv_quote="*",
            column_mapping={},
            primary_col="a",
            values_mapping={
                "c": ([
                    ("3", " "),
                    ("(?P<test>\\W+)$", "\\g<test>\\g<test>")
                ], True),
                "d": ([("^0*", "")], True),
                "e": ([("^0*", "")], False)
            },
            extra_triple_callback=et_cb
        )
        c2.get_data()
        out_c2 = list(c2.rdf())
        self.assertListEqual(out_c2, [
            ("test:abc1", "a", "test:separator"),
            ("test:abc1", "test:a", "\"1\"^^xsd:integer"),
            ("test:abc1", "test:b", "\"2|2\""),
            ("test:abc1", "test:c", "\"hello world # #\""),
            ("test:abc1", "test:d", "\"5.20\"^^xsd:decimal"),
            ("test:abc1", "test:e", "5"),
            ("test:abc1", "test", "1"),
            ("example", "test", "test:abc1"),
            ("example", "test", "test:x"),
        ])

        set_warn_missing_col_mapping(True)
        with self.assertLogs('csv2rdf', level='WARN') as log:
            out_c2_wcolm = list(c2.rdf())
            self.assertListEqual(out_c2, out_c2_wcolm)
            self.assertCountEqual(log.output, [
                'WARNING:csv2rdf:Dataset separator. Missing column mapping ' +
                f'for {col}' for col in ('a', 'b', 'c', 'd', 'e')])
        set_warn_missing_col_mapping(False)

    @patch("dataset.logger.info", lambda *_: None)
    def test_kml_dataset(self):
        i = next_id()
        i1 = str(int(i) + 1)

        aux_geo: list[tuple[str, str]] = []

        def aux_geo_cb(subj: str, wkt: str):
            aux_geo.append((subj, wkt))

        k = KMLDataset(
            _dataset="k",
            _command="cat test/test.kml",
            _store_filename="test/test.kml",
            _primary_prefix="wkgeo:id_prefix_",
            parent="parent",
            aux_geo_callback=aux_geo_cb
        )

        k.get_data()
        subj1 = f"wkgeo:id_prefix_{i1}"
        self.assertListEqual(list(k.rdf()), [
            (subj1, "a", "wkgeo:k"),
            (subj1, "rdfs:member", "parent"),
            (subj1, "rdfs:label", "\"example 1.1\""),
            (subj1, 'rdfs:comment',
             '"A multi polygon in kml"'),
            (subj1, 'dct:identifier', '"example1_1"'),
            (subj1, "geo:hasGeometry",
             f"{subj1}_geo"),
            (f"{subj1}_geo", "geo:asWKT", "\"MULTIPOLYGON((" +
             "(1.0 1.5, 2.0 2.5, 3.0 4.0, 3.0 1.5), " +
             "(2.0 2.25, 2.5 2.25, 2.5 2.5)), " +
             "((11.0 11.5, 12.0 12.5, 13.0 14.0, 13.0 11.5), " +
             "(12.0 12.25, 12.5 12.25, 12.5 12.5)))\"^^geo:wktLiteral")
        ])
        self.assertListEqual(aux_geo, [
            (subj1, "MULTIPOLYGON((" +
             "(1.0 1.5, 2.0 2.5, 3.0 4.0, 3.0 1.5), " +
             "(2.0 2.25, 2.5 2.25, 2.5 2.5)), " +
             "((11.0 11.5, 12.0 12.5, 13.0 14.0, 13.0 11.5), " +
             "(12.0 12.25, 12.5 12.25, 12.5 12.5)))")
        ])


class TestGeometry(unittest.TestCase):
    EXAMPLE_KML_POINT = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document id="example">
            <Folder id="example1">
                <Placemark id="example1_1">
                    <name>example 1.1</name>
                    <description>A simple point in kml</description>
                    <Point>
                        <coordinates>
                            30.5,60.5,0
                        </coordinates>
                    </Point>
                </Placemark>
            </Folder>
        </Document>
    </kml>
    """

    EXAMPLE_KML_MULTIPOINT = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document id="example">
            <Folder id="example1">
                <Placemark id="example1_1">
                    <name>example 1.1</name>
                    <description>A multi point in kml</description>
                    <MultiGeometry>
                        <Point>
                            <coordinates>
                                30.5,60.5,0
                            </coordinates>
                        </Point>
                        <Point>
                            <coordinates>
                                31.5,61.5,0
                            </coordinates>
                        </Point>
                    </MultiGeometry>
                </Placemark>
            </Folder>
        </Document>
    </kml>
    """

    EXAMPLE_KML_LINESTRING = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document id="example">
            <Folder id="example1">
                <Placemark id="example1_1">
                    <name>example 1.1</name>
                    <description>A simple line string in kml</description>
                    <LineString>
                        <coordinates>
                            1.0,1.5,0 2.0,2.5,0
                            3.0,4.0,0
                        </coordinates>
                    </LineString>
                </Placemark>
            </Folder>
        </Document>
    </kml>
    """

    EXAMPLE_KML_MULTILINESTRING = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document id="example">
            <Folder id="example1">
                <Placemark id="example1_1">
                    <name>example 1.1</name>
                    <description>A multi line string in kml</description>
                    <MultiGeometry>
                        <LineString>
                            <coordinates>
                                1.0,1.5,0 2.0,2.5,0
                                3.0,4.0,0
                            </coordinates>
                        </LineString>
                        <LineString>
                            <coordinates>
                                10.0,10.5,0 20.0,20.5,0
                                30.0,40.0,0
                            </coordinates>
                        </LineString>
                    </MultiGeometry>
                </Placemark>
            </Folder>
        </Document>
    </kml>
    """

    EXAMPLE_KML_GX_TRACK = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2"
         xmlns:gx="http://www.google.com/kml/ext/2.2">
        <Document id="example">
            <Folder id="example1">
                <Placemark id="example1_1">
                    <name>example 1.1</name>
                    <description>A simple line string in kml</description>
                    <gx:Track>
                        <gx:coord>1.0 1.5 0</gx:coord>
                        <gx:coord>2.0 2.5 0</gx:coord>
                        <gx:coord>3.0 4.0 0</gx:coord>
                    </gx:Track>
                </Placemark>
            </Folder>
        </Document>
    </kml>
    """

    EXAMPLE_KML_POLYGON = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document id="example">
            <Folder id="example1">
                <Placemark id="example1_1">
                    <name>example 1.1</name>
                    <description>A simple polygon in kml</description>
                    <Polygon>
                        <outerBoundaryIs>
                            <LinearRing>
                                <coordinates>
                                    1.0,1.5,0 2.0,2.5,0
                                    3.0,4.0,0 3.0,1.5,0
                                </coordinates>
                            </LinearRing>
                        </outerBoundaryIs>
                    </Polygon>
                </Placemark>
            </Folder>
        </Document>
    </kml>
    """

    EXAMPLE_KML_INVALID_POLYGON = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document id="example">
            <Folder id="example1">
                <Placemark id="example1_1">
                    <name>example 1.1</name>
                    <description>A simple polygon in kml</description>
                    <Polygon></Polygon>
                </Placemark>
            </Folder>
        </Document>
    </kml>
    """

    EXAMPLE_KML_POLYGON_HOLE = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document id="example">
            <Folder id="example1">
                <Placemark id="example1_1">
                    <name>example 1.1</name>
                    <description>A polygon with hole in kml</description>
                    <Polygon>
                        <outerBoundaryIs>
                            <LinearRing>
                                <coordinates>
                                    1.0,1.5,0 2.0,2.5,0
                                    3.0,4.0,0 3.0,1.5,0
                                </coordinates>
                            </LinearRing>
                        </outerBoundaryIs>
                        <innerBoundaryIs>
                            <LinearRing>
                                <coordinates>
                                    2.0,2.25,0 2.5,2.25,0 2.5,2.5,0
                                </coordinates>
                            </LinearRing>
                        </innerBoundaryIs>
                    </Polygon>
                </Placemark>
            </Folder>
        </Document>
    </kml>
    """

    EXAMPLE_KML_MULTIPOLYGON = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document id="example">
            <Folder id="example1">
                <Placemark id="example1_1">
                    <name>example 1.1</name>
                    <description>A multi polygon in kml</description>
                    <MultiGeometry>
                        <Polygon>
                            <outerBoundaryIs>
                                <LinearRing>
                                    <coordinates>
                                        1.0,1.5,0 2.0,2.5,0
                                        3.0,4.0,0 3.0,1.5,0
                                    </coordinates>
                                </LinearRing>
                            </outerBoundaryIs>
                            <innerBoundaryIs>
                                <LinearRing>
                                    <coordinates>
                                        2.0,2.25,0 2.5,2.25,0 2.5,2.5,0
                                    </coordinates>
                                </LinearRing>
                            </innerBoundaryIs>
                        </Polygon>
                        <Polygon>
                            <outerBoundaryIs>
                                <LinearRing>
                                    <coordinates>
                                        11.0,11.5,0 12.0,12.5,0
                                        13.0,14.0,0 13.0,11.5,0
                                    </coordinates>
                                </LinearRing>
                            </outerBoundaryIs>
                            <innerBoundaryIs>
                                <LinearRing>
                                    <coordinates>
                                        12.0,12.25,0 12.5,12.25,0 12.5,12.5,0
                                    </coordinates>
                                </LinearRing>
                            </innerBoundaryIs>
                        </Polygon>
                    </MultiGeometry>
                </Placemark>
            </Folder>
        </Document>
    </kml>
    """

    EXAMPLE_KML_GEOMETRYCOLLECTION = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document id="example">
            <Folder id="example1">
                <Placemark id="example1_1">
                    <name>example 1.1</name>
                    <description>A geometry collection in kml</description>
                    <MultiGeometry>
                        <Polygon>
                            <outerBoundaryIs>
                                <LinearRing>
                                    <coordinates>
                                        1.0,1.5,0 2.0,2.5,0
                                        3.0,4.0,0 3.0,1.5,0
                                    </coordinates>
                                </LinearRing>
                            </outerBoundaryIs>
                            <innerBoundaryIs>
                                <LinearRing>
                                    <coordinates>
                                        2.0,2.25,0 2.5,2.25,0 2.5,2.5,0
                                    </coordinates>
                                </LinearRing>
                            </innerBoundaryIs>
                        </Polygon>
                        <Point>
                            <coordinates>
                                30.5,60.5,0
                            </coordinates>
                        </Point>
                        <Point>
                            <coordinates>
                                10.5,10.5,0
                            </coordinates>
                        </Point>
                        <LineString>
                            <coordinates>
                                10.0,10.5,0 20.0,20.5,0
                                30.0,40.0,0
                            </coordinates>
                        </LineString>
                    </MultiGeometry>
                </Placemark>
            </Folder>
        </Document>
    </kml>
    """

    EXAMPLE_KML_PLACEMARKS = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document id="example">
            <Folder id="example1">
                <Placemark id="example1_1">
                    <name>example 1.1</name>
                    <description>A simple poylgon in kml</description>
                    <Polygon>
                        <outerBoundaryIs>
                            <LinearRing>
                                <coordinates>
                                    1.0,1.5,0 2.0,2.5,0
                                    3.0,4.0,0 3.0,1.5,0
                                </coordinates>
                            </LinearRing>
                        </outerBoundaryIs>
                        <innerBoundaryIs>
                            <LinearRing>
                                <coordinates>
                                    2.0,2.25,0 2.5,2.25,0 2.5,2.5,0
                                </coordinates>
                            </LinearRing>
                        </innerBoundaryIs>
                    </Polygon>
                </Placemark>
                <Placemark id="example1_2">
                    <name>example 1.2</name>
                    <description>A simple point in kml</description>
                    <Point>
                        <coordinates>
                            30.5,60.5,0
                        </coordinates>
                    </Point>
                </Placemark>
            </Folder>
        </Document>
    </kml>
    """

    def from_kml_helper(self, kml: str) -> Geometry:
        xpath = KMLXPathHelper()
        placemarks = KMLPlacemark.from_kml(
            ET.fromstring(kml), xpath)
        self.assertEqual(len(placemarks), 1)
        placemark = placemarks[0]
        self.assertEqual(placemark.placemark_id, "example1_1")
        self.assertEqual(placemark.name, "example 1.1")
        self.assertIsNotNone(placemark.description)
        self.assertRegex(
            placemark.description or "", r'^A [\w ]* in kml$')
        self.assertIsInstance(placemark.geometry, Geometry)
        return placemark.geometry

    def test_point(self):
        p1 = Point(17.0, 99.5)
        self.assertEqual(p1.lat, 17.0)
        self.assertEqual(p1.lng, 99.5)
        p2 = Point(0, 0)
        self.assertEqual(p2.lat, 0)
        self.assertEqual(p2.lng, 0)
        p3 = Point(lng=1, lat=2)
        self.assertEqual(p3.lat, 2)
        self.assertEqual(p3.lng, 1)

        with self.assertRaises(AssertionError):
            Point(99.0, 50.123)
        with self.assertRaises(AssertionError):
            Point(-99.0, 50.123)
        with self.assertRaises(AssertionError):
            Point(5.0, 250.123)
        with self.assertRaises(AssertionError):
            Point(5.0, -250.123)

        p4 = self.from_kml_helper(self.EXAMPLE_KML_POINT)

        self.assertIsNotNone(p4)
        if p4 is not None:  # For type checker (covered by assert above)
            self.assertEqual(p4, Point(lat=60.5, lng=30.5))

        p5 = Point.from_kml_coords("30.5,60.5,0")
        self.assertEqual(p5.lat, 60.5)
        self.assertEqual(p5.lng, 30.5)

        p6 = Point.from_kml_coords("99.5,-90.0,0")
        self.assertEqual(p6.lat, -90.0)
        self.assertEqual(p6.lng, 99.5)

        self.assertEqual(p6.to_wkt(), "POINT(99.5 -90.0)")
        self.assertEqual(p6.to_wkt(True), "POINT(99.5 -90.0)")
        self.assertEqual(p6.to_wkt(False), "(99.5 -90.0)")

        self.assertEqual(p6.wkt_type, "POINT")

        # Test Geometry.wkt_literal
        self.assertEqual(p6.wkt_literal(),
                         "\"POINT(99.5 -90.0)\"^^geo:wktLiteral")
        self.assertEqual(p6.wkt_literal(True),
                         "\"POINT(99.5 -90.0)\"^^geo:wktLiteral")
        self.assertEqual(p6.wkt_literal(False), "POINT(99.5 -90.0)")

    def test_line_string(self):
        l1 = LineString([Point(1.5, 1.0), Point(2.5, 2.0), Point(4.0, 3.0)])
        l2 = self.from_kml_helper(self.EXAMPLE_KML_LINESTRING)
        self.assertEqual(l1, l2)
        l3 = LineString.from_kml_coords(
            "1.0,1.5,5890 2.0,2.5,1230 3.0,4.0,5670")
        self.assertEqual(l1, l3)
        l4 = self.from_kml_helper(self.EXAMPLE_KML_GX_TRACK)
        self.assertEqual(l1, l4)
        self.assertEqual(l1.to_wkt(), "LINESTRING(1.0 1.5, 2.0 2.5, 3.0 4.0)")
        self.assertEqual(
            l1.to_wkt(True), "LINESTRING(1.0 1.5, 2.0 2.5, 3.0 4.0)")
        self.assertEqual(l1.to_wkt(False),
                         "(1.0 1.5, 2.0 2.5, 3.0 4.0)")

    def test_polygon(self):
        p1 = self.from_kml_helper(self.EXAMPLE_KML_POLYGON)

        # Polygon without hole
        l1 = LineString.from_kml_coords(
            "1.0,1.5,0 2.0,2.5,0  3.0,4.0,0 3.0,1.5,0")
        self.assertIsNotNone(l1)
        if l1 is not None:  # For type checker
            self.assertEqual(p1, Polygon(l1, []))

        # Polygon with hole
        p2 = self.from_kml_helper(self.EXAMPLE_KML_POLYGON_HOLE)
        l2 = LineString.from_kml_coords("2.0,2.25,0 2.5,2.25,0 2.5,2.5,0")
        self.assertIsNotNone(l2)
        if l1 is not None and l2 is not None:  # For type checker
            self.assertEqual(p2, Polygon(l1, [l2]))

        # Invalid empty polygon
        self.assertEqual(len(KMLPlacemark.from_kml(
            ET.fromstring(self.EXAMPLE_KML_INVALID_POLYGON),
            KMLXPathHelper())), 0)

    def test_geometry_collection(self):
        # One Element
        p1 = Polygon(LineString(
            [Point(lng=1.0, lat=2.5), Point(lng=3.6, lat=4.7),
             Point(lng=5.5, lat=5.9)]), [])
        a = GeometryCollection([p1])
        self.assertEqual(a.wkt_literal(),
                         '"MULTIPOLYGON(((1.0 2.5, 3.6 4.7, 5.5 5.9)))"' +
                         '^^geo:wktLiteral')

        # Two homogeneous elements
        p2 = Polygon(
            LineString([
                Point(lng=7.5, lat=1.5), Point(lng=3.1, lat=1.2),
                Point(lng=4.2, lat=3.7), Point(lng=3.5, lat=35.4)
            ]),
            [
                LineString([
                    Point(lng=2.1, lat=2.1), Point(lng=2.2, lat=2.2),
                    Point(lng=2.3, lat=2.3)
                ])
            ]
        )
        a.members.append(p2)
        self.assertEqual(a.wkt_literal(),
                         '"MULTIPOLYGON(((1.0 2.5, 3.6 4.7, 5.5 5.9)), ' +
                         '((7.5 1.5, 3.1 1.2, 4.2 3.7, 3.5 35.4), ' +
                         '(2.1 2.1, 2.2 2.2, 2.3 2.3)))"' +
                         '^^geo:wktLiteral')
        self.assertEqual(a.wkt_literal(False),
                         'MULTIPOLYGON(((1.0 2.5, 3.6 4.7, 5.5 5.9)), ' +
                         '((7.5 1.5, 3.1 1.2, 4.2 3.7, 3.5 35.4), ' +
                         '(2.1 2.1, 2.2 2.2, 2.3 2.3)))')

        points: list[Polygon | LineString | Point] = [
            Point(lng=12.0, lat=13.0), Point(lng=22.0, lat=23.0)
        ]
        self.assertEqual(GeometryCollection(points).to_wkt(),
                         "MULTIPOINT((12.0 13.0), (22.0 23.0))")

        lines: list[Polygon | LineString | Point] = [
            LineString([Point(lng=12.0, lat=13.0), Point(lng=22.0, lat=23.0)]),
            LineString([Point(lng=1.0, lat=2.0), Point(lng=1.0, lat=3.0)])
        ]
        self.assertEqual(
            GeometryCollection(lines).to_wkt(),
            "MULTILINESTRING((12.0 13.0, 22.0 23.0), (1.0 2.0, 1.0 3.0))"
        )

        # Heterogen elements
        items: list[Polygon | LineString | Point] = [p1, points[0], lines[0]]
        self.assertEqual(
            GeometryCollection(items).to_wkt(),
            "GEOMETRYCOLLECTION(POLYGON((1.0 2.5, 3.6 4.7, 5.5 5.9)), " +
            "POINT(12.0 13.0), LINESTRING(12.0 13.0, 22.0 23.0))")

        # Geometry collection from KML
        point1 = Point(lng=30.5, lat=60.5)
        point2 = Point(lng=31.5, lat=61.5)
        multipoint = self.from_kml_helper(self.EXAMPLE_KML_MULTIPOINT)
        self.assertEqual(multipoint, GeometryCollection([point1, point2]))
        self.assertEqual(multipoint.to_wkt(),
                         "MULTIPOINT((30.5 60.5), (31.5 61.5))")

        line1 = LineString([Point(lng=1.0, lat=1.5), Point(lng=2.0, lat=2.5),
                            Point(lng=3.0, lat=4.0)])
        line2 = LineString([Point(lng=10.0, lat=10.5),
                            Point(lng=20.0, lat=20.5),
                            Point(lng=30.0, lat=40.0)])
        multiline = self.from_kml_helper(self.EXAMPLE_KML_MULTILINESTRING)
        self.assertEqual(multiline, GeometryCollection([line1, line2]))
        self.assertEqual(multiline.to_wkt(),
                         "MULTILINESTRING((1.0 1.5, 2.0 2.5, 3.0 4.0), " +
                         "(10.0 10.5, 20.0 20.5, 30.0 40.0))")

        line3 = LineString([Point(lng=1.0, lat=1.5), Point(lng=2.0, lat=2.5),
                            Point(lng=3.0, lat=4.0), Point(lng=3.0, lat=1.5)])
        line4 = LineString([Point(lng=2.0, lat=2.25), Point(lng=2.5, lat=2.25),
                            Point(lng=2.5, lat=2.5)])
        polygon1 = Polygon(line3, [line4])
        line5 = LineString([Point(lng=11.0, lat=11.5),
                            Point(lng=12.0, lat=12.5),
                            Point(lng=13.0, lat=14.0),
                            Point(lng=13.0, lat=11.5)])
        line6 = LineString([Point(lng=12.0, lat=12.25),
                            Point(lng=12.5, lat=12.25),
                            Point(lng=12.5, lat=12.5)])
        polygon2 = Polygon(line5, [line6])
        multipoly = self.from_kml_helper(self.EXAMPLE_KML_MULTIPOLYGON)
        self.assertEqual(multipoly, GeometryCollection([polygon1, polygon2]))
        if isinstance(multipoly, GeometryCollection):  # Type checker
            self.assertTrue(multipoly.homogeneous())
        self.assertEqual(multipoly.to_wkt(),
                         "MULTIPOLYGON((" +
                         "(1.0 1.5, 2.0 2.5, 3.0 4.0, 3.0 1.5), " +
                         "(2.0 2.25, 2.5 2.25, 2.5 2.5)), " +
                         "((11.0 11.5, 12.0 12.5, 13.0 14.0, 13.0 11.5), " +
                         "(12.0 12.25, 12.5 12.25, 12.5 12.5)))")

        point3 = Point(lat=10.5, lng=10.5)
        geocollect = self.from_kml_helper(self.EXAMPLE_KML_GEOMETRYCOLLECTION)
        self.assertIsInstance(geocollect, GeometryCollection)
        if isinstance(geocollect, GeometryCollection):  # Type checker
            self.assertFalse(geocollect.homogeneous())
            exp_members: list[Polygon | LineString | Point] = [
                polygon1, point1, point3, line2]
            # This test method's name is weird: from docs:
            # `assertCountEqual(a, b)` a and b have the same elements in the
            # same number, regardless of their order.
            self.assertCountEqual(geocollect.members, exp_members)
            wkt = geocollect.to_wkt()
            exp_wkt = [g.to_wkt()
                       for g in GeometryCollection(exp_members).members]
            # Remove each expected WKT exactly once (because the order is not
            # defined). If every expected member is contained, we get the
            # string below.
            for a in exp_wkt:
                wkt = wkt.replace(a, "X", 1)
            self.assertEqual(wkt, "GEOMETRYCOLLECTION(X, X, X, X)")

    def test_kml_placemark(self):
        xpath = KMLXPathHelper()
        root = ET.fromstring(self.EXAMPLE_KML_PLACEMARKS)
        placemarks = KMLPlacemark.from_kml(root, xpath)
        self.assertEqual(len(placemarks), 2)

        p_poly, p_point = placemarks
        expect_poly = KMLPlacemark(
            Polygon(
                LineString([
                    Point(1.5, 1.0),
                    Point(2.5, 2.0),
                    Point(4.0, 3.0),
                    Point(1.5, 3.0)
                ]),
                [
                    LineString([
                        Point(2.25, 2.0),
                        Point(2.25, 2.5),
                        Point(2.5, 2.5)
                    ])
                ]
            ),
            "example1_1",
            "example 1.1",
            "A simple poylgon in kml"
        )
        self.assertEqual(p_poly, expect_poly)

        expect_point = KMLPlacemark(
            Point(60.5, 30.5),
            "example1_2",
            "example 1.2",
            "A simple point in kml"
        )
        self.assertEqual(p_point, expect_point)

    def test_kml_xpath_helper(self):
        ns1 = "http://www.opengis.net/kml/2.2"
        ns2 = "http://www.opengis.net/kml/2.1"
        ns3 = "http://earth.google.com/kml/2.2"

        example1 = self.EXAMPLE_KML_PLACEMARKS
        self.assertIn(ns1, example1)
        example2 = example1.replace(ns1, ns2)
        example3 = example1.replace(ns1, ns3)

        self.assertEqual(KMLXPathHelper(ns1), KMLXPathHelper())
        self.assertEqual(
            KMLXPathHelper.from_kml_source(example1), KMLXPathHelper())
        self.assertEqual(
            KMLXPathHelper.from_kml_source(example2), KMLXPathHelper(ns2))
        self.assertEqual(
            KMLXPathHelper.from_kml_source(example3), KMLXPathHelper(ns3))

        # XPaths
        root = ET.fromstring(example3)
        xpath = KMLXPathHelper.from_kml_source(example3)

        places = root.findall(xpath.KML_PLACEMARK_XPATH)
        self.assertEqual(len(places), 2)
        for p in places:
            self.assertEqual(
                len(p.findall(xpath.KML_NAME_XPATH)), 1)
            self.assertEqual(
                len(p.findall(xpath.KML_DESCRIPTION_XPATH)), 1)
            self.assertEqual(
                len(p.findall(xpath.KML_LINE_XPATH)), 0)

        p0, p1 = places
        self.assertEqual(
            len(p1.findall(xpath.KML_POINT_XPATH)), 1)
        self.assertEqual(
            len(p0.findall(xpath.KML_POLYGON_XPATH)), 1)
        poly = p0.find(xpath.KML_POLYGON_XPATH)
        self.assertIsNotNone(poly)
        if poly is not None:
            outer = poly.findall(xpath.KML_POLYGON_OUTER_XPATH)
            self.assertEqual(len(outer), 1)
            inner = poly.findall(xpath.KML_POLYGON_INNER_XPATH)
            self.assertEqual(len(inner), 1)


class TestGTFS(unittest.TestCase):
    def make_gtfs_and_get_result(self, feed: str) -> str:
        "Helper function to run gtfs2rdf"

        # Clear old extracted files
        files = ["agency.txt",
                 "calendar.txt",
                 "calendar_dates.txt",
                 "fare_attributes.txt",
                 "fare_rules.txt",
                 "frequencies.txt",
                 "feed_info.txt",
                 "routes.txt",
                 "shapes.txt",
                 "stop_times.txt",
                 "stops.txt",
                 "trips.txt",
                 "transfers.txt"]
        for f in files:
            Path(f).unlink(missing_ok=True)

        # Make GTFS feed object and extract files
        d = GTFSFeed(_feed=re.sub(r'\W', "", feed),
                     _filename=feed + ".zip", _excludes=[],
                     _add_linestrings=True)
        d.get_all_data()

        # Test output
        test_fn = "gtfs-sample.ttl"
        with open(test_fn, "w") as f:
            d.to_file(f)
        result = ""
        with open(test_fn, "r") as f:
            result = f.read()

        # Clean up
        for f in files:
            Path(f).unlink(missing_ok=True)

        return result

    @patch("gtfs2rdf.logger.info", lambda *_: None)
    def setUp(self) -> None:
        os.chdir(Path(prog_dir, "test"))

        result = self.make_gtfs_and_get_result("minimal-gtfs")
        self.all_triples = {
            tuple(line[:-2].split(" ", 2))
            for line in result.splitlines()
            if line.strip() and line.endswith(" .")
        }

        self.prefix_re = r'^(?P<prefix>\w+):\w+$'
        self.prefixes = {p[:-1]: o
                         for s, p, o in self.all_triples if s == "@prefix"}
        self.triples = {(s, p, o)
                        for s, p, o in self.all_triples if s != "@prefix"}

        self.trip = 'gtfs:trip_minimalgtfs_2508971872'
        self.route = 'gtfs:route_minimalgtfs_3108738_106'
        self.agency = 'gtfs:agency_minimalgtfs_10443'
        self.shape = 'gtfs:shape_minimalgtfs_97560'
        self.stop1 = 'gtfs:stop_minimalgtfs_de0831690112'
        self.stop2 = 'gtfs:stop_minimalgtfs_de08311650864'

        return super().setUp()

    def tearDown(self):
        os.chdir(Path(prog_dir))

    def exists_s_for_p_o(self, p: str, o: str) -> set[str]:
        return {s for s, _p, _o in self.all_triples if p == _p and o == _o}

    def exists_o_for_s_p(self, s: str, p: str) -> set[str]:
        return {o for _s, _p, o in self.all_triples if s == _s and p == _p}

    def any_item(self, s: set[Any]) -> Optional[Any]:
        for i in s:
            return i
        return None

    def get_all_s(self, po: set[tuple[str, str]]) -> set[str]:
        first = self.any_item(po)
        if not first:
            return set()
        p, o = first
        res = self.exists_s_for_p_o(p, o)
        for p, o in po:
            res &= self.exists_s_for_p_o(p, o)
        return res

    def check(self, expected: set[Triple]):
        for t in expected:
            self.assertIn(t, self.triples)

    def get_subj(self, s_set: set[str]) -> str:
        self.assertEqual(len(s_set), 1)
        s = list(s_set)[0]
        self.assertRegex(s, self.prefix_re)
        return s

    def test_gtfs_prefixes(self):
        # All used prefixes are declared
        for _triple in self.all_triples:
            for value in _triple:
                m = re.match(self.prefix_re, value)
                if m:
                    prefix = m.group("prefix")
                    self.assertIn(prefix, self.prefixes)

    def test_gtfs_agency(self):
        # Agency
        self.check({
            (self.agency, 'a', 'gtfs:Agency'),
            (self.agency, 'rdfs:member', 'gtfs:feed_minimalgtfs'),
            (self.agency, 'dct:identifier', '"10443"^^xsd:integer'),
            (self.agency, 'foaf:name',
             '"DB Regio AG Baden-W\\u00fcrttemberg"'),
            (self.agency, 'foaf:page', '"https://www.bahn.de"'),
            (self.agency, 'gtfs:timeZone', '"Europe/Berlin"')
        })

    def test_gtfs_feed_info(self):
        # Feed info
        s = self.get_subj(self.get_all_s({
            ('a', 'gtfs:Feed'),
            ('rdfs:member', 'gtfs:feed_minimalgtfs'),
            ('rdfs:label', '"DELFI e.V."'),
            ('dct:publisher', '"https://www.delfi.de"'),
            ('dct:language', '"de"'),
            ('schema:startDate', '"20240520"^^xsd:integer'),
            ('schema:endDate', '"20241214"^^xsd:integer'),
            ('schema:version', '"2024-6-3T13:31:45"'),
            ('dcat:contactPoint', '"x@y.de"'),
            ('gtfs:feed_contact_url', '"https://www.delfi.de"'),
            ('a', 'dcat:Dataset')
        }))
        self.assertRegex(s, r'^gtfs:feed_minimalgtfs_\w+$')

        self.check({
            (s, 'dct:temporal', s + '_temporal'),
            (s + '_temporal', 'schema:startDate', '"2024/05/20"^^xsd:date'),
            (s + '_temporal', 'schema:endDate', '"2024/12/14"^^xsd:date')
        })

    def test_gtfs_calendar(self):
        # Calendar
        s = self.get_subj(self.get_all_s({
            ('a', 'gtfs:CalendarRule'),
            ('rdfs:member', 'gtfs:feed_minimalgtfs'),
            ('gtfs:service', 'gtfs:service_minimalgtfs_5957'),
            ('gtfs:monday', '"true"^^xsd:boolean'),
            ('gtfs:tuesday', '"true"^^xsd:boolean'),
            ('gtfs:wednesday', '"true"^^xsd:boolean'),
            ('gtfs:thursday', '"true"^^xsd:boolean'),
            ('gtfs:friday', '"true"^^xsd:boolean'),
            ('gtfs:saturday', '"false"^^xsd:boolean'),
            ('gtfs:sunday', '"false"^^xsd:boolean'),
            ('schema:startDate', '"20240520"^^xsd:integer'),
            ('schema:endDate', '"20241214"^^xsd:integer')
        }))
        self.assertRegex(s, r'^gtfs:calendar_minimalgtfs_\w+$')

        self.check({
            (s, 'dct:temporal', s + '_temporal'),
            (s + '_temporal', 'schema:startDate', '"2024/05/20"^^xsd:date'),
            (s + '_temporal', 'schema:endDate', '"2024/12/14"^^xsd:date')
        })

    def test_gtfs_calendar_dates(self):
        # Calendar Dates
        cd_s1 = self.get_subj(self.get_all_s({
            ('a', 'gtfs:CalendarDateRule'),
            ('rdfs:member', 'gtfs:feed_minimalgtfs'),
            ('gtfs:service', 'gtfs:service_minimalgtfs_5957'),
            ('dct:date', '"20240520"^^xsd:integer'),
            ('gtfs:dateAddition', '"false"^^xsd:boolean'),
        }))

        cd_s2 = self.get_subj(self.get_all_s({
            ('a', 'gtfs:CalendarDateRule'),
            ('rdfs:member', 'gtfs:feed_minimalgtfs'),
            ('gtfs:service', 'gtfs:service_minimalgtfs_5957'),
            ('dct:date', '"20240526"^^xsd:integer'),
            ('gtfs:dateAddition', '"true"^^xsd:boolean')
        }))

        for c in (cd_s1, cd_s2):
            self.assertRegex(c, r'^gtfs:calendar_date_minimalgtfs_\w+$')
        self.assertNotEqual(cd_s1, cd_s2)

    def test_gtfs_frequency(self):
        # Frequency
        s = self.get_subj(self.get_all_s({
            ('a', 'gtfs:Frequency'),
            ('rdfs:member', 'gtfs:feed_minimalgtfs'),
            ('gtfs:trip', 'gtfs:trip_minimalgtfs_2507470876'),
            ('gtfs:startTime', '"6:30:00"'),
            ('gtfs:endTime', '"7:00:00"'),
            ('gtfs:headwaySeconds', '"60"^^xsd:integer')
        }))
        self.assertRegex(s, r'^gtfs:frequency_minimalgtfs_\w+$')

    def test_gtfs_route(self):
        # Route
        self.check({
            (self.route, 'a', 'gtfs:Route'),
            (self.route, 'rdfs:member', 'gtfs:feed_minimalgtfs'),
            (self.route, 'dct:identifier', '"3108738_106"'),
            (self.route, 'gtfs:agency', self.agency),
            (self.route, 'gtfs:shortName', '"RE7"'),
            (self.route, 'gtfs:routeType', '"106"')
        })

    def test_gtfs_shape(self):
        # Shapes
        self.check({
            (self.shape, 'a', 'gtfs:Shape')
        })

        sp1 = self.get_subj(self.get_all_s({
            ('a', 'gtfs:ShapePoint'),
            ('rdfs:member', 'gtfs:feed_minimalgtfs'),
            ('geo:lat', '"48.476475"^^xsd:decimal'),
            ('geo:long', '"7.946723"^^xsd:decimal'),
            ('gtfs:pointSequence', '"0"^^xsd:integer')
        }))

        self.check({
            (self.shape, 'gtfs:shapePoint', sp1),
            (sp1, 'geo:hasGeometry', sp1 + '_geo'),
            (sp1, 'geo:hasCentroid', sp1 + '_geo'),
            (sp1 + '_geo', 'geo:asWKT',
             '"POINT(7.946723 48.476475)"^^geo:wktLiteral')
        })

        sp2 = self.get_subj(self.get_all_s({
            ('a', 'gtfs:ShapePoint'),
            ('rdfs:member', 'gtfs:feed_minimalgtfs'),
            ('geo:lat', '"48.381589"^^xsd:decimal'),
            ('geo:long', '"7.862948"^^xsd:decimal'),
            ('gtfs:pointSequence', '"1"^^xsd:integer')
        }))

        self.check({
            (self.shape, 'gtfs:shapePoint', sp2),
            (sp2, 'geo:hasGeometry', sp2 + '_geo'),
            (sp2, 'geo:hasCentroid', sp2 + '_geo'),
            (sp2 + '_geo', 'geo:asWKT',
             '"POINT(7.862948 48.381589)"^^geo:wktLiteral')
        })

        sp3 = self.get_subj(self.get_all_s({
            ('a', 'gtfs:ShapePoint'),
            ('rdfs:member', 'gtfs:feed_minimalgtfs'),
            ('geo:lat', '"48.381589"^^xsd:decimal'),
            ('geo:long', '"7.862948"^^xsd:decimal'),
            ('gtfs:pointSequence', '"2"^^xsd:integer')
        }))

        self.check({
            (self.shape, 'gtfs:shapePoint', sp3),
            (sp3, 'geo:hasGeometry', sp3 + '_geo'),
            (sp3, 'geo:hasCentroid', sp3 + '_geo'),
            (sp3+'_geo', 'geo:asWKT',
             '"POINT(7.862948 48.381589)"^^geo:wktLiteral')
        })

        for s in (sp1, sp2, sp3):
            self.assertRegex(s, r'^gtfs:shapepoint_minimalgtfs_\w+$')

        self.assertNotEqual(sp1, sp2)
        self.assertNotEqual(sp1, sp3)
        self.assertNotEqual(sp2, sp3)

    def test_gtfs_linestring_minimal(self):
        # Shape Line String
        self.check({
            (self.shape, 'geo:hasGeometry', self.shape + '_geo'),
            (self.shape + '_geo', 'geo:asWKT',
             '"LINESTRING(7.946723 48.476475, 7.862948 48.381589, ' +
             '7.862948 48.381589)"^^geo:wktLiteral')
        })

    def check_parent_station(self, stop: str):
        parent_station = self.get_subj(
            self.exists_o_for_s_p(stop, 'gtfs:parentStation'))
        self.check({(parent_station, "a", "gtfs:Station")})
        self.assertRegex(parent_station, r'^gtfs:station_\w+$')

    def test_gtfs_stop(self):
        # Stops
        self.check({
            (self.stop1, 'a', 'gtfs:Stop'),
            (self.stop1, 'rdfs:member', 'gtfs:feed_minimalgtfs'),
            (self.stop1, 'dct:identifier', '"de0831690112"'),
            (self.stop1, 'foaf:name', '"Emmendingen Bahnhof"'),
            (self.stop1, 'dct:description', '"Zug->Freiburg"'),
            (self.stop1, 'geo:lat', '"48.119569000000"^^xsd:decimal'),
            (self.stop1, 'geo:long', '"7.847548000000"^^xsd:decimal'),
            (self.stop1, 'gtfs:location_type', '"0"^^xsd:integer'),
            (self.stop1, 'gtfs:wheelchairAccessible', '"0"^^xsd:integer'),
            (self.stop1, 'gtfs:platform', '"2"^^xsd:integer'),
            (self.stop1, 'gtfs:level_id', '"2"^^xsd:integer'),
            (self.stop1, 'geo:hasGeometry', self.stop1 + '_geo'),
            (self.stop1, 'geo:hasCentroid', self.stop1 + '_geo'),
            (self.stop1 + '_geo', 'geo:asWKT',
             '"POINT(7.847548000000 48.119569000000)"^^geo:wktLiteral')
        })
        self.check_parent_station(self.stop1)

        self.check({
            (self.stop2, 'a', 'gtfs:Stop'),
            (self.stop2, 'rdfs:member', 'gtfs:feed_minimalgtfs'),
            (self.stop2, 'dct:identifier', '"de08311650864"'),
            (self.stop2, 'foaf:name', '"Freiburg Hauptbahnhof"'),
            (self.stop2, 'dct:description', '"Bahnsteig Gleis 4"'),
            (self.stop2, 'geo:lat', '"47.997765000000"^^xsd:decimal'),
            (self.stop2, 'geo:long', '"7.841295000000"^^xsd:decimal'),
            (self.stop2, 'gtfs:location_type', '"0"^^xsd:integer'),
            (self.stop2, 'gtfs:wheelchairAccessible', '"0"^^xsd:integer'),
            (self.stop2, 'gtfs:platform', '"4"^^xsd:integer'),
            (self.stop2, 'gtfs:level_id', '"2"^^xsd:integer'),
            (self.stop2, 'geo:hasGeometry', self.stop2 + '_geo'),
            (self.stop2, 'geo:hasCentroid', self.stop2 + '_geo'),
            (self.stop2 + '_geo', 'geo:asWKT',
             '"POINT(7.841295000000 47.997765000000)"^^geo:wktLiteral')
        })
        self.check_parent_station(self.stop2)

    def test_gtfs_stop_time(self):
        # Stop Times
        st1 = self.get_subj(self.get_all_s({
            ('a', 'gtfs:StopTime'),
            ('rdfs:member', 'gtfs:feed_minimalgtfs'),
            ('gtfs:trip', self.trip),
            ('gtfs:arrivalTime', '"5:49:00"'),
            ('gtfs:departureTime', '"5:50:00"'),
            ('gtfs:stop', self.stop1),
            ('gtfs:stopSequence', '"10"^^xsd:integer'),
            ('gtfs:pickupType', 'gtfs:Regular'),
            ('gtfs:dropOffType', 'gtfs:Regular')
        }))
        st2 = self.get_subj(self.get_all_s({
            ('a', 'gtfs:StopTime'),
            ('rdfs:member', 'gtfs:feed_minimalgtfs'),
            ('gtfs:trip', self.trip),
            ('gtfs:arrivalTime', '"6:07:00"'),
            ('gtfs:departureTime', '"6:27:00"'),
            ('gtfs:stop', self.stop2),
            ('gtfs:stopSequence', '"16"^^xsd:integer'),
            ('gtfs:pickupType', 'gtfs:Regular'),
            ('gtfs:dropOffType', 'gtfs:Regular')
        }))
        for st in (st1, st2):
            self.assertRegex(st, r'^gtfs:stop_time_\w+$')

    def test_gtfs_transfer_rule(self):
        # Transfer rule
        tr = self.get_subj(self.get_all_s({
            ('a', 'gtfs:TransferRule'),
            ('rdfs:member', 'gtfs:feed_minimalgtfs'),
            ('gtfs:originStop', 'gtfs:stop_minimalgtfs_de064402404'),
            ('gtfs:destinationStop', 'gtfs:stop_minimalgtfs_de06440240411'),
            ('gtfs:transferType', 'gtfs:MinimumTimeTransfer'),
            ('gtfs:minimumTransferTime', '"180"^^xsd:integer')
        }))
        self.assertRegex(tr, r'^gtfs:transfer_rule_\w+$')

    def test_gtfs_trip(self):
        # Trip
        self.check({
            (self.trip, 'a', 'gtfs:Trip'),
            (self.trip, 'rdfs:member', 'gtfs:feed_minimalgtfs'),
            (self.trip, 'gtfs:route', self.route),
            (self.trip, 'gtfs:service', 'gtfs:service_minimalgtfs_5957'),
            (self.trip, 'dct:identifier', '"2508971872"^^xsd:integer'),
            (self.trip, 'gtfs:headsign', '"Basel Bad Bf"'),
            (self.trip, 'gtfs:shortName', '"05331"^^xsd:integer'),
            (self.trip, 'gtfs:direction', '"false"^^xsd:boolean'),
            (self.trip, 'gtfs:shape', self.shape),
            (self.trip, 'gtfs:wheelchairAccessible', '"false"^^xsd:boolean'),
            (self.trip, 'gtfs:bikesAllowed', '"false"^^xsd:boolean')
        })

        os.chdir(prog_dir)

    @patch("gtfs2rdf.logger.info", lambda *_: None)
    def test_gtfs_using_counters(self):
        os.chdir(Path(prog_dir, "test"))

        # Create object and extract sample feed
        with self.assertLogs("gtfs2rdf", level="WARNING"):
            result = self.make_gtfs_and_get_result("sample-feed-1")
        lines = result.splitlines()
        result = "\n".join(
            line for line in lines if not line.startswith("@prefix"))

        # Test whether some examples from the data are present in the result
        expect = {
            "\n": 496,  # Correct number of triples
            "rdfs:member gtfs:feed_samplefeed1 .": 68,
            "a gtfs:Agency .": 1,
            "a gtfs:CalendarRule .": 2,
            '_temporal schema:startDate "2007/01/01"^^xsd:date .': 2,
            'gtfs:sunday "true"^^xsd:boolean .': 2,
            'a gtfs:CalendarDateRule .': 1,
            'a gtfs:Frequency .': 11,
            'a gtfs:Route .': 5,
            'a gtfs:Stop .': 9,
            'geo:asWKT "POINT(-117.133162 36.425288)"^^geo:wktLiteral .': 1,
            'geo:hasGeometry': 9,
            'geo:hasCentroid': 9
        }
        for _e, _n in expect.items():
            self.assertEqual(result.count(_e), _n)

        os.chdir(prog_dir)

    @patch("gtfs2rdf.logger.info", lambda *_: None)
    def test_gtfs_linestrings(self):
        os.chdir(Path(prog_dir, "test"))

        result2 = self.make_gtfs_and_get_result("feed-only-shapes")
        lines2 = set(result2.splitlines())
        exp_triple = "gtfs:shape_feedonlyshapes_A_shp_geo geo:asWKT " + \
            "\"LINESTRING(-122.48161 37.61956, -122.41070 37.64430, " + \
            "-122.30839 37.65863)\"^^geo:wktLiteral ."
        self.assertIn(exp_triple, lines2)

        os.chdir(prog_dir)


class TestElection(unittest.TestCase):
    @patch("election2rdf.logger.info", lambda *_: None)
    @patch("dataset.logger.info", lambda *_: None)
    def test_election(self):
        aux_geo_: list[tuple[str, str]] = []

        def aux_geo(_id: str, _wkt: str):
            nonlocal aux_geo_
            aux_geo_.append((_id, _wkt))

        test_cmd = "echo \"id;label;country;date\\n1;$ELECTION_LABEL;" + \
            "$ELECTION_COUNTRY;$ELECTION_DATE\""
        test_fn = "test/env.csv"
        Path(test_fn).unlink(True)
        datasets = [
            CSVDataset(_dataset="x",
                       _command=test_cmd,
                       _store_filename=test_fn,
                       _primary_prefix="election:",
                       primary_col="id",
                       csv_separator=";"),
            KMLDataset(_dataset="k",
                       _command="cat test/test.kml",
                       _store_filename="test/test.kml",
                       _primary_prefix="election:")
        ]
        i = int(next_id())
        e = Election("Demo Election 2021", datasets, "DemoCountry", "Q1",
                     "123", 2021, "2021/01/01", "demo_", aux_geo)
        self.assertEqual(e.id_prefix, "demo_")
        e.get_all_data()
        self.assertListEqual(list(e.datasets[0].rdf()), [
            ("election:1", "a", "election:x"),
            ("election:1", "election:id", "\"1\"^^xsd:integer"),
            ("election:1", "election:label", "\"Demo Election 2021\""),
            ("election:1", "election:country", "\"DemoCountry\""),
            ("election:1", "election:date", "\"2021/01/01\"^^xsd:date")
        ])
        output = list(e.rdf())
        subj = "election:demo_" + str(i + 1)
        self.assertListEqual(output[:7], [
            (subj, "a", "election:election"),
            (subj, "rdfs:label", "\"Demo Election 2021\""),
            (subj, "election:wikidata", "wd:Q1"),
            (subj, "election:osm", "osmrel:123"),
            (subj, "election:countryname", "\"DemoCountry\""),
            (subj, "election:date", "\"2021/01/01\"^^xsd:date"),
            (subj, "election:year", "\"2021\"^^xsd:integer")
        ])

        test_fn2 = "test/out.ttl"
        test_fn3 = "test/out.tsv"
        Path(test_fn2).unlink(True)
        Path(test_fn3).unlink(True)
        with open(test_fn2, "w") as f1, open(test_fn3, "w") as f2:
            e.to_file(f1, f2)

        f1_content = ""
        f2_content = ""
        with open(test_fn2, "r") as f1, open(test_fn3, "r") as f2:
            f1_content = f1.read()
            f2_content = f2.read()
        self.assertGreater(f1_content.count("election:"), 30)
        self.assertGreaterEqual(f1_content.count(" a "), 3)
        self.assertGreaterEqual(f1_content.count(" rdfs:member "), 2)
        self.assertGreater(f1_content.count(" .\n"), 15)
        self.assertEqual(f1_content.count("geo:hasGeometry"), 1)
        self.assertEqual(f2_content.count("\t"), 1)
        self.assertEqual(f2_content.count("election:"), 1)
        self.assertEqual(f2_content.count("POLYGON("), 1)

    def test_load_config(self):
        os.chdir(Path(prog_dir, "test"))
        e = Election.load_from_config("election_mini.json")
        self.assertEqual(e.label, "ONLY FOR TESTING")
        self.assertEqual(e.countryname, "Test land")
        self.assertEqual(e.wikidata, "Q1")
        self.assertEqual(e.osm, "1")
        self.assertEqual(e.year, 2021)
        self.assertEqual(e.date, "26.09.2021")
        self.assertEqual(e.id_prefix, "btw21_")
        self.assertIsNone(e.aux_geo_callback)
        self.assertEqual(len(e.datasets), 2)
        self.assertCountEqual(e.datasets, [
            KMLDataset(_dataset="districts", _command="cat KML_Samples.kml",
                       _store_filename="election_test.kml",
                       _primary_prefix="election:districts_",
                       aux_geo_callback=None),
            CSVDataset(_dataset="result",
                       _command="cat basic.csv",
                       _store_filename="election_test.csv",
                       _primary_prefix="election:result_",
                       primary_col=None,
                       csv_separator=";",
                       csv_quote="\"",
                       column_mapping={
                           "a": "body",
                           "b": "date",
                           "c": "district"
                       }),
        ])
        os.chdir(prog_dir)


class TestRunAsProgram(unittest.TestCase):
    """
    Tests for the programs' main routines
    """

    @patch("csv2rdf.logger.info", lambda *_: None)
    def test_csv2rdf(self):
        p = str(Path(prog_dir, "test"))
        args = [
            "-i", p + "/separator.csv",
            "-d", "separator",
            "-p", "test:abc",
            "-r", "https://example.com/#",
            "-o", "csv_test.ttl.bz2",
            "--separator", "|",
            "--quote", "*",
            "-cm", '{"c": "f"}',
            "-vm", json.dumps({
                "f": ([
                    ("3", " "),
                    ("(?P<test>\\W+)$", "\\g<test>\\g<test>")
                ], "lit"),
                "d": ([("^0*", "")], "lit"),
                "e": ([("^0*", "")], "iri")
            }),
            "-a", '{"additionalprefix":  "https://example.com/additional#"}'
        ]

        with TemporaryDirectory() as d:
            os.chdir(d)
            csv2rdf_main(args)

            # Check result
            with bz2.open(Path("csv_test.ttl.bz2"), "r") as f:
                result = f.read().decode("utf-8").splitlines()

        triples_no_prefixes = [
            tuple(line[:-2].split(" ", 2))
            for line in result
            if not line.startswith("@prefix")
        ]
        self.assertEqual(len(triples_no_prefixes), 6)
        subj = triples_no_prefixes[0][0]
        self.assertTrue(subj.startswith("test:abc"))
        self.assertListEqual(triples_no_prefixes, [
            (subj, "a", "test:separator"),
            (subj, "test:a", "\"1\"^^xsd:integer"),
            (subj, "test:b", "\"2|2\""),
            (subj, "test:f", "\"hello world # #\""),
            (subj, "test:d", "\"5.20\"^^xsd:decimal"),
            (subj, "test:e", "5"),])
        prefixes = [line for line in result if line.startswith("@prefix")]
        self.assertIn(
            '@prefix additionalprefix: <https://example.com/additional#> .',
            prefixes)
        os.chdir(prog_dir)

    @patch("kml2rdf.logger.info", lambda *_: None)
    @patch("dataset.logger.info", lambda *_: None)
    def test_kml2rdf(self):
        p = str(Path(prog_dir, "test"))

        def args(fn: str) -> list[str]:
            return [
                "-i", p + "/" + fn,
                "-d", "myKMLdataset",
                "-o", fn + ".ttl.bz2",
                "-p", "example:",
                "-r", "http://example.com/",
                "-e", "example:something",
                "-x", fn + "-aux.tsv"
            ]

        # Runs without error
        with TemporaryDirectory() as d:
            os.chdir(d)
            kml2rdf_main(args("KML_Samples.kml"))

        # Bad filename KML but is named KMZ
        os.chdir(prog_dir)
        shutil.copyfile("test/KML_Samples.kml", "test/kml_bad_name.kmz")
        with self.assertLogs('kml2rdf', level='WARNING') as log:
            with TemporaryDirectory() as d:
                os.chdir(d)
                kml2rdf_main(args("kml_bad_name.kmz"))
            self.assertEqual(len(log.output), 1)
            self.assertIn(
                "Input filename ends with .kmz, but option --kmz is " +
                "not given. Treating as KML.", log.output[0])

        # Bad filename is accepted: KML as anything else
        os.chdir(prog_dir)
        shutil.copyfile("test/KML_Samples.kml", "test/kml_bad_name.txt")
        with self.assertNoLogs('kml2rdf', level='WARNING'):
            with TemporaryDirectory() as d:
                os.chdir(d)
                kml2rdf_main(args("kml_bad_name.txt"))

        # Bad filename: KMZ as KML
        os.chdir(prog_dir)
        shutil.copyfile("test/KML_Samples.kmz", "test/kml_bad_name.kml")
        with self.assertLogs('kml2rdf', level='WARNING') as log:
            with TemporaryDirectory() as d:
                os.chdir(d)
                kml2rdf_main(args("kml_bad_name.kml") + ["-z"])
            self.assertEqual(len(log.output), 1)
            self.assertIn("The filename ends .kml but is treated as .kmz",
                          log.output[0])

        with TemporaryDirectory() as d:
            os.chdir(d)
            kml2rdf_main(args("KML_Samples.kmz") + ["-z"])

        with self.assertRaisesRegex(AssertionError,
                                    "Valid KMZ must contain exactly one " +
                                    "\\.kml file"):
            with TemporaryDirectory() as d:
                os.chdir(d)
                kml2rdf_main(args("invalid.kmz") + ["-z"])

        os.chdir(prog_dir)

    @patch("gtfs2rdf.logger.info", lambda *_: None)
    def test_gtfs2rdf(self):
        os.chdir(prog_dir)
        p = str(Path(prog_dir, "test"))
        args = [
            "-f", "somefeed",
            "-i", p + "/sample-feed-1.zip",
            "-o", p + "/sample-feed.ttl.bz2",
            "-x", p + "/sample-feed-aux.tsv"
        ]

        # Runs without error and calls the GTFS feed class appropriately
        with TemporaryDirectory() as d, \
                patch("gtfs2rdf.GTFSFeed") as mock_gtfs:
            os.chdir(d)
            gtfs2rdf_main(args)
            mock_gtfs.assert_called_once_with("somefeed",
                                              p + "/sample-feed-1.zip",
                                              [], False)
        os.chdir(prog_dir)
        # Content for the given input is tested in the TestGTFS class

    @patch("election2rdf.logger.info", lambda *_: None)
    @patch("dataset.logger.info", lambda *_: None)
    def test_election2rdf(self):
        os.chdir(prog_dir)
        p = str(Path(prog_dir, "test"))

        def args(wrong_fn: bool = False) -> list[str]:
            return [
                "-c", p + "/election_mini.json",
                "-o", "election.ttl" + ("" if wrong_fn else ".bz2"),
                "-x", "election-aux.tsv"
            ]

        # Runs without error
        result_: list[str] = []
        result_aux: list[tuple[str, ...]] = []
        with TemporaryDirectory() as d:
            os.chdir(d)
            shutil.copy(Path(p, "test.kml"), d)
            shutil.copy(Path(p, "basic_e.csv"), d)
            election2rdf_main(args())

            with bz2.open("election.ttl.bz2", "r") as f:
                result_ = f.read().decode("utf-8").splitlines()
            with open("election-aux.tsv", "r") as f:
                result_aux = [tuple(line.split("\t", 1)) for line in f if line]

        result_u = {
            tuple(line[:-2].split(" ", 2))
            for line in result_
            if not line.startswith("@prefix")
        }

        # Get main subject
        def get_subj(type_: str = "election", n: int = 1) -> set[str]:
            t = {
                s for s, p, o in result_u
                if p == "a" and o == "election:" + type_
            }
            self.assertEqual(len(t), n)
            for subj in t:
                pref_ = (type_ + "_") if type_ != "election" else ""
                self.assertRegex(subj, r'^election:' + pref_ + r'btw21_+\d+$')
            return t

        def get_p_o(subj: str) -> set[tuple[str, ...]]:
            return {(p, o) for s, p, o in result_u if s == subj}

        # Test metadata triples
        main_subj = list(get_subj())[0]
        self.assertSetEqual(get_p_o(main_subj), {
            ('election:year', '"2021"^^xsd:integer'),
            ('election:wikidata', 'wd:Q1'),
            ('election:countryname', '"Test land"'),
            ('election:date', '"2021/09/26"^^xsd:date'),
            ('a', 'election:election'),
            ('election:osm', 'osmrel:1'),
            ('rdfs:label', '"ONLY FOR TESTING"')
        })

        kml_subj = list(get_subj("districts"))[0]
        self.assertSetEqual(get_p_o(kml_subj), {
            ('a', 'election:districts'),
            ('rdfs:comment', '"A multi polygon in kml"'),
            ('geo:hasGeometry', kml_subj + '_geo'),
            ('dct:identifier', '"example1_1"'),
            ('rdfs:member', main_subj),
            ('rdfs:label', '"example 1.1"')
        })
        self.assertSetEqual(get_p_o(kml_subj + "_geo"), {
            ('geo:asWKT', "\"MULTIPOLYGON((" +
             "(1.0 1.5, 2.0 2.5, 3.0 4.0, 3.0 1.5), " +
             "(2.0 2.25, 2.5 2.25, 2.5 2.5)), " +
             "((11.0 11.5, 12.0 12.5, 13.0 14.0, 13.0 11.5), " +
             "(12.0 12.25, 12.5 12.25, 12.5 12.5)))\"^^geo:wktLiteral")
        })

        csv_subjs = get_subj("result", 2)
        expected1 = {
            ('a', 'election:result'),
            ('rdfs:member', main_subj),
            ('election:date', '"k"'),
            ('election:body', '"1"^^xsd:integer'),
            ('election:district', '"l"'),
        }
        expected2 = {
            ('election:date', '"m"'),
            ('a', 'election:result'),
            ('rdfs:member', main_subj),
            ('election:district', '"01.01.2024;x"'),
            ('election:body', '"-4.5"^^xsd:decimal')
        }

        actual = {frozenset(get_p_o(s)) for s in csv_subjs}
        expected = {frozenset(expected1), frozenset(expected2)}
        self.assertSetEqual(actual, expected)

        self.assertListEqual(result_aux, [
            (kml_subj, "MULTIPOLYGON((" +
             "(1.0 1.5, 2.0 2.5, 3.0 4.0, 3.0 1.5), " +
             "(2.0 2.25, 2.5 2.25, 2.5 2.5)), " +
             "((11.0 11.5, 12.0 12.5, 13.0 14.0, 13.0 11.5), " +
             "(12.0 12.25, 12.5 12.25, 12.5 12.5)))\n")])

        # Test name warning
        with TemporaryDirectory() as d:
            os.chdir(d)
            shutil.copy(Path(p, "test.kml"), d)
            shutil.copy(Path(p, "basic_e.csv"), d)
            with self.assertLogs("election2rdf", level="WARN") as log:
                election2rdf_main(args(True))
                self.assertEqual(len(log.output), 1)
                self.assertIn("Output filename does not end in '.ttl.bz2'",
                              log.output[0])
        os.chdir(prog_dir)


if __name__ == '__main__':
    unittest.main()
