#!/bin/env python3
"""
Unit tests for compose_spatial.py

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

from enum import Enum
import json
import re
from io import BytesIO
from socket import socket
from typing import Optional, Any
import unittest
from unittest.mock import patch, mock_open
from email.message import Message
from pathlib import Path
from compose_spatial import ComposeSpatialHTTPRequestHandler, \
    ComposeSpatialHTTPServer, QueryConfig, Template, ReplaceRule, \
    SpatialSearch, SpatialSearchConfig, ProvidedValues, GroupTemplate, \
    RightShard, cli_main, group, clean_name, get, indent, Mime, guess_type, \
    get_config_and_queries, get_all_configs, get_file_contents, main, \
    serve_main

PROGRAM_DIR = Path(__file__).parent.resolve()
assert PROGRAM_DIR.is_dir(), "Unit tests cannot be run from ZIP module"

try:
    with open(PROGRAM_DIR / "test" / "write.txt", "w") as f:
        f.write("test")
except PermissionError:
    raise PermissionError(
        "The unit tests in this module require write access to " +
        str(PROGRAM_DIR / "test") + ".")


def sparql_eq_helper(code: str) -> str:
    """
    Helper function that removes most whitespace and comments from a SPARQL
    string to make unit tests easier.
    """
    lines = code.replace("\r", "").split("\n")
    for i, line in enumerate(lines):
        # Remove comments and leading/trailing whitespace
        lines[i] = re.sub(r'#.*$', '', line).strip()
    return " ".join(line for line in lines if line != "")


def always_raises(*args, **kwargs):
    raise AssertionError("This should be unreachable")


class ComposeSpatialTestCase(unittest.TestCase):
    def assertSparqlEq(self, a: str, b: str):
        self.assertEqual(sparql_eq_helper(a), sparql_eq_helper(b))


class TestComposeSpatial(ComposeSpatialTestCase):

    def test_from_config(self):
        pth = PROGRAM_DIR / "test" / "test_compose.json"
        with open(pth, "r") as f:
            config = json.load(f)
        qc = QueryConfig.from_config(config)
        expected = QueryConfig(
            template=Template(
                filename='template.rq',
                replace_rules=[
                    ReplaceRule(
                        search='%DEMO%',
                        replace='demo',
                        replace_file=None
                    ),
                    ReplaceRule(
                        search='%TEST%',
                        replace=None,
                        replace_file='test.rq'
                    )
                ]
            ),
            spatial_searches=[
                SpatialSearch(
                    config=SpatialSearchConfig(
                        algorithm='s2',
                        maxDistance=700,
                        numNearestNeighbors=2
                    ),
                    left=['station.rq'],
                    right=[
                        RightShard(
                            filename='restaurant.rq',
                            payload=['?restaurant_name']
                        )
                    ],
                    template_pattern='%SPATIALSEARCH%',
                    group_template=GroupTemplate(
                        filename='group_template.rq',
                        queries_pattern='%QUERIES%',
                        select_pattern='%SELECT%'
                    ),
                    group_size=10,
                    add_selectors=['%CENTROID%', '%DIST%'],
                    dist_pattern='%DIST%',
                    count_pattern='%COUNT%',
                    centroid_pattern='%CENTROID%',
                    provided_values=[
                        ProvidedValues(
                            variable='?poi_predicates',
                            values=[
                                'osmkey:building',
                                'osmkey:shop',
                                'osmkey:amenity',
                                'osmkey:landuse'
                            ]
                        )
                    ],
                    name_template='%LEFT%_%RIGHT%',
                    name_type_pattern='%TYPE%',
                    name_left_pattern='%LEFT%',
                    name_right_pattern='%RIGHT%'
                )
            ]
        )
        self.assertEqual(qc, expected)

    def test_query_config(self):
        # Invariants
        with self.assertRaises(AssertionError):
            QueryConfig(None, [])  # type: ignore
        with self.assertRaises(AssertionError):
            QueryConfig(Template("abc.rq", []), [])

        # Compose
        with patch("compose_spatial.SpatialSearch.compose") as mock_scompose, \
                patch("compose_spatial.Template.compose") as mock_tcompose:
            pth = PROGRAM_DIR / "test" / "test_compose.json"
            with open(pth, "r") as f:
                config = json.load(f)
            qc = QueryConfig.from_config(config)

            mock_scompose.return_value = "x"
            mock_tcompose.return_value = "y"
            self.assertEqual(qc.compose({}), "y\n")
            mock_tcompose.assert_called_once()
            mock_scompose.assert_called_once()

    def test_template(self):
        # Invariants
        with self.assertRaises(AssertionError):
            Template("", [])
        with self.assertRaises(AssertionError):
            Template("", [
                ReplaceRule("%SEARCH%", "Something", None)
            ])

        # Compose
        replace = [
            ReplaceRule("%SEARCH1%", "Something:%SEARCH2%", None),
            ReplaceRule("%SEARCH2%", None, "replace.rq"),
        ]
        t = Template("example.rq", replace)
        files = {
            "example.rq": """
            SELECT * WHERE {
                ?x <pred> "%SEARCH1%" .
                {
                    %GROUPA%
                }
                {
                    %GROUPB%
                }
            }
            """,
            "replace.rq": "Search2ReplaceFile"
        }
        groups = [
            ("%GROUPA%", "{ ?a ?b ?c . }"),
            ("%GROUPA%", "{ ?x ?y ?z . }"),
            ("%GROUPB%", "{ ?a <b> \"%SEARCH2%\" . }"),
        ]
        out = t.compose(files, groups)
        expected = """
            SELECT * WHERE {
                ?x <pred> "Something:Search2ReplaceFile" .
                {
                    { ?a ?b ?c . }
                    { ?x ?y ?z . }
                }
                {
                    { ?a <b> \"Search2ReplaceFile\" . }
                }
            }
        """
        self.assertSparqlEq(out, expected)

        replace2 = [ReplaceRule("%SEARCH1%", "%SEARCH1%", None)]
        t2 = Template("example.rq", replace2)
        with self.assertRaisesRegex(RecursionError,
                                    "Maximum replace depth exceeded"):
            t2.compose(files, groups)

    def test_replace_rule(self):
        # Invariants
        with self.assertRaises(AssertionError):
            ReplaceRule("", "x", None)
        with self.assertRaises(AssertionError):
            ReplaceRule("xyz", None, None)
        with self.assertRaises(AssertionError):
            ReplaceRule("xyz", "x", "y")

        # Method: can be replaced
        self.assertTrue(ReplaceRule("%SEARCH%", "Something", None)
                        .can_be_replaced("xyz%SEARCH%xyz"))
        self.assertTrue(ReplaceRule("%(\\w+)%", "Something", None)
                        .can_be_replaced("xyz%SEARCH%xyz"))
        self.assertFalse(ReplaceRule("%(\\w+)%", "Something", None)
                         .can_be_replaced("xyz%%xyz"))
        self.assertFalse(ReplaceRule("%(\\w+)%", "Something", None)
                         .can_be_replaced("xyz"))

        # Method: apply
        self.assertEqual(ReplaceRule("%(\\w+)%", "Something", None)
                         .apply({}, "xyz%xyz%xyz"), "xyzSomethingxyz")
        self.assertEqual(ReplaceRule("%(\\w+)%", "Something", None)
                         .apply({}, "xyz%%xyz"), "xyz%%xyz")
        self.assertEqual(ReplaceRule("%(\\w+)%", "Something\\1", None)
                         .apply({}, "xyz%xyz%xyz"), "xyzSomethingxyzxyz")
        self.assertEqual(ReplaceRule("%(\\w+)%", None, "replace.rq")
                         .apply({
                             "replace.rq": "Something\\1."
                         }, "xyz%xyz%xyz"), "xyzSomething\\1.xyz")

    def test_spatial_search_config(self):
        # Invariants
        with self.assertRaises(AssertionError):
            SpatialSearchConfig("abc", 1, 1)
        with self.assertRaises(AssertionError):
            SpatialSearchConfig("s2", None, None)
        with self.assertRaises(AssertionError):
            SpatialSearchConfig("s2", 0, 0)
        with self.assertRaises(AssertionError):
            SpatialSearchConfig("s2", -2, None)
        with self.assertRaises(AssertionError):
            SpatialSearchConfig("s2", None, -1)

        conf1 = SpatialSearchConfig(
            algorithm="s2", maxDistance=100, numNearestNeighbors=None)
        self.assertEqual(str(conf1), "maxDist100")
        conf2 = SpatialSearchConfig(
            algorithm="s2", maxDistance=100, numNearestNeighbors=5)
        self.assertEqual(str(conf2), "numNearestNeighbors5_maxDist100")
        conf3 = SpatialSearchConfig(
            algorithm="s2", maxDistance=None, numNearestNeighbors=5)
        self.assertEqual(str(conf3), "numNearestNeighbors5")
        conf4 = SpatialSearchConfig(
            algorithm="baseline", maxDistance=100, numNearestNeighbors=None)
        self.assertEqual(str(conf4), "maxDist100")

        # Compose
        out = conf2.compose(
            "?left", "?right", "?dist", [
                "?payloada",
                "?payloadb"
            ],
            "BODY")
        expected = """
        SERVICE spatialSearch: {
            _:config spatialSearch:algorithm spatialSearch:s2 ;
                     spatialSearch:left ?left ;
                     spatialSearch:right ?right ;
                     spatialSearch:numNearestNeighbors 5 ;
                     spatialSearch:maxDistance 100 ;
                     spatialSearch:payload ?payloada, ?payloadb ;
                     spatialSearch:bindDistance ?dist .
            {
                BODY
            }
        }
        """

        self.assertSparqlEq(out, expected)

    def test_right_shard(self):
        # Invariants
        with self.assertRaises(AssertionError):
            RightShard("", ["?x"])
        with self.assertRaises(AssertionError):
            RightShard("x", [""])
        with self.assertRaises(AssertionError):
            RightShard("x", ["?x", "?y", "z"])

        # Methods / Valid Objects
        r = RightShard("xyz.rq",
                       ["?x", "<all>", "spatialSearch:all"])
        self.assertListEqual(r.payload, ["spatialSearch:all"])
        self.assertEqual(r.compose_payload(), "spatialSearch:all")
        self.assertTrue(r.includes_variable("?x"))
        self.assertTrue(r.includes_variable("?a"))

        r1 = RightShard("xyz.rq",
                        ["?x", "<all>", "?z"])
        self.assertListEqual(r1.payload, ["spatialSearch:all"])
        self.assertTrue(r1.includes_variable("?x"))
        self.assertTrue(r1.includes_variable("?y"))

        r2 = RightShard("xyz.rq",
                        ["?x", "?y", "?z"])
        self.assertListEqual(r2.payload, ["?x", "?y", "?z"])
        self.assertEqual(r2.compose_payload(), "?x, ?y, ?z")
        self.assertTrue(r2.includes_variable("?x"))
        self.assertTrue(r2.includes_variable("?y"))
        self.assertTrue(r2.includes_variable("?z"))
        self.assertFalse(r2.includes_variable("?a"))

    def test_spatial_search(self):
        pth = PROGRAM_DIR / "test" / "test_compose.json"
        with open(pth, "r") as f:
            config = get(json.load(f), "spatial_searches")[0]
        s = SpatialSearch.from_config(config)

        # Invariants
        with self.assertRaises(KeyError):
            _c = config.copy()
            del _c["config"]
            SpatialSearch.from_config(_c)
        with self.assertRaises(AssertionError):
            _c = config.copy()
            _c["right"] = []
            SpatialSearch.from_config(_c)
        with self.assertRaises(AssertionError):
            _c = config.copy()
            _c["left"] = []
            SpatialSearch.from_config(_c)
        with self.assertRaises(KeyError):
            _c = config.copy()
            _c["add_selectors"] = {}
            SpatialSearch.from_config(_c)
        with self.assertRaises(KeyError):
            _c = config.copy()
            _c["add_selectors"] = {"selectors": ["?x"]}
            SpatialSearch.from_config(_c)
        with self.assertRaises(AssertionError):
            _c = config.copy()
            _c["add_selectors"]["selectors"] = ["", ","]
            SpatialSearch.from_config(_c)

        self.assertEqual(s.apply_name_template("left", "right"), "left_right")

        #
        qname, query, select = s.compose_single(
            {
                "left.rq": """
                ?left <p> <o> .
                """,
                "right.rq": """
                ?right <p> <o> .
                """,
            }, "left.rq", RightShard("right.rq", ["?count_right"])
        )
        self.assertEqual(qname, "left_right")
        self.assertSparqlEq(select, "?right_centroid ?dist_left_right")
        self.assertSparqlEq(query, """
            SERVICE spatialSearch: {
                _:config spatialSearch:algorithm spatialSearch:s2 ;
                        spatialSearch:left ?left_centroid ;
                        spatialSearch:right ?right_centroid ;
                        spatialSearch:numNearestNeighbors 2 ;
                        spatialSearch:maxDistance 700 ;
                        spatialSearch:payload ?count_right ;
                        spatialSearch:bindDistance ?dist_left_right .
                {
                    ?right <p> <o> .
                    ?right geo:hasCentroid/geo:asWKT ?right_centroid .
                    BIND(COUNT(*) AS ?count_right)
                }
            }
        """)

        #
        qname, query = s.compose_left({
            "left.rq": "?left <p> <o> ."
        }, "left.rq")
        self.assertEqual(qname, "left_")
        self.assertSparqlEq(query, """
        {
            ?left <p> <o> .
            ?left geo:hasCentroid/geo:asWKT ?left_centroid .
            BIND(COUNT(*) AS ?count_left)
        }
        """)

        #
        pattern, query = s.compose({
            "station.rq": "?station <p> <o> .",
            "restaurant.rq": """
            ?restaurant <p> <o> .
            ?restaurant <n> ?restaurant_name .
            """,
            "group_template.rq": """
            {
                SELECT
                    %SELECT%
                WHERE {
                    %QUERIES%
                }
            }
            """
        })
        self.assertEqual(pattern, s.template_pattern)
        self.assertSparqlEq(query, """
        {
        {
            SELECT
                ?restaurant_centroid
                ?dist_station_restaurant
            WHERE {

            {
                ?station <p> <o> .
                ?station geo:hasCentroid/geo:asWKT ?station_centroid .
                BIND(COUNT(*) AS ?count_station)
            }
            SERVICE spatialSearch: {
                _:config spatialSearch:algorithm spatialSearch:s2 ;
                        spatialSearch:left ?station_centroid ;
                        spatialSearch:right ?restaurant_centroid ;
                        spatialSearch:numNearestNeighbors 2 ;
                        spatialSearch:maxDistance 700 ;
                        spatialSearch:payload ?restaurant_name ;
                        spatialSearch:bindDistance ?dist_station_restaurant .
                {
                ?restaurant <p> <o> .
                ?restaurant <n> ?restaurant_name .
                ?restaurant geo:hasCentroid/geo:asWKT ?restaurant_centroid .
                }
            }
            }
        }
        }
        """)

    def test_provided_values(self):
        # Invariants
        with self.assertRaises(AssertionError):
            ProvidedValues("xyz", ["q:abc", "q:xyz"])
        with self.assertRaises(AssertionError):
            ProvidedValues("?xyz--.1", ["q:abc", "q:xyz"])
        with self.assertRaises(AssertionError):
            ProvidedValues("?xy13*", ["q:abc", "q:xyz"])
        with self.assertRaises(AssertionError):
            ProvidedValues("?xyz", [])
        ProvidedValues("?xyz1", ["q:abc", "q:xyz"])
        ProvidedValues("?xyz123_1_abc", ["q:abc", "q:xyz"])

        # Compose
        p = ProvidedValues("?xyz", ["q:abc", "q:xyz"])
        out = p.compose("""
        ?a <pred> ?b .
        ?b <pred2> ?xyz .
        """, "pref")
        expected = """
            VALUES ?pref_xyz_ {
                q:abc
                q:xyz
            }
            ?a <pred> ?b .
            ?b <pred2> ?pref_xyz_ .
        """
        self.assertSparqlEq(out, expected)

        inp = """
        ?a <pred> ?b .
        ?b <pred2> ?xz .
        """
        out = p.compose(inp, "pref")
        self.assertSparqlEq(out, inp)

    def test_group_template(self):
        # Invariants
        with self.assertRaises(AssertionError):
            GroupTemplate("", "abc", "xyz")
        with self.assertRaises(AssertionError):
            GroupTemplate("abc", "", "xyz")
        with self.assertRaises(AssertionError):
            GroupTemplate("abc", "xyz", "")
        with self.assertRaises(AssertionError):
            GroupTemplate("abc", "abc", "abc")
        GroupTemplate("abc", "abc", "xyz")

        # Compose
        gt = GroupTemplate(filename="gt.rq",
                           queries_pattern="%QUERIES%",
                           select_pattern="%SELECT%")
        with self.assertRaises(AssertionError):
            gt.compose({}, [("abc", "q:abc")], ["?abc"])

        out = gt.compose({
            "gt.rq": """
                SELECT %SELECT% WHERE {
                    %QUERIES%
                }
                """
        }, [
            ("abc", "q:abc"),
            ("xyz", "q:xyz")
        ], [
            "?abc"
        ])

        expected = """
            SELECT ?abc WHERE {
            # --- Begin of `abc`
            q:abc
            # --- End of `abc`

            # --- Begin of `xyz`
            q:xyz
            # --- End of `xyz`
            }
        """

        self.assertSparqlEq(out, expected)
        self.assertIn("# --- Begin of `abc`", out)
        self.assertIn("# --- Begin of `xyz`", out)
        self.assertIn("# --- End of `abc`", out)
        self.assertIn("# --- End of `xyz`", out)


class TestUtils(ComposeSpatialTestCase):
    def test_input_loaders(self):
        # Zip + Dir
        self.assertEqual(
            get_file_contents(str(PROGRAM_DIR / "test"), "basic.csv"),
            "a,b,c\n1,k,l\n-4.5,m,01.01.2024\n")
        tc = ""
        with open(PROGRAM_DIR / "test" / "test_compose.json", "r") as f:
            tc = f.read()
        self.assertEqual(
            get_file_contents(
                str(PROGRAM_DIR / "test" / "test_for_compose.zip"),
                "test_compose.json"), tc)

        conf, files = get_config_and_queries(
            str(PROGRAM_DIR / "test"), "test_compose.json")
        for f in ["template.rq", "test.rq", "station.rq",
                  "restaurant.rq", "group_template.rq"]:
            self.assertIn(f, files)
        self.assertDictEqual(conf, json.loads(tc))

        self.assertListEqual(
            get_all_configs(str(PROGRAM_DIR / "test")),
            ["test_compose.json"])
        self.assertListEqual(
            get_all_configs(
                str(PROGRAM_DIR / "test" / "test_for_compose.zip")),
            ["test_compose.json"])

    def test_guess_type(self):
        self.assertEqual(guess_type("lol.lol.rq"), Mime.SPARQL)
        self.assertEqual(guess_type("example.sparql"), Mime.SPARQL)
        self.assertEqual(guess_type("example.txt"), Mime.PLAIN)
        self.assertEqual(guess_type("lol.lol"), Mime.PLAIN)
        self.assertEqual(guess_type("mystyle.css"), Mime.CSS)
        self.assertEqual(guess_type("script.js"), Mime.JS)
        self.assertEqual(guess_type("index.html"), Mime.HTML)
        self.assertEqual(guess_type("something.htm"), Mime.HTML)
        self.assertEqual(guess_type("something.json"), Mime.JSON)

    def test_group(self):
        a = [1, 2, 3, 4, 5]
        b = a + [6]
        c = ["1", "2", "3"]
        self.assertListEqual(group(a, 4), [[1, 2, 3, 4], [5]])
        self.assertListEqual(group(b, 4), [[1, 2, 3, 4], [5, 6]])
        self.assertListEqual(group(b, 2), [[1, 2], [3, 4], [5, 6]])
        self.assertListEqual(group(b, 3), [[1, 2, 3], [4, 5, 6]])
        self.assertListEqual(group(c, 2), [["1", "2"], ["3"]])
        self.assertListEqual(group(c, 1), [["1"], ["2"], ["3"]])
        self.assertListEqual(group(c, 10), [["1", "2", "3"]])
        self.assertListEqual(group([], 10), [])

    def test_clean_name(self):
        self.assertEqual(clean_name("example.rq"), "example")
        self.assertEqual(clean_name("example"), "example")
        self.assertEqual(clean_name("example.sparql"), "example")
        self.assertEqual(clean_name("example.sparql.txt"),
                         "example.sparql.txt")
        self.assertEqual(clean_name("e.x.a.m.-ple.rq"), "e.x.a.m.-ple")
        self.assertEqual(clean_name("exa.rq.mple.sparql"), "exa.rq.mple")
        self.assertEqual(clean_name(""), "")

    def test_get(self):
        nested = {
            "a": {
                "b": {
                    "c": {
                        "d": 123
                    }
                },
                "x": {
                    "y": "z"
                }
            }
        }
        self.assertEqual(get(nested, "a", "b", "c"), {"d": 123})
        self.assertEqual(get(nested, "a", "b", "c", "d"), 123)
        self.assertEqual(get(nested, "a", "x", "y"), "z")
        self.assertIsNone(get(nested, "x", "y"))
        self.assertIsNone(get(nested, "a", "x", "y", "z"))
        self.assertIsNone(get(nested, ""))
        self.assertEqual(get(nested), nested)
        self.assertIsNone(get(nested, "..."))
        self.assertIsNone(get({}, "a", "x", "y", "z"))
        self.assertIsNone(get({1: 2}, "a", "x", "y", "z"))
        self.assertIsNone(get(None, "a"))  # type: ignore

    def test_indent(self):
        example1 = """
            PREFIX abc: <http://example.com>

            SELECT ?a ?b WHERE {
            ?a rdf:label ?b . # Get all subjects with their labels
              # Some filter...
            FILTER(REGEX("...", ?a))
            }
        """
        self.assertSparqlEq(example1, indent(example1))

        example2 = """


        Prefix abc: <abc>
        Select * { ab cd ef . }
        """
        self.assertSparqlEq(example2, indent(example2))

        example3 = """
            PREFIX abc: <http://example.com>

            SELECT
                ?a
                ?b

                WHERE {
            ?a rdf:label ?b .


                  FILTER(REGEX("...", ?a))
            }
        """
        self.assertSparqlEq(example3, indent(example3))

        example4 = """
            PREFIX abc: <http://example.com>

            SELECT * WHERE {
            ?a rdf:label ?b ;
            xyz ?c ;
            abc ?x .
            }
        """
        self.assertSparqlEq(example4, indent(example4))


class SupportedRequestMethod(Enum):
    GET = "GET"
    POST = "POST"


class TestServer(ComposeSpatialTestCase):
    def get_exp_file(self, fn: str) -> str:
        with open(Path(PROGRAM_DIR / "test" / fn), "r") as f:
            return f.read()

    @patch("compose_spatial.logger.info", lambda *_: None)
    def make_server(self, main="test_compose.json", serve="test_serve.json"):
        return ComposeSpatialHTTPServer(str(Path(
            PROGRAM_DIR / "test")), main, serve)

    def setUp(self):
        self.rh_sock = socket()
        self.server = self.make_server()
        self.server2 = self.make_server("test_compose_copy.json",
                                        "test_serve2.json")
        super().setUp()

    def tearDown(self):
        self.rh_sock.close()
        self.server.socket.close()
        self.server.server_close()
        self.server2.socket.close()
        self.server2.server_close()

    @patch("compose_spatial.logger.info", lambda *_: None)
    def helper_req_handler(self,
                           method: SupportedRequestMethod,
                           path: str, req_body: str,
                           req_headers: dict[str, str],
                           res_mime: str, res_body: Optional[str],
                           res_status: int) \
            -> tuple[dict[str, str], str]:
        # Fake all the network functions using patch
        with patch('http.server.BaseHTTPRequestHandler.handle_one_request'), \
                patch('http.server.BaseHTTPRequestHandler.end_headers'), \
                patch('http.server.BaseHTTPRequestHandler.send_response') \
                as mock_send_response, \
                patch('http.server.BaseHTTPRequestHandler.send_header') \
                as mock_send_header:

            # Construct a fake request
            rh = ComposeSpatialHTTPRequestHandler(
                self.rh_sock, ("123.123.123.123", 1234), self.server)
            rh.command = method.value
            rh.path = path

            # Add headers from dictionary
            rh.headers = Message()
            for key, val in req_headers.items():
                rh.headers.add_header(key, val)
            rh.headers.add_header("Content-length", str(len(req_body)))

            # Fake communication channels
            rh.rfile = BytesIO()
            rh.wfile = BytesIO()
            rh.rfile.write(req_body.encode("utf-8"))
            rh.rfile.seek(0)

            # Call the requested method
            if method == SupportedRequestMethod.GET:
                rh.do_GET()
            else:
                assert method == SupportedRequestMethod.POST
                rh.do_POST()
            mock_send_response.assert_called_once_with(res_status)
            self.assertGreaterEqual(mock_send_header.call_count, 2)

            actual_res_headers = {
                _call.args[0].lower(): _call.args[1]
                for _call in mock_send_header.call_args_list
                if len(_call.args) == 2 and type(_call.args[0]) is str
                and type(_call.args[1]) is str
            }

            if res_body is not None:
                self.assertDictEqual(actual_res_headers, {
                    'content-type': res_mime,
                    'content-length': str(len(res_body))
                })
            else:
                self.assertDictEqual(
                    # because of deprecated assertDictContainsSubset
                    actual_res_headers | {'content-type': res_mime},
                    actual_res_headers)

                # If we are not matching for a hard-coded response body,
                # we do not know the content-length. Therefore we only test
                # that the header is present
                self.assertIn("content-length", actual_res_headers)

            rh.rfile.seek(0)
            self.assertEqual(rh.rfile.read().decode("utf-8"), req_body)
            rh.wfile.seek(0)
            actual_res_body = rh.wfile.read()
            if res_body is not None:
                self.assertEqual(actual_res_body.decode("utf-8"), res_body)
            rh.rfile.close()
            rh.wfile.close()

            # Correctness of content-length
            self.assertEqual(int(actual_res_headers["content-length"]),
                             len(actual_res_body))

            # Return the headers and body for further tests
            return (
                actual_res_headers,
                actual_res_body.decode("utf-8")
            )

    @patch("compose_spatial.logger.info", lambda *_: None)
    def test_server(self):
        h = self.server
        self.assertTrue(h.verbose)
        self.assertTupleEqual(h.bind_address, ("0.0.0.0", 7991))
        self.assertFalse(h.knows("x"))
        self.assertTrue(h.knows("test_compose.json"))
        self.assertFalse(h.knows("test_compose_copy.json"))
        self.assertTrue(h.knows("test.rq"))
        self.assertEqual(h.get("restaurant.rq"),
                         self.get_exp_file("restaurant.rq"))
        # Prevent address already in use when opening second server

        h2 = self.server2
        self.assertTrue(h2.knows("test_compose_copy.json"))
        self.assertTrue(h2.knows("test_compose.json"))
        self.assertEqual(h2.get("test_compose_copy.json"),
                         self.get_exp_file("test_compose_copy.json"))

    @patch("compose_spatial.logger.info", lambda *_: None)
    def test_request_handler_get(self):
        exp = self.get_exp_file("restaurant.rq")
        self.helper_req_handler(SupportedRequestMethod.GET, "/restaurant.rq",
                                "", {}, "application/sparql-query", exp, 200)
        self.helper_req_handler(SupportedRequestMethod.GET,
                                "/does-not-exist.rq",
                                "", {}, "text/plain", "Not Found", 404)

        with open(PROGRAM_DIR / "compose" / "index.html", "r") as f:
            exp2 = f.read()
        self.helper_req_handler(SupportedRequestMethod.GET, "/",
                                "", {}, "text/html", exp2, 200)
        self.helper_req_handler(SupportedRequestMethod.GET, "/index.html",
                                "", {}, "text/html", exp2, 200)

        self.helper_req_handler(SupportedRequestMethod.GET,
                                "/blank_compose.json",
                                "", {}, "application/json", "{}", 200)

        with open(PROGRAM_DIR / "compose" / "main.js", "r") as f:
            exp3 = f.read()
        self.helper_req_handler(SupportedRequestMethod.GET, "/main.js",
                                "", {}, "application/javascript", exp3, 200)

        with open(PROGRAM_DIR / "compose" / "style.css", "r") as f:
            exp4 = f.read()
        self.helper_req_handler(SupportedRequestMethod.GET, "/style.css",
                                "", {}, "text/css", exp4, 200)

    @patch("compose_spatial.logger.info", lambda *_: None)
    def test_request_handler_post(self):
        self.helper_req_handler(SupportedRequestMethod.POST, "/index.html",
                                "<h1>Something</h1>",
                                {"Content-type": "text/html"},
                                "text/plain", "Method not allowed", 405)

        error400 = "Expected HTTP headers 'Content-type: application/json'" + \
            " and 'Accept: application/sparql-query' not found."
        self.helper_req_handler(SupportedRequestMethod.POST, "/compose",
                                "{}",
                                {"Content-type": "text/html"},
                                "text/plain", error400, 400)

        self.helper_req_handler(SupportedRequestMethod.POST, "/compose",
                                "{}",
                                {"Content-type": "application/sparql-query"},
                                "text/plain", error400, 400)

        self.helper_req_handler(SupportedRequestMethod.POST, "/compose",
                                "{}",
                                {
                                    "Content-type": "application/json",
                                    "Accept": "text/plain"
                                },
                                "text/plain", error400, 400)

        post_header = {
            "Content-type": "application/json",
            "Accept": "application/sparql-query"
        }

        with self.assertLogs("compose_spatial", level="ERROR"):
            _, res_body = self.helper_req_handler(
                SupportedRequestMethod.POST, "/compose", "{}", post_header,
                "text/plain", None, 500)
            self.assertIn("Error", res_body)

        with self.assertLogs("compose_spatial", level="ERROR"):
            _, res_body = self.helper_req_handler(
                SupportedRequestMethod.POST, "/compose", """
                {
                    "template": 123,
                    "spatial_searches": []
                }
                """, post_header,
                "text/plain", None, 500)
            self.assertIn("Error", res_body)

        with open(PROGRAM_DIR / "test" / "test_compose.json", "r") as f:
            req_body = f.read()
        # Example query is empty because template is empty...
        _, res_body = self.helper_req_handler(
            SupportedRequestMethod.POST, "/compose", req_body, post_header,
            "application/sparql-query", "\n", 200)


class TestRunAsProgram(unittest.TestCase):
    def test_main(self):
        with patch("compose_spatial.cli_main") as mock_cli_main:
            main([
                "-c", "test_compose.json",
                "-o", "output.rq",
                "config.zip"
            ])
            mock_cli_main.assert_called_once_with(
                "config.zip", "test_compose.json", "output.rq")

        with patch("compose_spatial.serve_main") as mock_serve_main:
            main([
                "-c", "test_compose.json",
                "-s", "test_serve.json",
                "config.zip"
            ])
            mock_serve_main.assert_called_once_with(
                "config.zip", "test_compose.json", "test_serve.json")

    def test_cli(self):
        # Output to stdout
        with patch("builtins.print") as mock_print:
            cli_main(str(PROGRAM_DIR / "test"),
                     "test_compose.json", None)
            mock_print.assert_called_once_with("\n", end="")

        # Output to file
        with patch("builtins.open", mock_open()) as mock_file, \
                patch("compose_spatial.get_config_and_queries") as mock_get, \
                patch("compose_spatial.QueryConfig.from_config"):
            mock_get.return_value = ({}, {})
            cli_main(str(PROGRAM_DIR / "test"),
                     "test_compose.json", "output.rq")
            mock_file.assert_called_once_with("output.rq", "w")

    def test_server(self):
        constructorargs: tuple[Any, ...] = ()
        serve_counter = 0

        class FakeServer:
            def __init__(self, *args):
                nonlocal constructorargs
                assert constructorargs == ()
                constructorargs = args

            def serve_forever(self):
                nonlocal serve_counter
                serve_counter += 1

        args = (str(PROGRAM_DIR / "test"),
                "test_compose.json", "test_serve.json")

        with patch("compose_spatial.ComposeSpatialHTTPServer", FakeServer):
            serve_main(*args)

        self.assertTupleEqual(constructorargs, args)
        self.assertEqual(serve_counter, 1)


if __name__ == '__main__':
    unittest.main()
