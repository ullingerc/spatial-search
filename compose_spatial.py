#!/bin/env python3
"""
Automatically compose large spatial search SPARQL queries for QLever

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
import json
import argparse
import logging
from pathlib import Path
from zipfile import ZipFile
from typing import Any, Iterator, Literal, Optional, TypedDict, NotRequired, \
    Sequence
from enum import Enum
from dataclasses import dataclass
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse


PROGRAM_DESCRIPTION = """
    Automatically compose large spatial search SPARQL queries for QLever.
"""

MAX_REPLACE_DEPTH = 100
CONFIG_FILENAME_SUFFIX = "_compose.json"
CONFIG_FILENAME_GLOB = f"**/*{CONFIG_FILENAME_SUFFIX}"
QLEVER_SUPPORTED_ALGORITHMS = {
    # Algorithm IRI w/o prefix => Description
    "baseline": "Nested-Loop Baseline Algorithm",
    "s2": "Fast S2-PointIndex-Based Algorithm",
    "boundingBox": "Rtree-Index-Based Algorithm"
}
QLEVER_DEFAULT_ALGORITHM = "s2"
SPATIAL_SEARCH = "spatialSearch:"
SPATIAL_SEARCH_ALL = SPATIAL_SEARCH + "all"


class Mime(Enum):
    "Mime types"
    JSON = "application/json"
    SPARQL = "application/sparql-query"
    PLAIN = "text/plain"
    HTML = "text/html"
    JS = "application/javascript"
    CSS = "text/css"


# Structure of the configuration dictionary
GroupTemplatePatternsDict = TypedDict("GroupTemplatePatternsDict", {
    "queries": str,
    "select": str
})
GroupTemplateDict = TypedDict("GroupTemplateDict", {
    "filename": str,
    "patterns": GroupTemplatePatternsDict
})
ProvidedValuesDict = TypedDict("ProvidedValuesDict", {
    "variable": str,
    "values": list[str]
})
SpatialSearchConfigDict = TypedDict("SpatialSearchConfigDict", {
    "algorithm": Literal["s2"] | Literal["baseline"] | Literal["boundingBox"],
    "maxDistance": int | str,
    "numNearestNeighbors": int | str
})
NameTemplatePatternsDict = TypedDict("NameTemplatePatternsDict", {
    "type": str,
    "left": str,
    "right": str
})
NameTemplateDict = TypedDict("NameTemplateDict", {
    "template": str,
    "patterns": NameTemplatePatternsDict
})
SelectorPatternsDict = TypedDict("SelectorPatternsDict", {
    "dist": str,
    "count": str,
    "centroid": str
})
SelectorsDict = TypedDict("SelectorsDict", {
    "selectors": list[str],
    "patterns": SelectorPatternsDict
})
RightShardDict = TypedDict("RightShardDict", {
    "filename": str,
    "payload": NotRequired[list[str]]
})
SpatialSearchDict = TypedDict("SpatialSearchDict", {
    "config": SpatialSearchConfigDict,
    "left": list[str],
    "right": list[RightShardDict],
    "group_template": GroupTemplateDict,
    "template_pattern": str,
    "group_size": NotRequired[Optional[int]],
    "provided_values": NotRequired[list[ProvidedValuesDict]],
    "name_template": NameTemplateDict,
    "add_selectors": SelectorsDict
})
ReplaceRuleDict = TypedDict("ReplaceRuleDict", {
    "search": str,
    "replace": NotRequired[str],
    "replace_file": NotRequired[str]
})
TemplateDict = TypedDict("TemplateDict", {
    "filename": str,
    "replace": NotRequired[list[ReplaceRuleDict]]
})
QueryConfigDict = TypedDict("QueryConfigDict", {
    "template": TemplateDict,
    "spatial_searches": list[SpatialSearchDict]
})
FilesCache = dict[str, str]

# Structure of the integrated HTTP server's configuration dict
ServeDict = TypedDict("ServeDict", {
    "address": str,
    "port": int,
    "verbose": bool,
    "templates": list[str],
    "group_templates": list[str],
    "shards": list[str],
    "replace_files": list[str],
    "default_left": str,
    "default_right": str
})

# Precompiled regular expressions
FILE_EXTENSION_RE = re.compile(r'^.*\.(?P<ext>\w{2,6})$')
SPARQL_FILE_EXT_RE = re.compile(r'\.(rq|sparql)$')
SPARQL_VAR_RE = re.compile(r'^\?\w+$')


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def guess_type(pth: str | Path) -> Mime:
    """
    Given a file path guess the mime type based on its suffix
    """
    m = FILE_EXTENSION_RE.match(str(pth))
    if m:
        match m.group("ext"):
            case "html" | "htm":
                return Mime.HTML
            case "json":
                return Mime.JSON
            case "css":
                return Mime.CSS
            case "js":
                return Mime.JS
            case "rq" | "sparql":
                return Mime.SPARQL
    return Mime.PLAIN


def get(d: dict[Any, Any], *keys: str) -> Any:
    """
    Shorthand to get values from nested dicts
    """
    if type(d) is not dict:
        return None
    if keys == ():
        return d
    for key in keys[:-1]:
        d = d.get(key, {})
        if type(d) is not dict:
            return None
    return d.get(keys[-1], None)


def clean_name(name: str) -> str:
    """
    Remove sparql file name extension
    """
    return SPARQL_FILE_EXT_RE.sub("", Path(name).name)


def group(items: list[Any], n: int) -> list[list[Any]]:
    """
    Split a list into a list of sublists with each at most n elements
    """
    # Typeannotation should use type variables, but these would exclude some
    # otherwise supported python versions...
    return [
        items[i:i+n]
        for i in range(0, len(items), n)
    ]


def indent(text: str) -> str:
    """
    Helper: given a SPARQL query, add whitespace to make it more readable.
    This is not a universally applicable implementation.
    """
    out = ""
    depth = 0
    blank = False
    indent_by_semicolon = 0
    for line in text.splitlines():
        line = line.strip()

        # Reduce multiple blank lines to one
        prev, blank = blank, not bool(line)
        if prev and blank:
            continue

        if not line.startswith("#"):
            if line.endswith("}"):
                depth -= 2
            if "WHERE" in line and "SELECT" not in line:
                depth -= 8

        out += (" " * depth) + line + "\n"

        if not line.startswith("#"):
            if line.endswith("{"):
                depth += 2
            if "SELECT" in line and "WHERE" not in line:
                depth += 8

            if indent_by_semicolon and line.endswith("."):
                depth -= indent_by_semicolon
                indent_by_semicolon = 0
            elif line.endswith(";") and not indent_by_semicolon:
                indent_by_semicolon = len(line.split()[0]) + 1
                depth += indent_by_semicolon

    return out


def get_file_contents(filename: str, sub_filename: str) -> str:
    """
    Retrieve the content of an input file contained in the main input
    directory. This can be a filesystem directory or a zipped one.
    """

    pth = Path(filename)
    assert pth.exists()

    if pth.is_dir():
        # Treat as directory
        with open(Path(pth, sub_filename), "r") as f:
            return f.read()
    else:
        # Treat as ZIP file
        with ZipFile(pth, "r") as zf:
            with zf.open(sub_filename, "r") as f:
                return f.read().decode("utf-8")


def get_all_configs(filename: str) -> list[str]:
    """
    Retrieve the list of configuration files contained in the main input
    directory. This can be a filesystem directory or a zipped one.
    """

    pth = Path(filename)
    assert pth.exists()

    if pth.is_dir():
        # Treat as directory
        return [str(fn.relative_to(pth))
                for fn in pth.rglob(CONFIG_FILENAME_GLOB)]
    else:
        # Treat as ZIP file
        with ZipFile(pth, "r") as zf:
            return [fn for fn in zf.namelist()
                    if fn.endswith(CONFIG_FILENAME_SUFFIX)]


def get_config_and_queries(filename: str, main_config: str) \
        -> tuple[QueryConfigDict, FilesCache]:
    """
    Loads all the required files defined by the given config file inside the
    given input directory into memory. The input directory can either be on the
    filesystem or a zip file.
    """
    assert Path(filename).exists()

    # Any type because the config is not yet constructed
    config: Any = {}

    files: FilesCache = {}

    def required_files_from_config() -> set[str]:
        def helper() -> Iterator[str]:
            """
            Helper that generates filenames to be loaded
            """

            # Template file
            yield get(config, "template", "filename")

            # Replace files
            t = get(config, "template", "replace")
            if t:
                for t_ in t:
                    rf = get(t_, "replace_file")
                    if rf:
                        yield rf

            # Files for query construction
            s = get(config, "spatial_searches")
            for _s in s:
                yield get(_s, "group_template", "filename")
                yield from get(_s, "left")
                for r in get(_s, "right"):
                    yield r["filename"]

        # Make list of required files unique
        return set(helper())

    config = json.loads(get_file_contents(filename, main_config))
    for sub_filename in required_files_from_config():
        files[sub_filename] = get_file_contents(filename, sub_filename)

    return (config, files)


@dataclass
class GroupTemplate:
    filename: str
    queries_pattern: str
    select_pattern: str

    def __post_init__(self):
        assert self.filename, "Group template needs a file"
        assert self.queries_pattern, \
            "Group template needs a queries pattern"
        assert self.select_pattern, \
            "Group template needs a select pattern"
        assert self.queries_pattern != self.select_pattern, \
            "Group template patterns need to be different"

    @staticmethod
    def from_config(d: GroupTemplateDict) -> 'GroupTemplate':
        return GroupTemplate(
            d["filename"],
            d["patterns"]["queries"],
            d["patterns"]["select"])

    def compose(self, files: dict[str, str],
                queries: list[tuple[str, str]], select: list[str]) -> str:
        select_str = "\n".join(set(s.strip() for s in select))
        queries_str = ""
        for qname, query in queries:
            queries_str = queries_str + "\n" + \
                "# --- Begin of `" + qname + "`\n" + query + \
                "\n# --- End of `" + qname + "`\n"
        assert self.filename in files, \
            f"Group template '{self.filename}' is missing"
        return files[self.filename] \
            .replace(self.queries_pattern, queries_str) \
            .replace(self.select_pattern, select_str)


@dataclass
class ProvidedValues:
    variable: str
    values: list[str]

    def __post_init__(self):
        assert re.match(
            r'^\?\w+$', self.variable), "Variable for values must begin " + \
            "with '?' and contain only alphanumeric chars and underscores"
        assert len(self.values), "Provided values needs at least one value"

    @staticmethod
    def from_config(d: ProvidedValuesDict) -> 'ProvidedValues':
        return ProvidedValues(
            d["variable"],
            d["values"])

    def compose(self, query: str, var_prefix: str) -> str:
        if self.variable not in query:
            return query

        new_variable = f"?{var_prefix}_{self.variable[1:]}_"
        values_content = '\n'.join(self.values)
        return f"""
            # This VALUES list must be declared each time for technical reasons
            VALUES {new_variable} {{
                {values_content}
            }}

            {query.replace(self.variable, new_variable)}
        """


@dataclass
class SpatialSearchConfig:
    algorithm: str
    maxDistance: Optional[int]
    numNearestNeighbors: Optional[int]
    # left, right, bindDistance is handled / internally managed by
    # 'SpatialSearch'

    def __post_init__(self):
        assert self.maxDistance or self.numNearestNeighbors, \
            "At least one of both (maxDistance or numNearestNeighbors) " + \
            "must be provided for spatial search config."
        assert (not self.maxDistance) or self.maxDistance > 0, \
            "maxDistance must be > 0 if given"
        assert (not self.numNearestNeighbors) or \
            self.numNearestNeighbors > 0, \
            "numNearestNeighbors must be > 0 if given"
        assert self.algorithm in QLEVER_SUPPORTED_ALGORITHMS, \
            f"Unsupported spatial search algorithm '{self.algorithm}'"

    def __str__(self) -> str:
        out = ""
        if self.numNearestNeighbors:
            out += f"numNearestNeighbors{self.numNearestNeighbors}"
        if self.maxDistance:
            if out:
                out += "_"
            out += f"maxDist{self.maxDistance}"
        return out

    @staticmethod
    def from_config(d: SpatialSearchConfigDict) -> 'SpatialSearchConfig':
        maxDistance, numNearestNeighbors = None, None
        if "maxDistance" in d and d["maxDistance"]:
            maxDistance = int(d["maxDistance"])
        if "numNearestNeighbors" in d and d["numNearestNeighbors"]:
            numNearestNeighbors = int(d["numNearestNeighbors"])
        algorithm = d.get("algorithm", "s2")
        return SpatialSearchConfig(
            algorithm,
            maxDistance,
            numNearestNeighbors
        )

    def compose(self, left: str, right: str, bind: str, payload: list[str],
                body: str) -> str:
        for x in (left, right, bind):
            assert x.startswith("?"), \
                f"Variables must begin with '?' but '{x}' given"

        config: dict[str, str] = {
            "algorithm": SPATIAL_SEARCH + self.algorithm,
            "left": left,
            "right": right
        }

        if self.numNearestNeighbors:
            config["numNearestNeighbors"] = str(self.numNearestNeighbors)
        if self.maxDistance:
            config["maxDistance"] = str(self.maxDistance)
        if payload:
            config["payload"] = ", ".join(payload)
        assert self.numNearestNeighbors or self.maxDistance, \
            "Incomplete spatial search configuration"

        config["bindDistance"] = bind

        config_pairs = ' ;\n'.join(
            f"{SPATIAL_SEARCH}{key} {val}" for key, val in config.items()
        ) + " ."
        return f"""
        SERVICE {SPATIAL_SEARCH} {{
            _:config {config_pairs}
            {{
                {body}
            }}
        }}
        """


@dataclass
class RightShard:
    filename: str
    payload: list[str]

    def __post_init__(self):
        assert self.filename, \
            "Filename of right shard may not be empty"
        seen_all = False
        for p in self.payload:
            assert p, "No empty payload values may be given"
            if p == "<all>" or p == SPATIAL_SEARCH_ALL:
                seen_all = True
            else:
                assert re.match(r'^\?\w+$', p), \
                    f"Invalid payload value '{p}' ('<all>', " + \
                    f"'{SPATIAL_SEARCH_ALL}' or a variable expected)"
        if seen_all:
            # Clean up
            self.payload = [SPATIAL_SEARCH_ALL]

    @staticmethod
    def from_config(d: RightShardDict) -> 'RightShard':
        return RightShard(d["filename"], d.get("payload") or [])

    def compose_payload(self) -> str:
        return ", ".join(self.payload)

    def includes_variable(self, var: str) -> bool:
        return var in self.payload or self.payload == [SPATIAL_SEARCH_ALL]


@dataclass
class SpatialSearch:
    config: SpatialSearchConfig
    left: list[str]
    right: list[RightShard]
    template_pattern: str
    group_template: GroupTemplate
    group_size: Optional[int]
    add_selectors: list[str]
    dist_pattern: str
    count_pattern: str
    centroid_pattern: str
    provided_values: list[ProvidedValues]
    name_template: str
    name_type_pattern: str
    name_left_pattern: str
    name_right_pattern: str

    def __post_init__(self):
        # Sanity checks
        assert self.config, "Spatial search needs a parameter config"

        assert len(self.right), "A least one right query is required"
        assert len(self.left), "A least one left query is required"
        assert len(self.add_selectors), "A least one selector is required"

        for s in self.add_selectors:
            assert s, f"Invalid or empty user selector '{s}'"

    @staticmethod
    def from_config(d: SpatialSearchDict) -> 'SpatialSearch':
        ap = d["add_selectors"]["patterns"]
        np = d["name_template"]["patterns"]
        sel = d["add_selectors"]["selectors"]
        pvs = [ProvidedValues.from_config(pv)
               for pv in d.get("provided_values", [])]
        gt = GroupTemplate.from_config(d["group_template"])

        return SpatialSearch(
            SpatialSearchConfig.from_config(d["config"]),
            d["left"],
            [RightShard.from_config(s) for s in d["right"]],
            d["template_pattern"],
            gt,
            d.get("group_size", None),
            sel,
            ap["dist"],
            ap["count"],
            ap["centroid"],
            pvs,
            d["name_template"]["template"],
            np["type"],
            np["left"],
            np["right"]
        )

    def apply_name_template(self, lname: str, rname: str) -> str:
        type_ = str(self.config)
        # re.sub(r'_+', '_', re.sub(r'\W', '_', str(self.config)))
        qname = self.name_template \
            .replace(self.name_type_pattern, type_) \
            .replace(self.name_left_pattern, lname) \
            .replace(self.name_right_pattern, rname)
        return re.sub(r'(^_+)|(_+$)', '', qname)

    def compose_single(self, files: dict[str, str], left: str,
                       right: RightShard) -> tuple[str, str, str]:
        lname = clean_name(left)
        rname = clean_name(right.filename)

        qname = self.apply_name_template(lname, rname)
        query, original = files[right.filename], files[right.filename]

        assert "?" + rname in query, f"Right variable ?{rname} is not defined"
        assert "?" + lname + "_centroid" not in query, \
            f"Centroid ?{lname}_centroid is defined, but should not be"

        for pv in self.provided_values:
            query = pv.compose(query, qname)

        if f"?{rname}_centroid" not in original:
            query = f"""
                {query}

                ?{rname} geo:hasCentroid/geo:asWKT ?{rname}_centroid .
            """

        count_var = f"?count_{rname}"
        if count_var not in original and (
                right.includes_variable(count_var)
                or count_var in right.payload):
            query = f"""
                {query}
                BIND(COUNT(*) AS ?count_{rname})
            """

        # Order is important here, the spatial join may not be
        # inside the subquery
        query = self.config.compose(
            f"?{lname}_centroid", f"?{rname}_centroid", f"?dist_{qname}",
            right.payload, query)

        select = f"\n# Select expressions for `{qname}`:\n"
        for selector in self.add_selectors:
            select += selector \
                .replace(self.dist_pattern, f"?dist_{qname}") \
                .replace(self.count_pattern, f"?count_{rname}") \
                .replace(self.centroid_pattern, f"?{rname}_centroid") \
                + "\n"

        return (qname, query, select)

    def compose_left(self, files: dict[str, str], left: str) \
            -> tuple[str, str]:
        lname = clean_name(left)
        qname = lname + "_"
        query = f"""
            {{
                {files[left]}

                ?{lname} geo:hasCentroid/geo:asWKT ?{lname}_centroid .
                BIND(COUNT(*) AS ?count_{lname})
            }}\n
        """
        return qname, query

    def compose(self, files: dict[str, str]) -> tuple[str, str]:
        """
        Will create subqueries for each combination of left and right.
        Each will contain one left query and up to group_size right queries.
        """
        collected_groups = []
        for left in self.left:
            # Grouping against overloading the query engine
            grouped = []
            if self.group_size:
                grouped = group(self.right, self.group_size)
            else:
                grouped = [self.right]
            for group_ in grouped:
                collected_queries = [
                    self.compose_left(files, left)
                ]
                collected_select = []
                for right in group_:
                    qname, query, select = self.compose_single(
                        files, left, right)
                    collected_queries.append((qname, query))
                    collected_select.append(select)
                collected_groups.append(
                    self.group_template.compose(
                        files, collected_queries, collected_select))
        return (
            self.template_pattern,
            "\n".join("\n{\n" + g + "\n}\n" for g in collected_groups))


@dataclass
class ReplaceRule:
    search: str
    replace: Optional[str]
    replace_file: Optional[str]

    def __post_init__(self):
        assert self.search, "Search pattern must not be empty"

        # Either from file or from string
        assert bool(self.replace) ^ bool(self.replace_file), \
            "You must provide 'replace' or 'replace_file' to a " + \
            "replace rule, not none or both"

    @staticmethod
    def from_config(d: ReplaceRuleDict) -> 'ReplaceRule':
        return ReplaceRule(
            d["search"],
            d.get("replace", None),
            d.get("replace_file", None)
        )

    def can_be_replaced(self, text: str) -> bool:
        return bool(re.search(self.search, text))

    def apply(self, files: dict[str, str], text: str) -> str:
        "Apply the replace rule without recursion"
        replace = self.replace
        if not replace:
            # For type checker
            assert self.replace_file is not None, \
                "A replace rule requires either replace_file or replace"
            # escape to not interpret \1 etc.
            replace = files[self.replace_file].replace('\\', '\\\\')
        # For type checker
        assert replace is not None, \
            "A replace rule requires either replace_file or replace"
        return re.sub(self.search, replace, text)


@dataclass
class Template:
    filename: str
    replace_rules: list[ReplaceRule]

    def __post_init__(self):
        assert self.filename, "Template needs a source file"

    @staticmethod
    def from_config(d: TemplateDict) -> 'Template':
        return Template(
            d["filename"],
            [ReplaceRule.from_config(r) for r in d.get("replace", [])]
        )

    def compose(self, files: dict[str, str], groups: list[tuple[str, str]]) \
            -> str:
        result = files[self.filename]

        # Insert spatial searches into template
        # The same pattern may be replaced multiple times
        groups_dict: dict[str, list[str]] = {}
        for pattern, value in groups:
            if pattern not in groups_dict:
                groups_dict[pattern] = []
            groups_dict[pattern].append(value)
        for pattern, values in groups_dict.items():
            value = "\n".join(values)
            result = result.replace(pattern, value)

        # Apply recursive replacement sequences
        # Max depth to avoid endless loop if search matches its own replacement
        for _ in range(MAX_REPLACE_DEPTH):
            replaced = False
            for r in self.replace_rules:
                if r.can_be_replaced(result):
                    replaced = True
                    result = r.apply(files, result)
            if not replaced:
                break
        else:
            raise RecursionError(
                "Maximum replace depth exceeded: please check your " +
                "configuration for a cycle in replace rules.")

        return result


@dataclass
class QueryConfig:
    template: Template
    spatial_searches: list[SpatialSearch]

    def __post_init__(self):
        assert self.template, "A template is required"
        assert len(self.spatial_searches), \
            "At least one spatial search is required"

    @staticmethod
    def from_config(d: QueryConfigDict) -> 'QueryConfig':
        assert "template" in d and "spatial_searches" in d, \
            "'template' and 'spatial_searches' configuration options are " + \
            "mandatory"
        return QueryConfig(
            Template.from_config(d["template"]),
            [SpatialSearch.from_config(s) for s in d["spatial_searches"]]
        )

    def compose(self, files: dict[str, str]) -> str:
        groups: list[tuple[str, str]] = []
        for s in self.spatial_searches:
            groups.append(s.compose(files))
        return self.template.compose(files, groups) + "\n"


def cli_main(_input: str, _main_config: str, _outfile: Optional[str]):
    """
    Main if the program is started as a conventional CLI application.
    Takes _input, _main_config, _outfile as paths and directly outputs results.
    """
    _config, _files = get_config_and_queries(
        _input, _main_config)
    config = QueryConfig.from_config(_config)
    result = indent(config.compose(_files))
    if _outfile is not None:
        with open(_outfile, "w") as f:
            f.write(result)
    else:
        print(result, end="")


class ComposeSpatialHTTPRequestHandler(BaseHTTPRequestHandler):
    """
    An instance of this class represents an HTTP request to the built-in
    web app. This is automatically instanciated by ComposeSpatialHTTPServer
    defined below.
    """

    def __compose_response(self, code: int, ctype: Mime, body: str):
        # For the type checker
        assert isinstance(self.server, ComposeSpatialHTTPServer)
        if self.server.verbose:
            logger.info("Response %d %s", code, ctype)

        self.send_response(code)
        self.send_header("Content-type", ctype.value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body.encode())

    def do_GET(self):
        """
        Process a GET request.
        """
        # For the type checker
        assert isinstance(self.server, ComposeSpatialHTTPServer)

        if self.server.verbose:
            logger.info("%s %s [%s:%d]", self.command,
                        self.path, *self.client_address)
        parsed = urlparse(self.path)
        pth = parsed.path

        if pth == "/":
            pth = "/index.html"

        if pth == "/blank_compose.json":
            self.__compose_response(200, Mime.JSON, "{}")
        elif self.server.knows(pth[1:]):
            self.__compose_response(
                200, guess_type(pth), self.server.get(pth[1:]))
        else:
            self.__compose_response(404, Mime.PLAIN, "Not Found")

    def do_POST(self):
        """
        Process a POST request.
        """
        # For the type checker
        assert isinstance(self.server, ComposeSpatialHTTPServer)

        if self.server.verbose:
            logger.info("%s %s [%s:%d]", self.command,
                        self.path, *self.client_address)
        p = urlparse(self.path).path

        if p != "/compose":
            self.__compose_response(405, Mime.PLAIN, "Method not allowed")
            return

        if self.headers["Content-type"] != Mime.JSON.value or \
                self.headers["Accept"] != Mime.SPARQL.value:
            self.__compose_response(
                400, Mime.PLAIN, "Expected HTTP headers" +
                " 'Content-type: application/json' and " +
                "'Accept: application/sparql-query' not found.")
            return

        try:
            data = json.loads(self.rfile.read(
                int(self.headers['Content-Length'])).decode("utf-8"))
            res = self.server.compose(data)
            self.__compose_response(200, Mime.SPARQL, res)
        except Exception as e:
            res = type(e).__name__ + ": " + str(e)
            if type(e) is KeyError:
                res += " - please make sure to add at least one item " + \
                    "to this mandatory field"
            logger.error(res)
            self.__compose_response(
                500, Mime.PLAIN, res)


class ComposeSpatialHTTPServer(HTTPServer):
    """
    A subclass of HTTPServer that implements custom logic for the internal
    web app. Mainly it loads all files required for the web app and compose
    tasks received from it into in-memory dictionaries. Thus a request is fast
    and cannot access files it is not intended to access.

    Fields:

    - `_input`:         A directory or ZIP file, where all SPARQL shards and
                        configuration files are stored.
    - `_main_config`:   The file path relative to `_input`, where a compose
                        config JSON can be found, that the web app will load
                        by default.
    - `_serve`:         The file path relative to `_input`, where a JSON can be
                        found, that specifies the behavior of this http server.
                        This includes the port to bind, allowed files, etc.
                        Details can be found in the separate documentation
                        file.

    All attributes are immutable after creation.
    """

    __pages: FilesCache
    __files: FilesCache
    __config: ServeDict

    def __get_pages(self):
        """
        Internal helper to load web app resources into a dict. These do not
        come from `_input` but must be relative to the program's location.
        """
        prog_dir = Path(__file__).parent
        compose_dir = prog_dir / "compose"
        for pth in compose_dir.rglob("**/*"):
            if not pth.is_file():
                continue
            with open(pth, "r") as f:
                self.__pages[str(pth.relative_to(compose_dir))] = f.read()

    def __init__(self, _input: str, _main_config: str, _serve: str):
        logger.info("Compose Spatial HTTP Server: Preparing...")

        # Get pages and resources (in-memory cache for HTTP request processing)
        self.__pages: FilesCache = {}
        self.__get_pages()

        # Load server configuration
        self.__config: ServeDict = json.loads(
            get_file_contents(_input, _serve))

        # Load all files into memory according to server and main config
        all_configs = get_all_configs(_input)
        if _main_config not in all_configs:
            all_configs.append(_main_config)

        self.__files: FilesCache = {}
        for config_fn in all_configs:
            _, c_files = get_config_and_queries(_input, config_fn)
            self.__files |= c_files

        retrieve_files_list = all_configs + \
            list(self.__config["templates"]) + \
            list(self.__config["group_templates"]) + \
            list(self.__config["shards"]) + \
            list(self.__config["replace_files"])
        for filename in retrieve_files_list:
            if filename not in self.__files:
                self.__files[filename] = get_file_contents(_input, filename)

        def get_desc(fn: str) -> str:
            """
            Extract descriptions from SPARQL shard files: if the first line is
            a comment, it is assumed to contain a description of what the query
            shard does
            """
            v = self.__files[fn]
            description = ""
            if fn.endswith(".rq") or fn.endswith(".sparql"):
                lines = v.splitlines()
                if not v.strip() or not lines:
                    return description
                head = lines[0].strip()
                if head.startswith("#"):
                    description = head[1:].strip()
            return description

        def desc_list(categ: str) -> list[tuple[str, str]]:
            return [
                (k, get_desc(k)) for k in self.__config[categ]
            ]

        # Generate dynamic configuration for web app and cache it in memory
        shards = desc_list("shards")
        templates = desc_list("templates")
        group_templates = desc_list("group_templates")
        replace_files = [("", "no file selected"), *desc_list("replace_files")]
        configs = [("blank_compose.json", "empty")] + \
            [(k, "") for k in all_configs]
        selects: dict[str, tuple[list[tuple[str, str]], Optional[str]]] = {
            "spatial_searches:_:config:algorithm": (
                list(QLEVER_SUPPORTED_ALGORITHMS.items()),
                QLEVER_DEFAULT_ALGORITHM
            ),
            "template:filename": (templates, None),
            "template:replace:_:replace_file": (replace_files, ""),
            "spatial_searches:_:group_template:filename":
            (group_templates, None),
            "spatial_searches:_:left:_": (
                shards, self.__config["default_left"]),
            "spatial_searches:_:right:_:filename": (
                shards, self.__config["default_right"])
        }

        self.__files["configs.json"] = json.dumps(configs)
        self.__files["default_input"] = _main_config
        self.__files["selects.json"] = json.dumps(selects)

        super().__init__(self.bind_address, ComposeSpatialHTTPRequestHandler)

        logger.info("Ready. HTTP server address is http://%s:%d/",
                    self.bind_address[0] or "localhost", self.bind_address[1])

    @property
    def verbose(self) -> bool:
        return bool(self.__config.get("verbose"))

    @property
    def bind_address(self) -> tuple[str, int]:
        return (self.__config["address"], self.__config["port"])

    def knows(self, fn: str) -> bool:
        """
        Whether the server can answer a GET request for a given file.
        Filename `fn` should not contain the leading slash.
        """
        return fn in self.__files or fn in self.__pages

    def get(self, fn: str) -> str:
        """
        Retrieve a cached file by name. Filename `fn` should not contain the
        leading slash.
        """
        if self.verbose:
            logger.info("Reading file %s", fn)
        return self.__files.get(fn, None) or self.__pages.get(fn, None) or ""

    def compose(self, data: QueryConfigDict) -> str:
        """
        Helper to answer a compose query.
        """
        if self.verbose:
            logger.info("Composing from config %s", repr(data))
        qconfig = QueryConfig.from_config(data)
        return indent(qconfig.compose(self.__files))


def serve_main(_input: str, _main_config: str, _serve: str):
    """
    Main if the program is started as a server for the included web app. Takes
    `_input`, `_main_config` and `_serve` as paths and communicates over HTTP
    using the bind address given in the `_serve` config file.
    """
    httpd = ComposeSpatialHTTPServer(_input, _main_config, _serve)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.server_close()


def parse_arguments(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    """
    Parse the program's command-line arguments
    """
    parser = argparse.ArgumentParser(
        description=PROGRAM_DESCRIPTION)

    # Required arguments
    parser.add_argument('input', nargs=1, type=str,
                        help='input configuration directory or zip file')
    parser.add_argument('--main-config', '-c', nargs=1, type=str,
                        help='filename of the main configuration json, ' +
                        'relative to input', required=True)

    # Optional arguments
    parser.add_argument('--output', '-o', nargs=1, type=str, default=None,
                        help='filename to write output SPARQL query, ' +
                        'ignored if --serve is given (default: stdout)')
    parser.add_argument('--serve', '-s', nargs=1, type=str,
                        help="""
                        If this argument is given, --output is ignored and a
                        webserver is started that serves a page where you can
                        interactively configure your query. --main-config is
                        used as a preset. The argument to --serve is expected
                        to be the path to a second configuration json file
                        relative to input that specifies the web server and
                        app's behavior.
                        """)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None):
    """
    The procedure to be run when the program is called directly.
    """
    args = parse_arguments(argv)
    _input, _main_config, _outfile = args.input[0], args.main_config[0], \
        args.output[0] if args.output else None
    if args.serve:
        _serve = args.serve[0]
        serve_main(_input, _main_config, _serve)
    else:
        cli_main(_input, _main_config, _outfile)


if __name__ == "__main__":
    main()
