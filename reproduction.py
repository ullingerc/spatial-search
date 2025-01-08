#!/bin/env python3
"""
Run all programs required to reproduce the thesis' results

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

from pathlib import Path
from typing import Callable, Iterator, Optional, Sequence, Any, TypedDict
from dataclasses import dataclass, field, is_dataclass, \
    asdict as dataclass_asdict
from time import time_ns, sleep
from enum import Enum
from statistics import mean, stdev, quantiles, median
import argparse
from shlex import quote
import json
import logging
import subprocess
import os
import re
import csv
from zipfile import ZipFile

PROGRAM_DESCRIPTION = """
\"Efficient Spatial Search for the QLever SPARQL engine\"

This program runs all steps required to reproduce the thesis' evaluation and
case study results.
"""

PROGRAM_DIR = Path(__file__).parent.resolve()

SUBPROCESS_TIMEOUT = 24 * 60 * 60  # 24h
VERBOSE = False

REQUIRED_ON_PATH = [
    "bash", "grep", "curl", "tar", "gunzip", "pwd", "mkdir", "df", "cat",
    "bzcat", "tail", "tee", "git", "time", "wc", "find", "rm", "chmod",
    "podman", "qlever"
]
SHELL = "/bin/bash"

# Bytes per Mebi/Gibibyte shorthands
MIB = 2 ** 20
GIB = 2 ** 30

FREE_SPACE_OUTPUT = 1024 * GIB
FREE_SPACE_CONTAINERS = 500 * GIB
FREE_MEMORY = 100 * GIB

# Nanoseconds per Milisecond shorthand
NS_MS = 10 ** 6

# Settings to bypass some steps
BYPASS_MEMORY_CHECK = False
BYPASS_DISK_CHECK = False
BYPASS_CONTAINER_BUILDS = False
BYPASS_GET_DATA: list[str] = []
BYPASS_INDEX_BUILD: list[str] = []
BYPASS_QUERY: list[str] = []
BYPASS_CASE_STUDY_QUERY = False

QLEVER_IMAGE = "docker.io/adfreiburg/qlever"
QLEVER_NO_GEOPOINT_HASH = "6384041460e62e6088fc86b5c5190e51cd986372"
QLEVER_NEW_HASH = "01d83064a97fa51925f7be3cc89a02acf2a20c02"
QLEVER_INSTANCES: dict[str, int] = {
    # directory/qleverfile name => port
    "osm-de": 7925,
    "election": 7926,
    "gtfs": 7928,
    "gtfs-no-geopoint": 7931
}
QLEVER_INDEX_FILES_GLOB = [
    "*.index.*", "*-log.txt", "*.meta-data.json",
    "*.settings.json", "*.vocabulary.*"
]
QLEVER_CONTROL_URL = "https://github.com/ad-freiburg/qlever-control.git"
QLEVER_ONLY_BASELINE_NOT_CARTESIAN = True

OSM2RDF_REPO = "https://github.com/ad-freiburg/osm2rdf.git"
OSM2RDF_COMMIT_HASH = "cf015f76bb9d257fbfa5eefbd31b828fbe75baaa"

OSM_GERMANY_PBF = "https://download.geofabrik.de/europe/germany-latest.osm.pbf"

OUTPUT_SUBDIRS: list[Path] = [Path(p) for p in QLEVER_INSTANCES.keys()] + \
    [Path("results"), Path("log")]

# Example OpenStreetMap tags for filtering subsets of OSM data of different
# sizes for evaluation steps
OsmTestTag = tuple[str, str, int]  # Tag key, Value, Approx.Size (OSM DE)
OSM_TEST_TAGS: list[OsmTestTag] = [
    ("leisure", "sauna", 1_500),
    ("railway", "station", 4_500),
    ("tourism", "viewpoint", 29_500),
    ("shop", "supermarket", 34_000),
    ("amenity", "restaurant", 102_000),
    ("amenity", "bench", 707_000),
    ("building", "*", 37_000_000)
]

# Example GTFS feeds and approximate number of gtfs:Stop entities
GTFS_TEST_FEEDS: dict[str, int] = {
    "delfi_20210924": 480_000,
    "delfi_20240603": 540_000,
    "vag_2024": 1_000,
    "fintraffic_2024": 1_500
}

# Maximum number of point combinations where we can reasonably still calculate
# a cartesian product
CARTESIAN_LIMIT = 250_000_000

# How often to run evaluation queries for more reliable execution times
QUERY_ITERATIONS = 10

# If an individual query in a benchmark takes longer than this amount of
# miliseconds, skip further iterations of the same query to reduce the overall
# running time of the evaluation -> this means that a single benchmark may take
# in the worst case approximately CANCEL_ITERATIONS_IF_LONGER_THAN *
# QUERY_ITERATIONS time (= ca. 1,7 h)
CANCEL_ITERATIONS_IF_LONGER_THAN = 10 * 60 * 1_000  # 10 min

# Sizes and filenames for checking output in "get_data" steps
ExpectedSize = tuple[str, int, int]  # Filename, Min, Max
ELECTION_EXPECTED_SIZE: list[ExpectedSize] = [
    (f, MIB // 2, 5 * MIB)
    for f in ("btw21.ttl.bz2", "btw21-aux-geo.tsv", "ew24.ttl.bz2")
]
OSM_DE_EXPECTED_SIZE: ExpectedSize = (
    "osm-germany.ttl.bz2", 50 * GIB, 200 * GIB)
GTFS_EXPECTED_SIZE: list[ExpectedSize] = [
    ("delfi21.ttl.bz2", 500 * MIB, 10 * GIB),
    ("delfi24.ttl.bz2", 500 * MIB, 10 * GIB),
    ("vag24.ttl.bz2", 10 * MIB, 100 * MIB),
    ("fintraffic24.ttl.bz2", 20 * MIB, 500 * MIB)
]
PG_EXPORT_WKT_SIZE: ExpectedSize = ("geometries.tsv", 10 * GIB, 50 * GIB)
PG_IMPORT_SPATIAL_REL_SIZE: ExpectedSize = (
    "geometries_spatialjoin.csv", 50 * GIB, 150 * GIB)
OSM_GERMANY_PBF_SIZE: ExpectedSize = (
    "osm-germany.pbf", 2 * GIB, 10 * GIB
)

# Filenames for extraction of zipped reproduction.py
EXTRACT_PROG_TMP_NAME = "temp_reproduction_code.zip"
EXTRACT_PROG_DIR = "program_dir"

# Allows skipping PostgreSQL queries that most likely won't finish in 24 hours
# based on experience from running them previously
POSTGRES_SKIP_24H_QUERIES = True
POSTGRES_24H_QUERIES: list[tuple[str, str, str, str]] = [
    ("building", "*", "railway", "station"),
    ("building", "*", "tourism", "viewpoint"),
]

# Don't apply CARTESIAN_LIMIT to GTFS baseline max distance search
GTFS_MAX_DIST_CARTESIAN_ALL = False

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class AnsiEsc(Enum):
    "Colors for fancy terminal output"
    RED = '\x1b[31m'
    GREEN = '\x1b[32m'
    YELLOW = '\x1b[33m'
    BLUE = '\x1b[34m'
    PINK = '\x1b[35m'
    NORMAL = '\x1b[0m'
    BOLD = '\x1b[1m'
    REVERSED = '\x1b[7m'


def _str(args: tuple[Any, ...]) -> str:
    "Helper to prepare for logging"
    return " ".join(str(a) for a in args)


def log_error(*args):
    logger.error(AnsiEsc.RED.value + _str(args) + AnsiEsc.NORMAL.value)


def log_warning(*args):
    logger.warning(AnsiEsc.YELLOW.value + _str(args) + AnsiEsc.NORMAL.value)


def log_success(*args):
    logger.info(AnsiEsc.GREEN.value + _str(args) + AnsiEsc.NORMAL.value)


def log_command(*args):
    logger.info(AnsiEsc.BLUE.value + _str(args) + AnsiEsc.NORMAL.value)


def log_info(*args):
    logger.info(_str(args))


def log_important(*args):
    logger.info(AnsiEsc.BOLD.value + AnsiEsc.REVERSED.value +
                AnsiEsc.BLUE.value + _str(args) + AnsiEsc.NORMAL.value)


class EnhancedJSONEncoder(json.JSONEncoder):
    """
    Allows encoding data containing dataclasses and pathlib.Path objects.
    Adapted from <https://stackoverflow.com/a/51286749>.
    """

    def default(self, o: Any) -> Any:
        if is_dataclass(o) and not isinstance(o, type):
            return dataclass_asdict(o)
        if isinstance(o, Path):
            return str(o)
        return super().default(o)


@dataclass
class InternalStep:
    success: bool = False
    stdout: str = ""
    stderr: str = ""
    identifier: Optional[str] = None

    @property
    def returncode(self):
        return 0 if self.success else 1


@dataclass
class SimpleSubproc:
    command: str
    cwd: Optional[Path]
    log_path: Optional[Path]
    returncode: int
    stdout: str
    stderr: str
    start: int
    end: int
    identifier: Optional[str] = None

    @staticmethod
    def run(command: str, cwd: Optional[Path] = None,
            fwd_output: bool | str | Path = False) -> 'SimpleSubproc':
        log_command(command)
        log_path: Optional[Path] = None
        match fwd_output:
            case str() | Path():
                log_path = Path(fwd_output).resolve()
                log_info("The command's output is being piped to a file. " +
                         f"Use 'tail -f {log_path}' to observe the log.")
                with open(log_path, "a") as log_file:
                    start = time_ns()
                    proc = subprocess.run(command, shell=True,
                                          executable=SHELL, cwd=cwd,
                                          timeout=SUBPROCESS_TIMEOUT,
                                          stderr=subprocess.STDOUT,
                                          stdout=log_file)
                    end = time_ns()
            case bool():
                start = time_ns()
                proc = subprocess.run(command, shell=True,
                                      executable=SHELL,
                                      cwd=cwd, timeout=SUBPROCESS_TIMEOUT,
                                      capture_output=(not fwd_output))
                end = time_ns()
        if VERBOSE:
            log_info("Command took",
                     round((end - start) / NS_MS), "ms")
        return SimpleSubproc(
            command,
            cwd,
            log_path,
            proc.returncode,
            "" if fwd_output else proc.stdout.decode("utf-8"),
            "" if fwd_output else proc.stderr.decode("utf-8"),
            start, end)

    @property
    def success(self) -> bool:
        return self.returncode == 0

    @property
    def time(self) -> float:
        "Running time in milliseconds"
        return (self.end - self.start) / 1000


SubResult = SimpleSubproc | InternalStep


@dataclass
class StepResult:
    subresults: list[SubResult] = field(
        default_factory=list)

    @property
    def success(self) -> bool:
        return all(i.success for i in self.subresults)

    def add(self, res: SubResult):
        self.subresults.append(res)


Results = dict[str, list[StepResult]]
SubResGen = Iterator[SubResult]
Step = Callable[[Results, Path], SubResGen]
Steps = list[Step]

IndexSizes = TypedDict("IndexSizes", {
    "postgres": dict[str, int],
    "qlever": dict[str, int]
})


###############################################################################


def dep(prev: Results, *args: Step):
    "Helper: Assert that steps have already been run successfully"
    for step in args:
        name = step.__name__
        assert name in prev, f"Step requires '{name}' to be run first"
        for p in prev[name]:
            assert p.success, \
                f"This step requires the step '{name}' to be successful, " + \
                "but it failed"


def check_dependencies(_: Results, output: Path) -> SubResGen:
    "Check for required programs"

    qmsg = " You can find install instructions at " + QLEVER_CONTROL_URL + "."

    for prog in REQUIRED_ON_PATH:
        p = SimpleSubproc.run(f"which '{prog}'", output)
        yield p
        assert p.success, \
            f"Required program '{prog}' not found on your $PATH. " + \
            "Please install it to proceed." + \
            (qmsg if prog == "qlever" else "")
        log_success(f"Found {p.stdout.rstrip()}")


def check_disk_free(prev: Results, output: Path) -> SubResGen:
    "Check for system requirements: disk space"

    dep(prev, check_dependencies)

    paths = [(output, FREE_SPACE_OUTPUT), (os.environ.get(
        "XDG_DATA_HOME") or os.environ.get("HOME"), FREE_SPACE_CONTAINERS)]

    for pth, req_space in paths:
        pth_ = quote(str(Path(pth).resolve()))
        p = SimpleSubproc.run(
            f"df --output=avail -B 1 {pth_} | tail -n 1", None)
        yield p

        assert p.success, f"Could not check for free disk space in '{pth}'"

        v = int(p.stdout.rstrip())
        if v >= req_space:
            log_success(f"Free space in {pth} is ok ({v / GIB} GiB).")
        else:
            msg = f"The directory '{pth}' has insufficient " + \
                "free space available: expected at least " + \
                f" {req_space / GIB} GiB, but found {v / GIB} GiB"
            if not BYPASS_DISK_CHECK:
                raise AssertionError(
                    msg + ". You can ignore this error at your own risk " +
                    "using --bypass-disk.")
            else:
                log_warning(msg)


def check_free_memory(prev: Results, output: Path) -> SubResGen:
    "Check for system requirements: free memory"

    dep(prev, check_dependencies)

    p = SimpleSubproc.run("cat /proc/meminfo", None)
    yield p
    assert p.success, "Could not check for free memory"
    m = re.findall(r'MemAvailable:\s*([0-9]+) kB', p.stdout.rstrip())
    assert len(m) == 1, "Invalid memory info from /proc/meminfo"
    v = int(m[0]) * 1024

    error = f"The free memory {v / GIB} GiB is " + \
        "insufficient and may lead to errors. Expecting at least " + \
        f"{FREE_MEMORY / GIB} GiB to be available during the entire " + \
        "processing. Please also make sure that no programs (like " + \
        "'earlyoom') are in place that might prevent programs from " + \
        "accessing this amount of memory."
    if v < FREE_MEMORY:
        if BYPASS_MEMORY_CHECK:
            log_warning(error)
        else:
            raise AssertionError(
                error +
                " Skip this error at your own risk with '--bypass-mem'.")
    else:
        log_success(f"Free memory is ok ({v / GIB} GiB).")


def mkdirs(prev: Results, output: Path) -> SubResGen:
    "Make subdirectories in output directory"

    dep(prev, check_dependencies)

    for p in OUTPUT_SUBDIRS:
        yield SimpleSubproc.run(
            f"mkdir -p {quote(str(Path(output, p)))}", output)


def extract_files(prev: Results, output: Path) -> SubResGen:
    "Extract required scripts and files"

    dep(prev, check_dependencies, mkdirs)

    # If we are running as a standalone zipimport, we need to extract the
    # required auxiliary files from __file__
    global PROGRAM_DIR
    if PROGRAM_DIR.is_file():
        temp_pth = Path(output, EXTRACT_PROG_TMP_NAME)

        with open(PROGRAM_DIR, "rb") as in_f, \
                open(temp_pth, "wb") as out_f:
            # Check and skip header
            header_exp = "#!/bin/env python3\n"
            header_real = in_f.read(len(header_exp))
            assert header_real == header_exp.encode("utf-8"), \
                "Program bundle invalid"
            # Copy body
            out_f.write(in_f.read())

        # Extract all files into new temporary program dir
        new_prog_dir = Path(output, EXTRACT_PROG_DIR)
        new_prog_dir.mkdir(exist_ok=True)
        with ZipFile(temp_pth, "r") as zf:
            zf.extractall(new_prog_dir)
        temp_pth.unlink()
        log_info("Unpacked zipped reproduction.py code to", new_prog_dir)
        yield InternalStep(True)
        PROGRAM_DIR = new_prog_dir

        # Restore executability flags
        for ext in ("py", "sh"):
            yield SimpleSubproc.run(
                f"find . -name '*.{ext}' -exec chmod +x {{}} \\+",
                new_prog_dir)
    else:
        log_info("The program is not running from an archive. Skipping.")
        yield InternalStep(True)


PREP: Steps = [
    # expected time: < 1 sec
    # Note: The expected times here and in the other lists of steps refer to a
    # Ubuntu Server 24.04.1 LTS with AMD Ryzen 9 7950X (32 threads); 128 GB RAM
    check_dependencies,
    check_disk_free,
    check_free_memory,
    mkdirs,
    extract_files
]

###############################################################################


def build_spatial_container(prev: Results, output: Path) -> SubResGen:
    "Build main container image"

    dep(prev, *PREP)

    if BYPASS_CONTAINER_BUILDS:
        yield SimpleSubproc.run("podman image inspect localhost/spatial")
        return

    p = SimpleSubproc.run("podman build -t spatial .", PROGRAM_DIR,
                          Path(output, "log", "build_spatial.log"))
    yield p
    assert p.success, "Building main image failed"


def build_stat_repro_container(prev: Results, output: Path) -> SubResGen:
    "Build container image for statistical analysis"

    dep(prev, *PREP)

    if BYPASS_CONTAINER_BUILDS:
        yield SimpleSubproc.run(
            "podman image inspect localhost/spatial_stat_repro")
        return

    c = "podman build -t spatial_stat_repro " + \
        "--file=Dockerfile.stat_repro ."
    p = SimpleSubproc.run(c, Path(PROGRAM_DIR, "reproduction", "stat"),
                          Path(output, "log", "build_stat_repro.log"))
    yield p
    assert p.success, "Building statistical analysis image failed"


def pull_qlever(prev: Results, output: Path) -> SubResGen:
    "Pull qlever container images"

    dep(prev, *PREP)

    for hash_ in (QLEVER_NEW_HASH, QLEVER_NO_GEOPOINT_HASH):
        img = f"{QLEVER_IMAGE}:commit-{hash_[:7]}"

        if BYPASS_CONTAINER_BUILDS:
            yield SimpleSubproc.run("podman image inspect " + img)
            continue

        p = SimpleSubproc.run(
            "podman pull " + img,
            PROGRAM_DIR, Path(output, "log", "pull_qlever.log"))
        yield p
        assert p.success, \
            f"Could not pull qlever image for commit hash {hash_}"


def build_osm2rdf(prev: Results, output: Path) -> SubResGen:
    "Build osm2rdf container image"

    dep(prev, *PREP)

    if BYPASS_CONTAINER_BUILDS:
        yield SimpleSubproc.run("podman image inspect localhost/osm2rdf")
        return

    c = "podman build -t osm2rdf " + \
        "--file=Dockerfile.osm2rdf ."
    p = SimpleSubproc.run(c, Path(PROGRAM_DIR, "reproduction", "qlever"),
                          Path(output, "log", "build_osm2rdf_repro.log"))
    yield p
    assert p.success, "Building osm2rdf image failed"


def build_postgres_container(prev: Results, output: Path) -> SubResGen:
    "Build postgres image for evaluation"

    dep(prev, *PREP)

    if BYPASS_CONTAINER_BUILDS:
        yield SimpleSubproc.run("podman image inspect localhost/eval_postgres")
        return

    c = "podman build -t eval_postgres --file=Dockerfile.postgres ."
    p = SimpleSubproc.run(
        c, Path(PROGRAM_DIR, "reproduction", "postgres"),
        Path(output, "log", "build_postgres.log"))
    yield p
    assert p.success, "Building postgres evaluation image failed"


def build_spatialjoin_container(prev: Results, output: Path) -> SubResGen:
    "Build spatialjoin image for postgres evaluation"

    dep(prev, *PREP)

    if BYPASS_CONTAINER_BUILDS:
        yield SimpleSubproc.run("podman image inspect localhost/spatialjoin")
        return

    c = "podman build -t spatialjoin --file=Dockerfile.spatialjoin ."
    p = SimpleSubproc.run(
        c, Path(PROGRAM_DIR, "reproduction", "postgres"),
        Path(output, "log", "build_spatialjoin.log"))
    yield p
    assert p.success, "Building spatialjoin image failed"


BUILD_CONTAINERS: Steps = [
    # expected time: 8min
    build_spatial_container,  # ca. 10 sec
    build_stat_repro_container,  # ca. 1 min 15 sec
    pull_qlever,  # ca. 10 sec
    build_osm2rdf,  # ca. 3 min 10 sec
    build_postgres_container,  # ca. 20 sec
    build_spatialjoin_container,  # ca. 2 min 35 sec
]

###############################################################################


def main_container(command: str, dir: Path) -> SimpleSubproc:
    "Helper to run a Makefile target in the spatial container"
    proc = SimpleSubproc.run(
        f"podman run --rm -it -v ./:/output:rw spatial make {command}", dir)
    assert proc.success, \
        f"Could not run 'make {command}' in 'spatial' container"
    return proc


def qlever_clear_cache(port: int) -> SimpleSubproc:
    "Helper to request a cache reset on a QLever instance"
    proc = SimpleSubproc.run(
        f"curl 'http://localhost:{port}/' -X POST " +
        "-H 'Accept: application/qlever-results+json' " +
        "-H 'Content-Type: application/x-www-form-urlencoded' " +
        "--data 'cmd=clear-cache'")
    json.loads(proc.stdout)  # Should be a valid json
    assert proc.success, f"Could not clear cache of QLever instance at {port}."
    return proc


def qlever_query(port: int, name: Optional[str], query: Path, output: Path,
                 qlever_json: bool = True) -> SimpleSubproc:
    "Helper to run a SPARQL query on a QLever instance"
    accept = "application/qlever-results+json" if qlever_json \
        else "text/tab-separated-values"
    ext = "json" if qlever_json else "tsv"
    out = ""
    if name:
        out_pth = str(Path(output, "results", f"qlever_{port}_{name}.{ext}"))
        out = "-o " + quote(out_pth)
    proc = SimpleSubproc.run(
        f"curl 'http://localhost:{port}/' " +
        "--fail -X POST " +
        f"-H 'Accept: {accept}' " +
        "-H 'Content-Type: application/sparql-query' " +
        f"--data-binary @{quote(str(query))} " + out)
    proc.identifier = name
    assert proc.success, f"Could not run SPARQL query {str(query)} on " + \
        f"QLever instance at {port}."
    return proc

###############################################################################


def copy_qleverfiles(prev: Results, output: Path) -> SubResGen:
    "Copy Qleverfiles into appropriate directories"

    dep(prev, *PREP)

    for name in QLEVER_INSTANCES:
        source_ = str(Path(PROGRAM_DIR, "reproduction", "qlever",
                           f"Qleverfile-{name}.ini"))
        target_ = str(Path(output, name, "Qleverfile"))
        yield SimpleSubproc.run(f"cp {quote(source_)} {quote(target_)}",
                                PROGRAM_DIR)


def get_data_election(prev: Results, output: Path) -> SubResGen:
    "Generate election data sets"
    dep(prev, *PREP, build_spatial_container, copy_qleverfiles, pull_qlever)
    if "election" not in BYPASS_GET_DATA:
        yield SimpleSubproc.run("qlever get-data", Path(output, "election"),
                                Path(output, "log", "get_data_election.log"))

    # Check resulting data
    for f, _min, _max in ELECTION_EXPECTED_SIZE:
        assert _min <= Path(output, "election", f).stat().st_size <= _max, \
            f"Checking {f} failed"
    yield InternalStep(True)


def get_data_osm_de(prev: Results, output: Path) -> SubResGen:
    "Generate OpenStreetMap Germany data set with auxiliary geometries"

    dep(prev, *PREP, build_spatial_container,
        pull_qlever, build_osm2rdf, get_data_election, copy_qleverfiles)

    # Can also be generated using
    # "bzcat *.ttl.bz2 | grep '@prefix' > prefixes.ttl"
    el_prefixes = str(Path(PROGRAM_DIR, "election", "prefixes.ttl"))
    el_target = str(Path(output, "osm-de"))
    cp1 = SimpleSubproc.run(f"cp {quote(el_prefixes)} {quote(el_target)}")
    yield cp1
    assert cp1.success, "Copying election2rdf prefixes to OSM dir failed"

    aux_geo_src = str(Path(output, "election", "btw21-aux-geo.tsv"))
    aux_geo_trg = str(Path(output, "osm-de"))
    cp2 = SimpleSubproc.run(f"cp {quote(aux_geo_src)} {quote(aux_geo_trg)}")
    yield cp2
    assert cp2.success, "Copying btw21-aux-geo.tsv to OSM dir failed"

    if "osm-de" not in BYPASS_GET_DATA:
        yield SimpleSubproc.run("qlever get-data", Path(output, "osm-de"),
                                Path(output, "log", "get_data_osm-de.log"))

    # Check resulting data
    f, _min, _max = OSM_DE_EXPECTED_SIZE
    assert _min <= Path(output, "osm-de", f).stat().st_size <= _max, \
        "Checking osm-germany.ttl.bz2 failed"
    yield InternalStep(True)


def get_data_gtfs(prev: Results, output: Path) -> SubResGen:
    "Generate GTFS data sets"
    dep(prev, *PREP, build_spatial_container, pull_qlever, copy_qleverfiles)
    if "gtfs" not in BYPASS_GET_DATA:
        yield SimpleSubproc.run("qlever get-data", Path(output, "gtfs"),
                                Path(output, "log", "get_data_gtfs.log"))

    # Check resulting data
    for f, _min, _max in GTFS_EXPECTED_SIZE:
        assert _min <= Path(output, "gtfs", f).stat().st_size <= _max, \
            f"Checking {f} failed"
    yield InternalStep(True)


def copy_data_gtfs_no_geopoint(prev: Results, output: Path) -> SubResGen:
    "Copy gtfs2rdf output from gtfs to gtfs-no-geopoint"

    dep(prev, *PREP, get_data_gtfs)

    trg = Path(output, "gtfs-no-geopoint")

    for f, _min, _max in GTFS_EXPECTED_SIZE:
        pth = Path(output, "gtfs", f)
        assert pth.exists()
        assert _min <= pth.stat().st_size <= _max, \
            f"gtfs2rdf output file {pth} does not have the expected size"

        if "gtfs-no-geopoint" in BYPASS_GET_DATA:
            assert Path(trg, f).exists(), f + " missing"
            yield InternalStep(True)
        else:
            cp = SimpleSubproc.run(f"cp {quote(str(pth))} {quote(str(trg))}")
            yield cp
            assert cp.success, f"Copying gtfs2rdf output file {f} failed"


def get_data_postgres(prev: Results, output: Path) -> SubResGen:
    "Download OpenStreetMap Germany for PostgreSQL if not already downloaded"

    dep(prev, *PREP)

    _f, _min, _max = OSM_GERMANY_PBF_SIZE
    expected = Path(output, "osm-de", _f)
    if expected.exists():
        assert _min <= expected.stat().st_size <= _max, \
            "OSM Germany PBF file does not have the expected size"
        yield InternalStep(True)
        return

    log_info("Downloading", OSM_GERMANY_PBF)
    yield SimpleSubproc.run(
        "curl --fail -L -o " + quote(str(Path("osm-de", _f))) + " " +
        quote(OSM_GERMANY_PBF), output,
        Path(output, "log", "get_data_postgres.log"))


GET_DATA: Steps = [
    # expected time: 3 h 32 min
    copy_qleverfiles,  # ca. 1 sec
    get_data_election,  # ca. 16 sec
    get_data_osm_de,  # ca. download 2 min 5 sec + osm2rdf 2 h 12 min
    get_data_gtfs,  # ca. 1 h 23 min
    copy_data_gtfs_no_geopoint,  # ca. 2 sec
    # ca. Delfi21 35 min + Delfi24 38 min + VAG 35 sec +
    # Digitraffic.fi 1 min 56 sec = 1 h 16 min
    get_data_postgres  # ca. 0 sec (already exists from osm-de)
]

###############################################################################


def qlever_index(instance: str, output: Path) -> SubResGen:
    "Helper to run 'qlever index'"
    idx_dir = Path(output, instance)
    if instance in BYPASS_INDEX_BUILD:
        # Check if index is present
        n = 0
        for glob in QLEVER_INDEX_FILES_GLOB:
            i = len(list(idx_dir.glob(glob)))
            n += i
            assert i > 0, \
                "Index build was bypassed, but could not find files " + \
                f"matching '{glob}' in directory '{idx_dir}'"
        log_info(
            f"Bypassed index build for {instance}. {n} index files found.")
        yield InternalStep(True)
        return

    # Build index
    index_log = Path(output, "log", f"index_{instance}.log")
    p = SimpleSubproc.run("qlever index --overwrite-existing",
                          Path(output, instance),
                          index_log)
    yield p

    log: list[str] = []
    with open(index_log, "r") as f:
        log = [line.strip() for line in f if line.strip()]
    if not log:
        log_error("Missing Index Builder log.")
        yield InternalStep(False, "", "")
    elif "ERROR" in log[-1]:
        log_error("The QLever Index Builder crashed with message:", log[-1])
        yield InternalStep(False, "", log[-1])


def index_gtfs_no_geopoint(prev: Results, output: Path) -> SubResGen:
    "Index GTFS data set using old QLever without GeoPoints"
    dep(prev, *PREP, pull_qlever, copy_data_gtfs_no_geopoint)
    yield from qlever_index("gtfs-no-geopoint", output)


def index_osm_de(prev: Results, output: Path) -> SubResGen:
    "Index OpenStreetMap Germany using up-to-date QLever"
    dep(prev, *PREP, pull_qlever, get_data_osm_de)
    yield from qlever_index("osm-de", output)


def index_election(prev: Results, output: Path) -> SubResGen:
    "Index election data using up-to-date QLever"
    dep(prev, *PREP, pull_qlever, get_data_election)
    yield from qlever_index("election", output)


def index_gtfs(prev: Results, output: Path) -> SubResGen:
    "Index GTFS data sets using up-to-date QLever"
    dep(prev, *PREP, pull_qlever, get_data_gtfs)
    yield from qlever_index("gtfs", output)


def check_container_status(name: str, status: str) -> SubResGen:
    "Helper: Check if a podman container with a given name has a given status"

    p = SimpleSubproc.run("podman container inspect " + name)
    yield p

    statuses = [container["State"]["Status"]
                for container in json.loads(p.stdout)]
    yield InternalStep(len(statuses) > 0)
    yield InternalStep(all(actual == status for actual in statuses))


def check_container_running(name: str) -> SubResGen:
    "Helper: Check if a podman container with the given name is running"
    yield from check_container_status(name, "running")


def create_postgres(prev: Results, output: Path) -> SubResGen:
    "Start PostgreSQL container and initialize database"

    dep(prev, *PREP, build_postgres_container)

    if "postgres" in BYPASS_INDEX_BUILD:
        log_info("Podman Index Build Bypass: Trying to start the container " +
                 "'eval_postgres' instead of replacing it.")

        # Start container
        yield SimpleSubproc.run("podman start eval_postgres",
                                output,
                                Path(output, "log", "start_postgres.log"))

        # Wait for start
        log_info("Wait 10 sec for the detached container to start")
        sleep(10)
        yield InternalStep(True)

        # Check if it is running
        yield from check_container_running("eval_postgres")
        return

    results = str(Path(output, "results"))
    osm_de = str(Path(output, "osm-de"))
    cmd = f"podman run -dt -v {quote(results)}:/output:rw " + \
        f"-v {quote(osm_de)}:/mnt:ro --name eval_postgres " + \
        "--replace eval_postgres"
    yield SimpleSubproc.run(cmd, output,
                            Path(output, "log", "create_postgres.log"))

    log_info("Wait 10 sec for the detached container to start")
    sleep(10)
    yield InternalStep(True)


def stop_postgres(prev: Results, output: Path) -> SubResGen:
    "Stop PostgreSQL container"

    dep(prev, *PREP, build_postgres_container, create_postgres)

    yield SimpleSubproc.run("podman stop eval_postgres",
                            output, Path(output, "log", "stop_postgres.log"))
    yield from check_container_status("eval_postgres", "exited")


def start_postgres(prev: Results, output: Path) -> SubResGen:
    "Start existing PostgreSQL container"

    dep(prev, *PREP, build_postgres_container, create_postgres)

    yield SimpleSubproc.run("podman start eval_postgres",
                            output, Path(output, "log", "start_postgres.log"))

    log_info("Wait 10 sec for the detached container to start")
    sleep(10)
    yield InternalStep(True)

    yield from check_container_running("eval_postgres")


def run_psql(name: Optional[str], query: str, output: Path,
             query_dir: Optional[Path] = None) -> SimpleSubproc:
    "Helper: Run a query in the PostgreSQL container"

    if not query_dir:
        query_dir = Path(PROGRAM_DIR, "reproduction", "postgres")
    query_pth = str(Path(query_dir, query))

    out = ""
    q = ""
    log: Path | bool = False
    if name:
        out_pth = str(Path(output, "results", f"postgres_{name}.csv"))
        out = " > " + quote(out_pth)
        log = Path(output, "log", "run_postgres_" + name + ".log")
        q = "--quiet "

    psql = SimpleSubproc.run(
        f"podman exec -i -u postgres eval_postgres psql --csv -d osm {q}< " +
        quote(query_pth) + out, output, log)
    psql.identifier = name
    assert psql.success, \
        f"PostgreSQL: running query '{query}' failed: '{psql.stderr}'"
    return psql


def osm2pgsql(prev: Results, output: Path) -> SubResGen:
    "Import OpenStreetMap Germany into PostgreSQL using osm2pgsql"

    dep(prev, *PREP, build_postgres_container, create_postgres,
        get_data_postgres)

    if "postgres" in BYPASS_INDEX_BUILD:
        log_info("Bypass Index Build: Checking 'planet_osm_*' tables")
        p = run_psql(None, "check_osm2pgsql.sql", output)
        yield p
        r = re.findall(r'[0-9]+', p.stdout)
        assert len(r) == 3, "Not three tables"
        for r_count in r:
            assert int(r_count) > 1_000_000, "Table contents missing"
        return

    yield run_psql(None, "drop.sql", output)

    yield SimpleSubproc.run(
        "podman exec -u postgres eval_postgres osm2pgsql " +
        "--database osm /mnt/osm-germany.pbf",
        output, Path(output, "log", "osm2pgsql.log"))


def postgres_tables_indexes(prev: Results, output: Path) -> SubResGen:
    "PostgreSQL: create tables, indexes, precompute centroids"

    dep(prev, *PREP, build_postgres_container, create_postgres, osm2pgsql)

    if "postgres" in BYPASS_INDEX_BUILD:
        log_info("Bypass Index Build: Checking 'osm_centroids' table")
        p = run_psql(None, "check_osm_centroids.sql", output)
        yield p
        r = re.findall(r'[0-9]+', p.stdout)
        assert len(r) == 1 and int(r[0]) > 1_000_000, "Missing osm_centroids"
        return

    yield run_psql(None, "create_osm_centroids.sql", output)
    yield run_psql(None, "idx_centroids.sql", output)
    yield run_psql(None, "idx_text.sql", output)
    yield run_psql(None, "create_spatialrelations.sql",
                   output)


def get_postgres_data_size() -> tuple[SimpleSubproc, int]:
    "Helper: Collect the current size of the PostgreSQL data directory"
    cmd = "podman exec -u postgres eval_postgres bash -c \"du -bcs " + \
        "/var/lib/postgresql/data | tail -n 1 | grep -oP '[0-9]+'\""
    p = SimpleSubproc.run(cmd)
    n = int(p.stdout.strip())
    log_info(f"PostgreSQL Data Size: {n} bytes = ca. {round(n / GIB)} GiB")
    return p, n


index_size_pg_no_sr: Optional[int] = None


def collect_index_size_pg_no_sr(prev: Results, output: Path) -> SubResGen:
    "Collect size of PostgreSQL indexes without spatial relations"

    dep(prev, *PREP, create_postgres, postgres_tables_indexes)

    # Test if spatialrelations is empty else warn and do nothing
    log_info("Checking 'spatialrelations' table")
    p = run_psql(None, "check_spatialrelations.sql", output)
    yield p
    r = re.findall(r'[0-9]+', p.stdout)
    assert len(r) == 1, "Missing 'spatialrelations' table"
    if int(r[0]) > 0:
        log_warning("Cannot collect PostgreSQL data prior to spatial " +
                    "relations import, because table 'spatialrelations' is " +
                    f"already populated with {r[0]} entries. Skipping.")
        yield InternalStep(True,
                           f"Skip with {r[0]} entries in spatialrelations")
        return

    p, n = get_postgres_data_size()  # expected: ca. 70 GiB
    global index_size_pg_no_sr
    index_size_pg_no_sr = n
    yield p


def postgres_export_for_spatial(prev: Results, output: Path) -> SubResGen:
    "PostgreSQL: export WKT for precomputation of spatial relations"

    dep(prev, *PREP, build_postgres_container, create_postgres, osm2pgsql)

    if "postgres" in BYPASS_INDEX_BUILD:
        yield InternalStep(True)
        return

    # Give permission for export
    yield SimpleSubproc.run(
        "podman exec eval_postgres bash -c \"touch /output/geometries.tsv " +
        "&& chmod a+rw /output/geometries.tsv\"")

    yield run_psql(None, "export_for_spatialjoin.sql", output)

    # check geometries.tsv file size (22 GiB)
    _fn, _min, _max = PG_EXPORT_WKT_SIZE
    assert _min <= Path(output, "results", _fn).stat().st_size <= _max, \
        f"Expected size of {_fn} not met"


def spatialjoin_for_postgres(prev: Results, output: Path) -> SubResGen:
    "Precompute spatial relations using 'spatialjoin' for PostgreSQL"

    dep(prev, *PREP, build_postgres_container,
        create_postgres, osm2pgsql, postgres_export_for_spatial)

    if "postgres" in BYPASS_INDEX_BUILD:
        yield InternalStep(True)
        return

    # Run spatialjoin program. Use integers to represent the relation more
    # efficiently
    cmd = "podman run --memory=50G --rm -v ./:/mnt " + \
        "spatialjoin sh -c \"spatialjoin --intersects ' 1 ' " + \
        "--contains ' 2 ' --covers ' 3 ' --touches ' 4 ' " + \
        "--equals ' 5 ' --overlaps ' 6 ' --crosses ' 7 ' " + \
        "< /mnt/geometries.tsv > /mnt/geometries_spatialjoin.csv\""
    yield SimpleSubproc.run(cmd, Path(output, "results"),
                            Path(output, "log", "pg_spatialjoin.log"))

    # check geometries_spatialjoin.csv file size (86 GiB)
    _fn, _min, _max = PG_IMPORT_SPATIAL_REL_SIZE
    assert _min <= Path(output, "results", _fn).stat().st_size <= _max, \
        f"Expected size of {_fn} not met"


def postgres_import_spatial(prev: Results, output: Path) -> SubResGen:
    "PostgreSQL: import and index data from spatialjoin"

    dep(prev, *PREP, build_postgres_container, create_postgres, osm2pgsql,
        postgres_tables_indexes, postgres_export_for_spatial,
        spatialjoin_for_postgres)

    if "postgres" in BYPASS_INDEX_BUILD:
        log_info("Bypass Index Build: Checking 'spatialrelations' table")
        p = run_psql(None, "check_spatialrelations.sql", output)
        yield p
        r = re.findall(r'[0-9]+', p.stdout)
        assert len(r) == 1 and int(r[0]) > 1_000_000, \
            "Missing spatialrelations"
        return

    yield run_psql(None, "import_spatialjoin.sql", output)
    yield run_psql(None, "idx_spatialrelations.sql", output)


def collect_index_sizes(prev: Results, output: Path) -> SubResGen:
    "Collect sizes of QLever and PostgreSQL indexes"

    dep(prev, *PREP, create_postgres,
        index_election, index_gtfs, index_osm_de,
        index_gtfs_no_geopoint,
        osm2pgsql, postgres_tables_indexes,
        postgres_import_spatial)

    # Collect results
    sizes: IndexSizes = {
        "postgres": {},
        "qlever": {}
    }

    # PostgreSQL
    p, n = get_postgres_data_size()
    yield p
    sizes["postgres"]["osm-de"] = n

    if index_size_pg_no_sr:
        sizes["postgres"]["osm-de-no-spatialrelations"] = index_size_pg_no_sr

    # QLever
    cmd = f"du -bc  {' '.join(QLEVER_INDEX_FILES_GLOB)} | tail -n 1 | " + \
        "grep -oP '[0-9]+'"
    for d in QLEVER_INSTANCES:
        p = SimpleSubproc.run(cmd, Path(output, d))
        n = int(p.stdout.strip())
        log_info(
            f"QLever Data Size: {d}: {n} bytes = ca. {round(n / GIB)} GiB")
        sizes["qlever"][d] = n
        yield p

    # Write results JSON
    sizes_str = json.dumps(sizes)
    fn = Path(output, "results", "index_sizes.json")
    with open(fn, "w") as f:
        f.write(sizes_str)

    # Write results CSV
    fn_csv = Path(output, "results", "index_sizes.csv")
    with open(fn_csv, "w") as f:
        w = csv.writer(f)
        w.writerow(["system", "index", "size"])
        for system in sizes:
            for index, size in sizes[system].items():
                w.writerow([system, index, size])


CREATE_INDEXES: Steps = [
    # expected time: ca. 11 h 30 min
    index_election,  # ca. 2 sec
    index_gtfs,  # ca. 15 min 20 sec
    index_osm_de,  # ca. 9 h 52 min
    index_gtfs_no_geopoint,  # ca. 14 min 41 sec
    create_postgres,  # ca. 10 sec
    osm2pgsql,  # ca. 8 min 14 sec
    postgres_tables_indexes,  # ca. 27 min 26 sec
    collect_index_size_pg_no_sr,  # ca. 1 sec
    postgres_export_for_spatial,  # ca. 6 min 44 sec
    spatialjoin_for_postgres,  # ca. 10 min 27 sec
    postgres_import_spatial,
    # ca. import 17 min 23 sec + index 48 min 41 sec
    collect_index_sizes,  # ca. 5 sec
    stop_postgres,  # ca. 1 sec
]

###############################################################################


def qlever_start(instance: str, output: Path) -> SimpleSubproc:
    return SimpleSubproc.run("qlever start",
                             Path(output, instance),
                             Path(output, "log",
                                  f"start_{instance}.log"))


def qlever_stop(instance: str, output: Path) -> SimpleSubproc:
    return SimpleSubproc.run("qlever stop",
                             Path(output, instance),
                             Path(output, "log",
                                  f"stop_{instance}.log"))


def start_gtfs_no_geopoint(prev: Results, output: Path) -> SubResGen:
    "Start QLever without GeoPoints on GTFS data"
    dep(prev, *PREP, pull_qlever,
        copy_data_gtfs_no_geopoint, index_gtfs_no_geopoint)
    yield qlever_start("gtfs-no-geopoint", output)


def stop_gtfs_no_geopoint(prev: Results, output: Path) -> SubResGen:
    "Stop QLever instance on GTFS without GeoPoints"
    dep(prev, *PREP, pull_qlever)
    yield qlever_stop("gtfs-no-geopoint", output)


def start_osm_de(prev: Results, output: Path) -> SubResGen:
    "Start QLever on OpenStreetMap Germany"
    dep(prev, *PREP, pull_qlever, index_osm_de)
    yield qlever_start("osm-de", output)


def stop_osm_de(prev: Results, output: Path) -> SubResGen:
    "Stop QLever instance for OpenStreetMap Germany"
    dep(prev, *PREP, pull_qlever, index_osm_de, start_osm_de)
    yield qlever_stop("osm-de", output)


def start_election(prev: Results, output: Path) -> SubResGen:
    "Start QLever on election data"
    dep(prev, *PREP, pull_qlever, index_election)
    yield qlever_start("election", output)


def stop_election(prev: Results, output: Path) -> SubResGen:
    "Stop QLever instance for election data"
    dep(prev, *PREP, pull_qlever, index_election, start_election)
    yield qlever_stop("election", output)


def start_gtfs(prev: Results, output: Path) -> SubResGen:
    "Start QLever on GTFS data"
    dep(prev, *PREP, pull_qlever, index_gtfs)
    yield qlever_start("gtfs", output)


def stop_gtfs(prev: Results, output: Path) -> SubResGen:
    "Stop QLever instance for GTFS data"
    dep(prev, *PREP, pull_qlever, index_gtfs, start_gtfs)
    yield qlever_stop("gtfs", output)


###############################################################################


def gen_tasks() -> Iterator[tuple[OsmTestTag, OsmTestTag]]:
    """
    Helper: Generate pairs of join criteria from osm tags for evaluation of
    spatial search
    """
    for left in OSM_TEST_TAGS:
        for right in OSM_TEST_TAGS:
            if left == right:
                # Getting the nearest neighbor in a self join does not
                # make sense
                continue
            yield (left, right)


execution_times_cache: dict[str, list[int]] = {}


def export_time(name: str, proc: Optional[SimpleSubproc], output: Path):
    """
    Helper: Write the running time in milliseconds to a text list. If the
    process was unsuccessful, raise an exception. If None is given, reset the
    text file. Also record the execution time in `execution_times_cache` for
    use with `make_time_stats`.
    """
    filename = Path(output, "results", name + "_times.csv")
    match proc:
        case None:
            with open(filename, "w") as f:
                print("duration_ms", file=f)
            execution_times_cache[name] = []
        case SimpleSubproc():
            assert proc.success
            dur = round((proc.end - proc.start) / NS_MS)
            execution_times_cache[name].append(dur)
            log_info("Took", dur, "ms")
            with open(filename, "a") as f:
                print(dur, file=f)


def make_query(path: Path, out_filename: Path, replace: dict[str, str]):
    "Helper: get an query and apply replacements"
    in_filename = Path(PROGRAM_DIR, "reproduction", path)
    with open(in_filename, "r") as f:
        query = f.read()
    for s, r in replace.items():
        query = query.replace(s, r)
    with open(out_filename, "w") as f:
        f.write(query)


def make_rq(name: str, output: Path, suffix: str, replace: dict[str, str]) \
        -> Path:
    """
    Helper: get a SPARQL query, apply replacements and store it in the output
    directory with an appropriate filename
    """
    out_path = Path(output, "results",
                    "qlever_" + name + "_" + suffix + ".rq")
    make_query(Path("qlever", name + ".rq"), out_path, replace)
    return out_path


def make_sql(name: str, output: Path, suffix: str, replace: dict[str, str]) \
        -> Path:
    """
    Helper: get an SQL query, apply replacements and store it in the output
    directory with an appropriate filename
    """
    out_path = Path(output, "results",
                    "postgres_" + name + "_" + suffix + ".sql")
    make_query(Path("postgres", name + ".sql"), out_path, replace)
    return out_path


StatsDict = TypedDict("StatsDict", {
    "min": int | float | None,
    "1q": int | float | None,
    "median": int | float | None,
    "3q": int | float | None,
    "max": int | float | None,
    "mean": float | None,
    "stdev": float | None,
    "samples": int,
    "raw": list[int]
})


def make_time_stats(name: str, output: Optional[Path]) -> StatsDict:
    """
    Helper: Summarize times recorded in execution_times_cache using common
    descriptive statistical measures. Print, save to file and return the
    result.
    """
    times = execution_times_cache[name]
    stats: StatsDict = {
        "min": min(times),
        "1q": quantiles(times, n=4)[0] if len(times) >= 2 else None,
        "median": median(times),
        "3q": quantiles(times, n=4)[2] if len(times) >= 2 else None,
        "max": max(times),
        "mean": mean(times),
        "stdev": stdev(times) if len(times) >= 2 else None,
        "samples": len(times),
        "raw": times
    }
    if output:
        out_path = Path(output, "results", name + "_summary.json")
        with open(out_path, "w") as f:
            json.dump(stats, f)
        log_info("Time Statistics:", json.dumps(stats))
    return stats


def qlever_query_timed(instance: str, name: str, output: Path, query: Path) \
        -> SubResGen:
    """
    Helper: run a query on a QLever instance a few times and record the running
    times
    """
    port = QLEVER_INSTANCES[instance]
    name = f"qlever_{instance}_{name}"

    if instance in BYPASS_QUERY:
        log_info(f"Checking if benchmark query {name} has been run.")
        if Path(output, "results", name + "_summary.json").exists():
            log_warning("Benchmark summary JSON file found. Skipping.")
            yield InternalStep(True)
            return
        log_info("Benchmark summary JSON file not found. Running query.")

    export_time(name, None, output)
    for i in range(QUERY_ITERATIONS):
        try:
            p = qlever_query(port, f"{name}_{i}", query, output, False)
            yield p
            export_time(name, p, output)
            if (p.end - p.start) / NS_MS > CANCEL_ITERATIONS_IF_LONGER_THAN:
                log_warning(f"The benchmark query {name} took longer than " +
                            str(CANCEL_ITERATIONS_IF_LONGER_THAN) + " ms. " +
                            "Skipping further iterations.")
                break
            yield qlever_clear_cache(port)
            sleep(1)
        except subprocess.TimeoutExpired:
            # Query ran more than 24h
            summary = Path(output, "results", name + "_summary.json")
            with open(summary, "w") as f:
                json.dump({"error": "Timeout"}, f)
                yield InternalStep(True, f"Query {name} timed out")
                log_warning(f"The query {name} took more than 24h " +
                            "and was thus aborted.")
                return
    make_time_stats(name, output)


class QleverNNBenchmarkMode(Enum):
    S2 = ""
    CARTESIAN = "_cartesian"
    BASELINE = "_baseline"


def qlever_query_nearest_neighbor(instance: str, left: tuple[str, str],
                                  right: tuple[str, str],
                                  mode: QleverNNBenchmarkMode,
                                  output: Path) -> SubResGen:
    "Helper: Preapare a nearest neighbor SPARQL query run"
    name = "nearest_neighbor" + mode.value
    suffix = f"{left[0]}-{left[1] if left[1] != '*' else 'all'}_{right[0]}" + \
        f"-{right[1] if right[1] != '*' else 'all'}"
    assert re.match(r'^[0-9a-z_-]+$', name + "_" + suffix)
    query = make_rq(name, output, suffix, {
        "%TAG_LEFT%": left[0] + (
            f" \"{left[1]}\"" if left[1] != "*" else " []"),
        "%TAG_RIGHT%": right[0] + (
            f" \"{right[1]}\"" if right[1] != "*" else " []")
    })
    yield from qlever_query_timed(instance, name + "_" + suffix, output, query)


def qlever_benchmark_nearest_neighbor(instance: str,
                                      mode: QleverNNBenchmarkMode,
                                      output: Path) -> SubResGen:
    """
    Helper: Run a full nearest neighbor OpenStreetMap benchmark on a QLever
    instance
    """
    for left, right in gen_tasks():
        ltag, lval, lsize = left
        rtag, rval, rsize = right
        if mode in (QleverNNBenchmarkMode.CARTESIAN,
                    QleverNNBenchmarkMode.BASELINE) \
                and lsize * rsize > CARTESIAN_LIMIT:
            continue
        if QLEVER_ONLY_BASELINE_NOT_CARTESIAN and \
                mode == QleverNNBenchmarkMode.CARTESIAN:
            continue
        yield from qlever_query_nearest_neighbor(
            instance, (ltag, lval), (rtag, rval), mode, output)


class QleverGTFSBenchmarkMode(Enum):
    S2 = "s2"
    BASELINE = "baseline"
    CARTESIAN = "cartesian"


def qlever_query_gtfs_max_dist(instance: str, feed: str,
                               mode: QleverGTFSBenchmarkMode,
                               output: Path) -> SubResGen:
    name = "gtfs_max_dist"
    suffix = mode.value + "_" + feed
    if mode == QleverGTFSBenchmarkMode.CARTESIAN:
        name += "_cartesian"
        suffix = feed
    query = make_rq(name, output, suffix, {
        "%FEED%": feed,
        "%ALGORITHM%": mode.value
    })
    yield from qlever_query_timed(instance, name + "_" + suffix, output, query)


def query_gtfs_no_geopoint(prev: Results, output: Path) -> SubResGen:
    "Run evaluation on QLever without GeoPoints, GTFS data"

    dep(prev, *PREP, pull_qlever, copy_data_gtfs_no_geopoint,
        index_gtfs_no_geopoint, start_gtfs_no_geopoint)
    instance = "gtfs-no-geopoint"

    # Distance to Berlin
    query = Path(PROGRAM_DIR, "reproduction", "qlever", "dist_to_berlin.rq")
    yield from qlever_query_timed(instance, "dist_to_berlin", output, query)

    # Benchmark on differently sized GTFS feeds
    for feed, size in GTFS_TEST_FEEDS.items():
        # No GeoPoint version only supports cartesian / plain-SPARQL
        # Here we cannot honor GTFS_MAX_DIST_CARTESIAN_ALL because
        # the old QLever version does not have a lazy cartesian product yet
        # and thus would run out of memory confronted with a
        # hundred-billon-rows cartesian product
        if size ** 2 <= CARTESIAN_LIMIT:
            yield from qlever_query_gtfs_max_dist(
                instance, feed, QleverGTFSBenchmarkMode.CARTESIAN, output)


def query_gtfs(prev: Results, output: Path) -> SubResGen:
    "Run evaluation queries on QLever with GTFS data set"
    dep(prev, *PREP, pull_qlever, index_gtfs, start_gtfs)

    # Max Dist Join: Stops that are within 25 meters of each other
    instance = "gtfs"

    # For information count stops per GTFS feed
    query = Path(PROGRAM_DIR, "reproduction",
                 "qlever", "differently_sized_feeds.rq")
    yield qlever_query(QLEVER_INSTANCES[instance],
                       "differently_sized_feeds", query, output, False)

    # Distance to Berlin
    query = Path(PROGRAM_DIR, "reproduction", "qlever", "dist_to_berlin.rq")
    yield from qlever_query_timed(instance, "dist_to_berlin", output, query)

    # Benchmark on differently sized GTFS feeds
    for feed, size in GTFS_TEST_FEEDS.items():
        # Using algorithm s2
        yield from qlever_query_gtfs_max_dist(instance, feed,
                                              QleverGTFSBenchmarkMode.S2,
                                              output)
        # Using algorithm baseline
        if size ** 2 <= CARTESIAN_LIMIT or GTFS_MAX_DIST_CARTESIAN_ALL:
            yield from qlever_query_gtfs_max_dist(
                instance, feed, QleverGTFSBenchmarkMode.BASELINE, output)
            yield from qlever_query_gtfs_max_dist(
                instance, feed, QleverGTFSBenchmarkMode.CARTESIAN, output)


def query_osm_de(prev: Results, output: Path) -> SubResGen:
    "Run evaluation queries on QLever OpenStreetMap Germany"
    dep(prev, *PREP, pull_qlever, index_osm_de, start_osm_de)

    instance = "osm-de"

    # For information count tags: differently_sized_tags.rq
    query = Path(PROGRAM_DIR, "reproduction",
                 "qlever", "differently_sized_tags.rq")
    yield qlever_query(QLEVER_INSTANCES[instance],
                       "differently_sized_tags", query, output, False)

    # Distance to Berlin
    query = Path(PROGRAM_DIR, "reproduction", "qlever", "dist_to_berlin.rq")
    yield from qlever_query_timed(instance, "dist_to_berlin", output, query)

    # Nearest Neighbor Join
    for mode in QleverNNBenchmarkMode:
        yield from qlever_benchmark_nearest_neighbor(instance, mode, output)

    # Very Large Composed Query
    query = Path(PROGRAM_DIR, "reproduction", "qlever", "big_eval_query.rq")
    yield from qlever_query_timed(instance, "big_eval_query", output, query)


def psql_query_timed(name: str, output: Path, query: Path) -> SubResGen:
    """
    Helper: run a query on PostgreSQL a few times and record the running times
    """

    if "postgres" in BYPASS_QUERY:
        log_info(f"Checking if benchmark query postgres_{name} has been run.")
        if Path(output, "results", "postgres_" + name + "_summary.json") \
                .exists():
            log_warning("Benchmark summary JSON file found. Skipping.")
            yield InternalStep(True)
            return
        log_info("Benchmark summary JSON file not found. Running query.")

    export_time(f"postgres_{name}", None, output)
    for i in range(QUERY_ITERATIONS):
        try:
            p = run_psql(f"{name}_{i}", query.name, output, query.parent)
            yield p
            export_time(f"postgres_{name}", p, output)
            if (p.end - p.start) / NS_MS > CANCEL_ITERATIONS_IF_LONGER_THAN:
                log_warning(f"The benchmark query postgres_{name} took " +
                            "longer than " +
                            str(CANCEL_ITERATIONS_IF_LONGER_THAN) +
                            " ms. Skipping further iterations.")
                break
            sleep(1)
        except subprocess.TimeoutExpired:
            # Query ran more than 24h
            summary = Path(output, "results", "postgres_" +
                           name + "_summary.json")
            with open(summary, "w") as f:
                json.dump({"error": "Timeout"}, f)
                yield InternalStep(True, f"Query postgres_{name} timed out")
                log_warning(f"The query postgres_{name} took more than 24h " +
                            "and was thus aborted.")
                return
    make_time_stats(f"postgres_{name}", output)


class PostgresNNBenchmarkMode(Enum):
    GIST_INDEX = ""
    ADHOC_INDEX = "_adhoc"
    CARTESIAN = "_cartesian"


def psql_query_nearest_neighbors(left: tuple[str, str],
                                 right: tuple[str, str],
                                 mode: PostgresNNBenchmarkMode,
                                 explain: bool,
                                 output: Path) -> SubResGen:
    "Helper: Preapare a nearest neighbor PostgreSQL run"
    name = "nearest_neighbor" + mode.value
    suffix = f"{left[0]}-{left[1] if left[1] != '*' else 'all'}_{right[0]}" + \
        f"-{right[1] if right[1] != '*' else 'all'}"
    assert re.match(r'^[0-9a-z_-]+$', name + "_" + suffix)

    query = make_sql(name, output, suffix, {
        "%TAG_LEFT%": left[0] + " " + (
            f"= '{left[1]}'" if left[1] != "*" else " IS NOT NULL"),
        "%TAG_RIGHT%": right[0] + " " + (
            f"= '{right[1]}'" if right[1] != "*" else " IS NOT NULL"),
        "/*EXPLAIN*/": ("EXPLAIN" if explain else "")
    })

    if explain:
        suffix += "_explain"

    # Explain needs to run only once. The normal query should run multiple
    # timed iterations
    if explain:
        if "postgres" in BYPASS_QUERY:
            log_info(
                f"Checking if query plan postgres_{name}_{suffix}.csv has " +
                "been saved.")
            if Path(output, "results", f"postgres_{name}_{suffix}.csv") \
                    .exists():
                log_warning("Query Plan CSV found. Skipping.")
                yield InternalStep(True)
                return
            log_info("Query Plan not found. Running explain query.")
        yield run_psql(f"{name}_{suffix}",
                       query.name, output, query.parent)
    else:
        yield from psql_query_timed(name + "_" + suffix, output, query)


def psql_benchmark_nearest_neighbors(mode: PostgresNNBenchmarkMode,
                                     output: Path) -> SubResGen:
    """
    Helper: Run a full nearest neighbor OpenStreetMap benchmark on PostgreSQL
    """
    for left, right in gen_tasks():
        ltag, lval, lsize = left
        rtag, rval, rsize = right
        if mode == PostgresNNBenchmarkMode.CARTESIAN \
                and lsize * rsize > CARTESIAN_LIMIT:
            continue
        if POSTGRES_SKIP_24H_QUERIES and \
                mode == PostgresNNBenchmarkMode.GIST_INDEX and \
                (ltag, lval, rtag, rval) in POSTGRES_24H_QUERIES:
            log_warning(
                f"The query for nearest neighbors, left: {ltag}={lval} to " +
                f"right: {rtag}={rval}, is known to reach the 24h timeout. " +
                "It will thus be skipped. To try running it anyway, use " +
                "--try-hopeless-queries.")
            continue
        for explain in (True, False):
            yield from psql_query_nearest_neighbors(
                (ltag, lval), (rtag, rval), mode, explain, output)


def query_postgres(prev: Results, output: Path) -> SubResGen:
    "Run evaluation queries on PostgreSQL"

    dep(prev, *PREP, create_postgres, osm2pgsql, postgres_tables_indexes,
        spatialjoin_for_postgres, postgres_import_spatial, start_postgres)

    # Distance to Berlin query
    query = Path(PROGRAM_DIR, "reproduction", "postgres", "dist_to_berlin.sql")
    yield from psql_query_timed("dist_to_berlin", output, query)

    # Benchmark Nearest Neighbor Join on differently sized left and right and
    # with different approaches (GiST-index on the full table, Ad-hoc GiST
    # index on the selected subset, Full cartesian product join)
    for mode in PostgresNNBenchmarkMode:
        yield from psql_benchmark_nearest_neighbors(mode, output)

    # Very Large Spatial Query with multiple nearest neighbor searches of
    # different size
    query = Path(PROGRAM_DIR, "reproduction", "postgres", "big_eval_query.sql")
    yield from psql_query_timed("big_eval_query", output, query)


def export_eval_results(prev: Results, output: Path) -> SubResGen:
    "Export results of the evaluation as csv tables"

    dep(prev, *PREP,
        query_osm_de,
        query_gtfs,
        query_gtfs_no_geopoint,
        query_postgres)

    # Summarize and export the execution_times_cache
    with open(Path(output, "results", "time_stats.csv"), "w") as f:
        fieldnames = ["name", "min", "1q", "median",
                      "3q", "max", "mean", "stdev"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for step_name in execution_times_cache:
            m = {k: v for k, v in make_time_stats(
                step_name, None).items() if k in fieldnames}
            writer.writerow({"name": step_name} | m)

    # Dummy
    yield InternalStep(True)


EVALUATION: Steps = [
    # expected time: ca. 68 h
    start_osm_de,  # ca. 33 sec
    query_osm_de,  # ca. 2 h 49 min
    stop_osm_de,  # ca. 1 sec
    start_gtfs,  # ca. 3 sec
    query_gtfs,  # ca. 5 min 9 sec (+ CartesianAll)
    stop_gtfs,  # ca. 1 sec
    start_gtfs_no_geopoint,  # ca. 34 sec
    query_gtfs_no_geopoint,  # ca. 2 min 26 sec
    stop_gtfs_no_geopoint,  # ca. 1 sec
    start_postgres,  # ca. 10 sec
    # update_postgres_config,
    # stop_postgres,
    # start_postgres,
    query_postgres,  # ca. 61 h 40 min, with hopless queries additional 2 x 24h
    # + big eval query 2 h 48 min
    stop_postgres,  # ca. 1 sec
    export_eval_results  # ca. 1 sec
]

###############################################################################


def compose_queries(prev: Results, output: Path) -> SubResGen:
    "Compose election queries"

    dep(prev, *PREP, build_spatial_container)

    yield main_container("btw21_compose", Path(output, "results"))
    yield main_container("ew24_compose", Path(output, "results"))


def case_study_queries(prev: Results, output: Path) -> SubResGen:
    "Run election queries"
    dep(prev, *PREP, build_spatial_container, get_data_election,
        get_data_gtfs, get_data_osm_de, index_election, index_gtfs,
        index_osm_de, start_osm_de, start_gtfs, start_election)

    if BYPASS_CASE_STUDY_QUERY:
        log_info("Bypass: Checking btw21.tsv and ew24.tsv")

        def helper(fn: str, exp: int) -> SubResGen:
            p = SimpleSubproc.run(f"cat {fn}.tsv | wc -l",
                                  Path(output, "results"))
            i = 0
            try:
                i = int(p.stdout)
            except Exception as e:
                yield InternalStep(False, "", str(e))
            yield p
            assert i == exp, f"Unexpected number of rows in {fn}.tsv: {i}"

        yield from helper("btw21", 300)
        yield from helper("ew24", 401)

        yield InternalStep(True)
        return

    log_warning(
        "These queries are very resource-demanding. Please ensure the " +
        "requirements are met. Empty results and errors from boost URL " +
        "library are typical symptoms of insufficient RAM.")

    yield qlever_query(QLEVER_INSTANCES["election"], "btw21",
                       Path(output, "results", "btw21.rq"), output, False)
    yield SimpleSubproc.run("cp qlever_7926_btw21.tsv btw21.tsv",
                            Path(output, "results"))

    yield qlever_query(QLEVER_INSTANCES["election"], "ew24",
                       Path(output, "results", "ew24.rq"), output, False)
    yield SimpleSubproc.run("cp qlever_7926_ew24.tsv ew24.tsv",
                            Path(output, "results"))


def case_study_statistics(prev: Results, output: Path) -> SubResGen:
    "Run statistical evaluation on query results and export results"
    dep(prev, *PREP, build_stat_repro_container, case_study_queries)
    res = str(Path(output, "results"))
    yield SimpleSubproc.run(
        f"podman run --rm -v {quote(res)}:/data:rw spatial_stat_repro")


CASE_STUDY: Steps = [
    # expected time: 23 min
    start_election,  # ca. 1 sec
    start_gtfs,  # ca. 3 sec
    start_osm_de,  # ca. 33 sec
    compose_queries,  # ca. 1 sec
    case_study_queries,  # ca. 22 min
    stop_election,  # ca. 1 sec
    stop_gtfs,  # ca. 1 sec
    stop_osm_de,  # ca. 1 sec
    case_study_statistics  # ca. 6 sec
]

###############################################################################


def remove_qlever_indexes(prev: Results, output: Path) -> SubResGen:
    "Remove the QLever indexes"
    dep(prev, *PREP)
    for d in QLEVER_INSTANCES:
        yield SimpleSubproc.run(
            "rm -rf " + " ".join(QLEVER_INDEX_FILES_GLOB + ["*.partial.*"]),
            Path(output, d))


def remove_temporary_files(prev: Results, output: Path) -> SubResGen:
    "Remove temporary files and intermediate results"
    dep(prev, *PREP)
    yield SimpleSubproc.run("rm -rf " + EXTRACT_PROG_TMP_NAME, output)
    yield SimpleSubproc.run("rm -rf " + EXTRACT_PROG_DIR, output)


def remove_datasets(prev: Results, output: Path) -> SubResGen:
    "Remove the computed turtle datasets and OSM PBF file"
    dep(prev, *PREP)
    yield SimpleSubproc.run("rm osm-de/osm-germany.pbf", output)
    yield SimpleSubproc.run("find . -name '*.ttl.bz2' -exec rm {} \\+", output)
    yield SimpleSubproc.run(
        "find . -name '*-aux-geo.tsv' -exec rm {} \\+", output)


def prune_containers(prev: Results, output: Path) -> SubResGen:
    "Remove exited podman containers"
    dep(prev, *PREP)
    yield SimpleSubproc.run("podman container prune --force", output)


def prune_container_images(prev: Results, output: Path) -> SubResGen:
    "Remove podman container images"
    dep(prev, *PREP)
    yield SimpleSubproc.run("podman image prune --all --force", output)


CLEAN_UP: Steps = [
    # expected time: 30 sec
    remove_qlever_indexes,  # ca. 10 sec
    remove_temporary_files,  # ca. 1 sec
    remove_datasets,  # ca. 10 sec
    prune_containers,  # ca. 5 sec
    prune_container_images  # ca. 5 sec
]


def export_step_times_csv(prev: Results, output: Path) -> SubResGen:
    "Export the running times of subprocesses"

    dep(prev, *PREP, *EVALUATION, *CASE_STUDY)

    # All previous subprocesses and their running times
    with open(Path(output, "results", "step_times_by_id.csv"), "w") as f, \
            open(Path(output, "results", "step_times_sum.csv"), "w") as sf:
        writer = csv.writer(f)
        writer.writerow([
            "step", "identifier", "returncode", "start", "end", "duration"])
        writer_sum = csv.writer(sf)
        writer_sum.writerow(["step", "duration"])
        for step_name, step_results in prev.items():
            _sum = 0
            for step_result in step_results:
                for result in step_result.subresults:
                    match result:
                        case SimpleSubproc():
                            _sum += result.end - result.start
                            if result.identifier:
                                writer.writerow([
                                    step_name, result.identifier,
                                    result.returncode, result.start,
                                    result.end,
                                    (result.end - result.start) / NS_MS])
            writer_sum.writerow([step_name, _sum / NS_MS])

    # Dummy
    yield InternalStep(True)


ALL_STEPS: Steps = [
    # The order is very important here!
    *PREP,  # ca. 1 sec
    *BUILD_CONTAINERS,  # ca. 8 min
    *GET_DATA,  # ca. 3 h 32 min
    *CREATE_INDEXES,  # ca. 11 h 30 min
    *EVALUATION,  # ca. 68 h
    *CASE_STUDY,  # ca. 23 min
    *CLEAN_UP,  # ca. 30 sec
    export_step_times_csv  # ca. 1 sec
    # over all expected time: 83.5 h
]

###############################################################################


def parse_arguments(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    """
    Parse the program's command-line arguments
    """
    parser = argparse.ArgumentParser(
        description=PROGRAM_DESCRIPTION)

    # Optional
    parser.add_argument(
        '--output', '-o', nargs=1, type=str, default=".",
        help='directory where the results and intermediate computations' +
        ' should be stored (default: current working directory)')
    parser.add_argument(
        '--list-steps', '-l', action='store_true',
        help='Output a list of steps with their identifiers and exit.'
    )
    parser.add_argument(
        '--skip', type=str, nargs='+',
        help='Skip steps (multiple possible, step identifiers as given by ' +
        ' --list-steps). Other steps that depend on excluded ones will ' +
        'fail. --skip has priority over --only.'
    )
    parser.add_argument(
        '--only', type=str, nargs='+',
        help='Skip steps that are not explicitly listed (multiple possible, ' +
        'step identifiers as given by --list-steps). Other steps that ' +
        'depend on excluded ones will fail. If used, all preparation steps ' +
        'are included whether given or not. --skip has priority over --only.'
    )
    parser.add_argument(
        '--only-postgres', action='store_true',
        help='Shorthand: Run only the PostgreSQL evaluation'
    )
    parser.add_argument(
        '--only-qlever-osm-de', action='store_true',
        help='Shorthand: Run only the QLever evaluation on OSM Germany'
    )
    parser.add_argument(
        '--only-qlever-gtfs', action='store_true',
        help='Shorthand: Run only the QLever evaluation on the GTFS data sets'
    )
    parser.add_argument(
        '--only-qlever-gtfs-no-geopoint', action='store_true',
        help='Shorthand: Run only the QLever no-GeoPoint-evaluation on ' +
        'the GTFS data sets'
    )
    parser.add_argument(
        '--only-qlever', action='store_true',
        help='Shorthand: Run only the QLever evaluation on all data sets'
    )
    parser.add_argument(
        '--only-case-study', action='store_true',
        help='Shorthand: Run only the case study'
    )
    parser.add_argument(
        '--bypass-mem', action='store_true',
        help='Bypass the check for enough free memory - only warn ' +
        '(at the risk of failing)'
    )
    parser.add_argument(
        '--bypass-disk', action='store_true',
        help='Bypass the check for enough free disk space - only warn ' +
        '(at the risk of failing)'
    )
    parser.add_argument(
        '--bypass-build', action='store_true',
        help='Bypass the building/pulling of container images - only check ' +
        'that the required images are present (at the risk of failing)'
    )
    parser.add_argument(
        '--bypass-get-data', type=str, nargs='+',
        choices=list(QLEVER_INSTANCES.keys()) + ["postgres"],
        help='Bypass the download and generation of required data sets - ' +
        'only check that they are present (at the risk of failing)'
    )
    parser.add_argument(
        '--bypass-index-build', type=str, nargs='+',
        choices=list(QLEVER_INSTANCES.keys()) + ["postgres"],
        help='Bypass the building of indexes for the given data sets - only ' +
        'check that they are present (at the risk of failing)'
    )
    parser.add_argument(
        '--bypass-query', type=str, nargs='+',
        choices=list(QLEVER_INSTANCES.keys()) + ["postgres"],
        help='Bypass rerunning the benchmark queries - check if they have ' +
        'been run and only run if they are missing'
    )
    parser.add_argument(
        '--bypass-case-study-query', action='store_true',
        help='Bypass rerunning the SPARQL queries for the case study - ' +
        'only check if the results are present (at the risk of failing)'
    )
    parser.add_argument(
        '--tidy', '-t', action='store_true',
        help='By default the cleaning up steps that delete indexes and ' +
        'other intermediate files are skipped. Activate them using this ' +
        'option.'
    )
    parser.add_argument(
        '--try-hopeless-queries', action='store_true',
        help='Try running queries anyway that are known to exceed the 24h ' +
        'timeout. Evaluation on PostgreSQL with this option can take up to ' +
        'one week.'
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true',
        help='Print more information'
    )
    return parser.parse_args(argv)


def get_step_info(step: Step) -> tuple[str, str]:
    "Extract the name and description of a `Step`"
    name = step.__name__
    doc_str = step.__doc__
    doc_lines = [
        line.strip()
        for line in (doc_str or "").splitlines()
        if line.strip()
    ]
    desc = doc_lines[0] if doc_lines else ""
    return (name, desc)


def main(argv: Optional[Sequence[str]] = None) -> bool:
    """
    The procedure to be run when the program is called directly.
    """

    args = parse_arguments(argv)

    if args.list_steps:
        # Helper variables for table-like formatting
        width = max(len(s.__name__) for s in ALL_STEPS)
        width_doc = max(len(s.__doc__ or "") for s in ALL_STEPS)
        col1 = "Identifier"
        col2 = "Description"
        pad_left = (len(str(len(ALL_STEPS))) * 2 + 3)
        pad_middle = (width - len(col1))

        # Table head
        print(' ' * pad_left + '\t' + col1 + ' ' * pad_middle + '\t' + col2)
        print('-' * pad_left + '\t' + '-' * width + '\t' + '-' * width_doc)

        # Table body
        for i, step in enumerate(ALL_STEPS):
            name, desc = get_step_info(step)
            print(AnsiEsc.BOLD.value +
                  f"[{i+1}/{len(ALL_STEPS)}]\t{name}" +
                  AnsiEsc.NORMAL.value +
                  f"{(width - len(name)) * ' '}\t{desc}")

        # End program after listing steps
        return True

    print(PROGRAM_DESCRIPTION)
    logger.info("arguments: %s", repr(args))
    assert Path(SHELL).exists()

    if os.environ.get("SPATIAL_CONTAINER"):
        log_error(
            "This program should not be run inside the container, because " +
            "it needs to start containers of its own. Please use " +
            "'make export_reproduction' to copy a bundled version to the " +
            "container's '/output' directory. You can then execute this " +
            "program on your host system. There it will check for its " +
            "required dependencies automatically.")
        return False

    output = Path(args.output[0]).resolve()
    if output == PROGRAM_DIR:
        log_error("The programs --output directory should not be the same as" +
                  " the location of the program file.")
        return False

    global BYPASS_MEMORY_CHECK
    BYPASS_MEMORY_CHECK = bool(args.bypass_mem)
    global BYPASS_DISK_CHECK
    BYPASS_DISK_CHECK = bool(args.bypass_disk)
    global BYPASS_CONTAINER_BUILDS
    BYPASS_CONTAINER_BUILDS = bool(args.bypass_build)
    global BYPASS_GET_DATA
    BYPASS_GET_DATA = args.bypass_get_data or []
    global BYPASS_INDEX_BUILD
    BYPASS_INDEX_BUILD = args.bypass_index_build or []
    global BYPASS_QUERY
    BYPASS_QUERY = args.bypass_query or []
    global POSTGRES_SKIP_24H_QUERIES
    POSTGRES_SKIP_24H_QUERIES = not args.try_hopeless_queries
    global BYPASS_CASE_STUDY_QUERY
    BYPASS_CASE_STUDY_QUERY = bool(args.bypass_case_study_query)
    global VERBOSE
    VERBOSE = bool(args.verbose)

    # Prepare skipping of steps
    def steps2set(steps: Steps) -> set[str]:
        return set(step.__name__ for step in steps)

    skip_steps: set[str] = set(args.skip or [])
    only: set[str] = set(args.only or [])

    # Apply shorthands
    if args.only_postgres:
        only.update({
            "build_postgres_container",
            "build_spatialjoin_container",
            "copy_qleverfiles",
            "create_postgres",
            "get_data_postgres",
            "osm2pgsql",
            "postgres_tables_indexes",
            "collect_index_size_pg_no_sr",
            "postgres_export_for_spatial",
            "spatialjoin_for_postgres",
            "postgres_import_spatial",
            "stop_postgres",
            "start_postgres",
            "query_postgres"
        })

    qlever_prepare = {
        "pull_qlever",
        "build_spatial_container",
        "build_osm2rdf",
        "copy_qleverfiles"
    }
    qlever_osm_de = {
        "get_data_election",
        "get_data_osm_de",
        "index_osm_de",
        "start_osm_de",
        "stop_osm_de"
    }
    qlever_gtfs = {
        "get_data_gtfs",
        "index_gtfs",
        "start_gtfs",
        "stop_gtfs"
    }
    qlever_gtfs_no_geopoint = {
        "get_data_gtfs",
        "copy_data_gtfs_no_geopoint",
        "index_gtfs_no_geopoint",
        "start_gtfs_no_geopoint",
        "stop_gtfs_no_geopoint"
    }
    qlever_election = {
        "get_data_election",
        "index_election",
        "start_election",
        "stop_election"
    }

    if args.only_qlever_osm_de or args.only_qlever:
        only.update(qlever_prepare, qlever_osm_de, {"query_osm_de"})
    if args.only_qlever_gtfs_no_geopoint or args.only_qlever:
        only.update(qlever_prepare, qlever_gtfs_no_geopoint,
                    {"query_gtfs_no_geopoint"})
    if args.only_qlever_gtfs or args.only_qlever:
        only.update(qlever_prepare, qlever_gtfs, {"query_gtfs"})
    if args.only_case_study or args.only_qlever:
        only.update(qlever_prepare, qlever_osm_de,
                    {"build_stat_repro_container"},
                    qlever_gtfs, qlever_election, steps2set(CASE_STUDY))

    if only:
        skip_steps.update(steps2set(ALL_STEPS) - (steps2set(PREP) | only))

    if not args.tidy:
        skip_steps.update(steps2set(CLEAN_UP))

    results: Results = {}

    for i, step in enumerate(ALL_STEPS):
        name, desc = get_step_info(step)
        log_important(f"[{i + 1}/{len(ALL_STEPS)}]", "Step:", desc)
        if name in skip_steps:
            msg = "Skipping this step."
            if step in CLEAN_UP:
                msg += " To activate this step, you may use the --tidy option."
            log_warning(msg)
            continue
        res = StepResult()
        try:
            for subresult in step(results, output):
                res.add(subresult)
        except Exception as e:
            if str(e):
                log_error(e)
            res.add(InternalStep(False, "", f"Exception: {e}"))
        if not res.success:
            log_error(f"Step \"{desc}\" was not successful. Reasons:")
            for j, subresult in enumerate(res.subresults):
                if not subresult.success:
                    log_error(f"Subtask #{j + 1} failed.",
                              subresult.stderr.rstrip())
            if step in PREP:
                log_error("Indispensable preparation step failed. Exiting.")
                return False
        else:
            log_success(f"Step \"{desc}\" successfully completed.")
        if name not in results:
            results[name] = []
        results[name].append(res)

        # Take backup
        if "mkdirs" in results:
            with open(Path(output, "results", "results.json"), "w") as f:
                json.dump(results, f, cls=EnhancedJSONEncoder)

    # Summarize + Check all results
    log_important("Done. All steps exited. Summary:")
    success, error, error_steps = 0, 0, set()
    for k, v in results.items():
        for item in v:
            if item.success:
                success += 1
            else:
                error += 1
                error_steps.add(k)
    if error == 0:
        log_success("Everything successful.")
        return True
    else:
        log_error(
            f"Had {success} successful steps, {error} failed steps.")
        log_error("Failed steps:", json.dumps(list(error_steps)))
        return False


if __name__ == "__main__":
    if not main():
        exit(1)
