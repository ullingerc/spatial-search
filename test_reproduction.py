#!/bin/env python3
"""
Unit tests for the reproduction script

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

from contextlib import nullcontext
from dataclasses import dataclass
import reproduction
import unittest
from typing import Optional, Callable
from pathlib import Path
from unittest.mock import patch, call
import json
import tempfile
import os
import re
import itertools


SubProcRunMatcher = tuple[str, Optional[Path], Optional[bool |
                       str | Path]] | tuple[str, Optional[Path]]
# Matcher: status code, stdout, stderr, identifier
SubProcFakeResult = tuple[int, str, str, Optional[str]]
MockSubProcRes = dict[SubProcRunMatcher, SubProcFakeResult]


def mock_subproc_run(mock_subproc_results: MockSubProcRes) \
    -> Callable[[str, Optional[Path], bool | str | Path],
                reproduction.SimpleSubproc]:
    """
    Creates a mock replacement for SimpleSubproc.run, which returns the results
    given by the dict
    """

    def mock_subproc_run_actual(command: str, cwd: Optional[Path] = None,
                                fwd_output: bool | str | Path = False) \
            -> reproduction.SimpleSubproc:
        """
        Mock replacement for SimpleSubproc.run, which does not actually execute
        a subprocess.
        """
        subproc_args = (command, cwd, fwd_output)
        if subproc_args not in mock_subproc_results and not fwd_output:
            subproc_args = (command, cwd)
        assert subproc_args in mock_subproc_results, repr(
            subproc_args) + " was called, but not expected"
        returncode, stdout, stderr, identifier = \
            mock_subproc_results[subproc_args]
        return reproduction.SimpleSubproc(command, cwd,
                                          None, returncode, stdout, stderr,
                                          1, 2, identifier)

    return mock_subproc_run_actual


class TestReproduction(unittest.TestCase):
    def ok(self) -> list[reproduction.StepResult]:
        "Helper to generate an successful dummy step"
        return [reproduction.StepResult([reproduction.InternalStep(True)])]

    @property
    def dummy(self) -> SubProcFakeResult:
        return (0, '', '', None)

    def test_json_enc(self):
        @dataclass
        class Demo:
            x: int
            y: str
        d = {"something": Demo(1, "a"), "x": [Demo(2, "b")]}
        self.assertEqual(
            json.dumps(d, cls=reproduction.EnhancedJSONEncoder),
            '{"something": {"x": 1, "y": "a"}, "x": [{"x": 2, "y": "b"}]}')

        class NoDataclass:
            x: int

            def __init__(self, x: int):
                self.x = x

        with self.assertRaises(TypeError):
            d2 = {"something": NoDataclass(1)}
            json.dumps(d2, cls=reproduction.EnhancedJSONEncoder)

    def test_internal_step(self):
        self.assertEqual(reproduction.InternalStep(True).returncode, 0)
        self.assertEqual(reproduction.InternalStep(False).returncode, 1)

    @patch("reproduction.VERBOSE", False)
    def test_simple_subproc(self):
        with self.assertLogs('reproduction', level='INFO') as log:
            sp = reproduction.SimpleSubproc.run("echo test", None)
            self.assertEqual(sp.command, "echo test")
            self.assertIsNone(sp.cwd)
            self.assertIsNone(sp.log_path)
            self.assertTrue(sp.success)
            self.assertIsNotNone(sp.time)
            self.assertEqual(sp.returncode, 0)

            self.assertEqual(len(log.output), 1)
            self.assertIn("echo test", log.output[0])

        with self.assertLogs('reproduction', level='INFO') as log:
            sp = reproduction.SimpleSubproc.run("echo test; exit 5", None)
            self.assertEqual(sp.command, "echo test; exit 5")
            self.assertIsNone(sp.cwd)
            self.assertIsNone(sp.log_path)
            self.assertFalse(sp.success)
            self.assertIsNotNone(sp.time)
            self.assertEqual(sp.returncode, 5)

            self.assertEqual(len(log.output), 1)
            self.assertIn("echo test; exit 5", log.output[0])

        with self.assertLogs('reproduction', level='INFO') as log:
            fn = tempfile.mktemp()
            sp = reproduction.SimpleSubproc.run("echo test", None, Path(fn))
            self.assertEqual(sp.command, "echo test")
            self.assertIsNone(sp.cwd)
            self.assertEqual(sp.log_path, Path(fn))
            self.assertTrue(sp.success)
            self.assertIsNotNone(sp.time)
            self.assertEqual(sp.returncode, 0)

            self.assertEqual(len(log.output), 2)
            self.assertIn("echo test", log.output[0])
            self.assertIn("tail -f " + fn, log.output[1])

            with open(fn, "r") as f:
                n = f.read()
            self.assertEqual(n, "test\n")

    def test_step_result(self):
        res = reproduction.StepResult()
        self.assertTrue(res.success)
        self.assertEqual(res.subresults, [])
        res.add(reproduction.InternalStep(True))
        self.assertTrue(res.success)
        self.assertEqual(res.subresults, [reproduction.InternalStep(True)])

        res = reproduction.StepResult(
            [reproduction.SimpleSubproc("", None, None, 1, "", "", 1, 2)])
        self.assertFalse(res.success)

    def test_log_functions(self):
        arg = ("x", 1, 3.5, "x")
        self.assertEqual(reproduction._str(arg), "x 1 3.5 x")
        self.assertEqual(reproduction._str(("x",)), "x")
        self.assertEqual(reproduction._str(()), "")

        expect = "x 1 3.5 x"

        with patch('reproduction.logger.info') as mock:
            reproduction.log_info(*arg)
            mock.assert_called_once()
            self.assertIn(expect, mock.call_args[0][0])

        with patch('reproduction.logger.error') as mock:
            reproduction.log_error(*arg)
            mock.assert_called_once()
            self.assertIn(expect, mock.call_args[0][0])

        with patch('reproduction.logger.warning') as mock:
            reproduction.log_warning(*arg)
            mock.assert_called_once()
            self.assertIn(expect, mock.call_args[0][0])

        with patch('reproduction.logger.info') as mock:
            reproduction.log_success(*arg)
            mock.assert_called_once()
            self.assertIn(expect, mock.call_args[0][0])

        with patch('reproduction.logger.info') as mock:
            reproduction.log_command(*arg)
            mock.assert_called_once()
            self.assertIn(expect, mock.call_args[0][0])

        with patch('reproduction.logger.info') as mock:
            reproduction.log_important(*arg)
            mock.assert_called_once()
            self.assertIn(expect, mock.call_args[0][0])

    def test_dep(self):
        reproduction.dep({
            "check_dependencies": self.ok()
        }, reproduction.check_dependencies)

        reproduction.dep({
            "check_dependencies": self.ok(),
            "check_free_memory": self.ok()
        }, reproduction.check_free_memory, reproduction.check_dependencies)

        with self.assertRaisesRegex(
                AssertionError,
                'Step requires \'check_dependencies\' to be run first'):
            reproduction.dep({}, reproduction.check_dependencies)

        with self.assertRaisesRegex(
                AssertionError, 'This step requires the step ' +
                '\'check_dependencies\' to be successful, but it failed'):
            reproduction.dep({
                "check_dependencies": [
                    reproduction.StepResult([reproduction.InternalStep(False)])
                ]
            }, reproduction.check_dependencies)

        with self.assertRaisesRegex(
                AssertionError, 'This step requires the step ' +
                '\'check_dependencies\' to be successful, but it failed'):
            reproduction.dep({
                "check_dependencies": [
                    reproduction.StepResult([
                        reproduction.InternalStep(True),
                        reproduction.InternalStep(False),
                        reproduction.InternalStep(True)
                    ])
                ]
            }, reproduction.check_dependencies)

    def test_get_step_info(self):
        def example1(prev: reproduction.Results, output: Path) \
                -> reproduction.SubResGen:
            """
            Example Text

            Bla Bla 123
            """
            yield reproduction.InternalStep(True)

        self.assertEqual(list(example1({}, Path("."))),
                         [reproduction.InternalStep(True)])
        self.assertTupleEqual(reproduction.get_step_info(example1),
                              ("example1", "Example Text"))

    def test_list_steps(self):
        with patch("builtins.print") as mock_print:
            rt = reproduction.main(["--list-steps"])
            self.assertTrue(rt)
            output = "\n".join(" ".join(_call.args)
                               for _call in mock_print.call_args_list)
            for f in reproduction.ALL_STEPS:
                self.assertIn(f.__name__, output)
                self.assertIn((f.__doc__ or "").splitlines()[0].strip(),
                              output)

    def test_spatial_container(self):
        with patch("os.environ.get") as mock_env, \
            self.assertLogs("reproduction", level="ERROR") as logs, \
                patch('builtins.print'):
            mock_env.return_value = "1"
            rt = reproduction.main([])
            self.assertIn(call("SPATIAL_CONTAINER"), mock_env.call_args_list)
            self.assertFalse(rt)
            self.assertTrue(any(
                "This program should not be run inside the container" in m
                for m in logs.output
            ))

    def test_equal_dirs(self):
        with self.assertLogs("reproduction", level="ERROR") as logs, \
                patch("os.environ.get") as mock_env, \
                patch('builtins.print'):
            mock_env.return_value = ""
            rt = reproduction.main(["-o", str(reproduction.PROGRAM_DIR)])
            self.assertFalse(rt)
            self.assertTrue(any(
                "The programs --output directory should not be the same as" +
                " the location of the program file." in m
                for m in logs.output
            ))

    def simulate_main_with_options(self,
                                   additional_cli_args: list[str] = [],
                                   override_expected: MockSubProcRes = {},
                                   expect_error_message: Optional[str] = None,
                                   with_bypass: bool = False,
                                   check_logs: bool = True
                                   ) -> str:
        """
        This helper function simulates a complete run of reproduction.py
        and patches almost everything that would have external impacts.
        All subprocess calls are checked against a dictionary of expected
        subprocess calls and return the fake result from the dictionary.
        """
        with tempfile.TemporaryDirectory() as tmpdir, \
                patch('reproduction.SimpleSubproc.run') as mock, \
                patch('os.environ.get') as mock_environ, \
                patch('reproduction.sleep'), \
                patch('reproduction.check_container_status') \
                as mock_c_status, \
                patch('reproduction.Path.glob') as mock_glob, \
                (self.assertLogs("reproduction", level="INFO")
                 if check_logs else nullcontext()) as logs, \
                patch('reproduction.print') as mock_print:

            mock_c_status.return_value = iter(
                [reproduction.InternalStep(True)])
            mock_glob.return_value = ["funny-file.txt"]

            fakefiles = {
                f"{tmpdir}/election/btw21.ttl.bz2": 900_000,
                f"{tmpdir}/election/btw21-aux-geo.tsv": 900_000,
                f"{tmpdir}/election/ew24.ttl.bz2": 900_000,
                f"{tmpdir}/osm-de/osm-germany.ttl.bz2": 150_000_000_000,
                f"{tmpdir}/osm-de/osm-germany.pbf": 5_000_000_000,
                f"{tmpdir}/results/geometries.tsv": 25_000_000_000,
                f"{tmpdir}/results/geometries_spatialjoin.csv": 90_000_000_000,
            }
            for i in ("gtfs", "gtfs-no-geopoint"):
                fakefiles |= {
                    f"{tmpdir}/{i}/delfi21.ttl.bz2": 1_000_000_000,
                    f"{tmpdir}/{i}/delfi24.ttl.bz2": 1_000_000_000,
                    f"{tmpdir}/{i}/vag24.ttl.bz2": 50_000_000,
                    f"{tmpdir}/{i}/fintraffic24.ttl.bz2": 50_000_000,
                }

            @dataclass
            class FakeStatResult:
                st_mode = 33188
                st_ino = 100
                st_dev = 5
                st_nlink = 1
                st_uid = 1000
                st_gid = 1000
                st_size = 100
                st_atime = 1736175139
                st_mtime = 1736175139
                st_ctime = 1736175139

            def fakestat(self: Path, *, follow_symlinks: bool = True):
                s = str(self)
                if s in fakefiles:
                    f = FakeStatResult()
                    f.st_size = fakefiles[s]
                    return f
                return os.stat(str(self))

            # We fake Path.stat on files
            reproduction.Path.stat = fakestat  # type: ignore

            # Fake environment
            env: dict[str, str] = {
                "XDG_DATA_HOME": str(Path(tmpdir) / "podman")
            }
            mock_environ.side_effect = lambda k: env.get(k)

            prog_dir = Path(reproduction.PROGRAM_DIR)

            expected: MockSubProcRes = {
                # Disk space check
                (f"df --output=avail -B 1 {tmpdir} | tail -n 1", None):
                (0, str(5 * (2**40)), "", None),
                (f"df --output=avail -B 1 {tmpdir}/podman | tail -n 1", None):
                (0, str(5 * (2**40)), "", None),

                # Mem check
                ("cat /proc/meminfo", None):
                (0, "\nMemAvailable:   180586500 kB\n", "", None),

                # Extract files
                ("find . -name '*.py' -exec chmod +x {} \\+", Path(prog_dir)):
                self.dummy,
                ("find . -name '*.sh' -exec chmod +x {} \\+", Path(prog_dir)):
                self.dummy,

                # Creation of PostgreSQL container / tables
                ('podman start eval_postgres', Path(tmpdir),
                 Path(tmpdir, 'log/start_postgres.log')): self.dummy,
                (f'podman run -dt -v {tmpdir}/results:/output:rw ' +
                 f'-v {tmpdir}/osm-de:/mnt:ro --name eval_postgres '
                 '--replace eval_postgres', Path(tmpdir),
                 Path(tmpdir, 'log/create_postgres.log')): self.dummy,
                ('podman container inspect eval_postgres', None): self.dummy,
                ('podman stop eval_postgres', Path(tmpdir),
                 Path(tmpdir, 'log/stop_postgres.log')): self.dummy,
                ('podman exec -i -u postgres eval_postgres psql --csv -d ' +
                 f'osm < {prog_dir}/reproduction/postgres/drop.sql',
                 Path(tmpdir)): self.dummy,

                # OSM Import PostgreSQL
                ('podman exec -u postgres eval_postgres osm2pgsql ' +
                 '--database osm /mnt/osm-germany.pbf', Path(tmpdir),
                 Path(tmpdir, 'log/osm2pgsql.log')): self.dummy,
                ('podman exec -u postgres eval_postgres bash -c "du -bcs ' +
                 '/var/lib/postgresql/data | tail -n 1 | grep -oP ' +
                 '\'[0-9]+\'"', None): (0, '75113189301\n', '', None),

                # Spatial relations for PostgreSQL
                ('podman exec eval_postgres bash -c "touch ' +
                 '/output/geometries.tsv && chmod a+rw /output/' +
                 'geometries.tsv"', None): self.dummy,
                ('podman run --memory=50G --rm -v ./:/mnt spatialjoin sh -c' +
                 ' "spatialjoin --intersects \' 1 \' --contains \' 2 \' ' +
                 '--covers \' 3 \' --touches \' 4 \' --equals \' 5 \' ' +
                 '--overlaps \' 6 \' --crosses \' 7 \' < /mnt/geometries.tsv' +
                 ' > /mnt/geometries_spatialjoin.csv"',
                 Path(tmpdir, 'results'),
                 Path(tmpdir, 'log/pg_spatialjoin.log')): self.dummy,
                ('podman exec -i -u postgres eval_postgres psql --csv -d '
                 f'osm < {prog_dir}/reproduction/postgres/check_' +
                 'spatialrelations.sql', Path(tmpdir)):
                (0, 'x\n5000000', '', None),

                # Meta info queries
                ("curl 'http://localhost:7925/' --fail -X POST -H 'Accept: " +
                 "text/tab-separated-values' -H 'Content-Type: application/" +
                 f"sparql-query' --data-binary @{prog_dir}/reproduction/" +
                 f"qlever/differently_sized_tags.rq -o {tmpdir}/results/" +
                 "qlever_7925_differently_sized_tags.tsv", None): self.dummy,

                ("curl 'http://localhost:7928/' --fail -X POST -H 'Accept: " +
                 "text/tab-separated-values' -H 'Content-Type: application/" +
                 f"sparql-query' --data-binary @{prog_dir}/reproduction/" +
                 f"qlever/differently_sized_feeds.rq -o {tmpdir}/results/" +
                 "qlever_7928_differently_sized_feeds.tsv", None): self.dummy,

                # Case study
                ('podman run --rm -it -v ./:/output:rw spatial make ' +
                 'btw21_compose', Path(tmpdir, 'results')): self.dummy,
                ('podman run --rm -it -v ./:/output:rw spatial make ' +
                 'ew24_compose', Path(tmpdir, 'results')): self.dummy,

                # Tidy
                ('rm -rf temp_reproduction_code.zip', Path(tmpdir)):
                self.dummy,
                ('rm -rf program_dir', Path(tmpdir)): self.dummy,
                ('rm osm-de/osm-germany.pbf', Path(tmpdir)): self.dummy,
                ('find . -name \'*.ttl.bz2\' -exec rm {} \\+', Path(tmpdir)):
                self.dummy,
                ("find . -name '*-aux-geo.tsv' -exec rm {} \\+", Path(tmpdir)):
                self.dummy,
                ("podman container prune --force", Path(tmpdir)): self.dummy,
                ("podman image prune --all --force", Path(tmpdir)): self.dummy,
            }

            for p in reproduction.REQUIRED_ON_PATH:
                expected[(f"which '{p}'", Path(tmpdir))] = \
                    (0, f"/bin/{p}", "", None)

            for d in reproduction.OUTPUT_SUBDIRS:
                expected[(f'mkdir -p {tmpdir}/{d}', Path(tmpdir))] = self.dummy
                os.mkdir(Path(tmpdir) / d)

            expected_with_bypass: MockSubProcRes = {
                # Container checks
                ('podman image inspect localhost/spatial', None): self.dummy,
                ('podman image inspect localhost/spatial_stat_repro', None):
                self.dummy,
                ('podman image inspect docker.io/adfreiburg/qlever:' +
                 'commit-01d8306', None): self.dummy,
                ('podman image inspect docker.io/adfreiburg/qlever:' +
                 'commit-6384041', None): self.dummy,
                ('podman image inspect localhost/osm2rdf', None): self.dummy,
                ('podman image inspect localhost/eval_postgres', None):
                self.dummy,
                ('podman image inspect localhost/spatialjoin', None):
                self.dummy,

                # PostgreSQL table checks
                ('podman exec -i -u postgres eval_postgres psql --csv -d ' +
                 f'osm < {prog_dir}/reproduction/postgres/check_osm2pgsql.sql',
                 Path(tmpdir)): (0, 'x\n5000000\n5000000\n5000000', '', None),
                ('podman exec -i -u postgres eval_postgres psql --csv -d ' +
                 f'osm < {prog_dir}/reproduction/postgres/check_osm_' +
                 'centroids.sql', Path(tmpdir)): (0, 'x\n5000000', '', None),

                # Case Study checks
                ('cat btw21.tsv | wc -l', Path(tmpdir, "results")):
                (0, '300', '', None),
                ('cat ew24.tsv | wc -l', Path(tmpdir, "results")):
                (0, '401', '', None),
            }

            expected_without_bypass: MockSubProcRes = {
                # Container builds
                ('podman build -t spatial .', Path(prog_dir),
                 Path(tmpdir, 'log/build_spatial.log')): self.dummy,
                ('podman build -t spatial_stat_repro --file=Dockerfile.' +
                 'stat_repro .', Path(f'{prog_dir}/reproduction/stat'),
                 Path(tmpdir, 'log/build_stat_repro.log')): self.dummy,
                ('podman pull docker.io/adfreiburg/qlever:commit-01d8306',
                 Path(prog_dir), Path(tmpdir, 'log/pull_qlever.log')):
                self.dummy,
                ('podman pull docker.io/adfreiburg/qlever:commit-6384041',
                 Path(prog_dir), Path(tmpdir, 'log/pull_qlever.log')):
                self.dummy,
                ('podman build -t osm2rdf --file=Dockerfile.osm2rdf .',
                 Path(f'{prog_dir}/reproduction/qlever'),
                 Path(tmpdir, 'log/build_osm2rdf_repro.log')): self.dummy,
                ('podman build -t eval_postgres --file=Dockerfile.postgres .',
                 Path(f'{prog_dir}/reproduction/postgres'),
                 Path(tmpdir, 'log/build_postgres.log')): self.dummy,
                ('podman build -t spatialjoin --file=Dockerfile.spatialjoin .',
                 Path(f'{prog_dir}/reproduction/postgres'),
                 Path(tmpdir, 'log/build_spatialjoin.log')): self.dummy,
            }

            if with_bypass:
                expected |= expected_with_bypass
            else:
                expected |= expected_without_bypass

            pg_setup = ["create_osm_centroids", "idx_centroids", "idx_text",
                        "idx_text", "create_spatialrelations",
                        "export_for_spatialjoin", "import_spatialjoin",
                        "idx_spatialrelations"]
            for query in pg_setup:
                expected[(
                    'podman exec -i -u postgres eval_postgres psql --csv ' +
                    f'-d osm < {prog_dir}/reproduction/postgres/{query}.sql',
                    Path(tmpdir))] = self.dummy

            pois = ['leisure-sauna', 'railway-station', 'tourism-viewpoint',
                    'shop-supermarket', 'amenity-restaurant', 'amenity-bench',
                    'building-all']
            pg_modes = ("", "_cartesian", "_adhoc")
            it = (*range(10), "explain")

            for mode, i, left, right in itertools.product(
                    pg_modes, it, pois, pois):
                if left == right:
                    continue

                expected[(
                    'podman exec -i -u postgres eval_postgres psql --csv ' +
                    f'-d osm --quiet < {tmpdir}/results/postgres_nearest_' +
                    f'neighbor{mode}_{left}_{right}.sql > {tmpdir}/results/' +
                    f'postgres_nearest_neighbor{mode}_{left}_{right}_{i}.csv',
                    Path(tmpdir),
                    Path(tmpdir, 'log/run_postgres_nearest_neighbor' +
                         f'{mode}_{left}_{right}_{i}.log')
                )] = (
                    0, 'x\n1', '',
                    f'nearest_neighbor{mode}_{left}_{right}_{i}'
                )

            ql_modes = ("", "_baseline")
            for mode_, i, left, right in itertools.product(
                    ql_modes, it, pois, pois):
                if left == right:
                    continue
                expected[(
                    "curl 'http://localhost:7925/' --fail -X POST -H " +
                    "'Accept: text/tab-separated-values' -H 'Content-Type: " +
                    f"application/sparql-query' --data-binary @{tmpdir}/" +
                    f"results/qlever_nearest_neighbor{mode_}_{left}_{right}" +
                    f".rq -o {tmpdir}/results/qlever_7925_qlever_osm-de_" +
                    f"nearest_neighbor{mode_}_{left}_{right}_{i}.tsv", None
                )] = (
                    0, 'x\n1', '',
                    f'qlever_osm-de_nearest_neighbor{mode_}_{left}_{right}_{i}'
                )

            other_benchmark_queries = ("dist_to_berlin", "big_eval_query")

            for q, i in itertools.product(
                    other_benchmark_queries, range(10)):
                expected[(
                    'podman exec -i -u postgres eval_postgres psql --csv ' +
                    f'-d osm --quiet < {prog_dir}/reproduction/postgres/{q}' +
                    f'.sql > {tmpdir}/results/postgres_{q}_{i}.csv',
                    Path(tmpdir),
                    Path(tmpdir, f'log/run_postgres_{q}_{i}.log')
                )] = (0, 'x\n400', '', f'{q}_{i}')

            gtfs_ql = (("gtfs", 7928), ("gtfs-no-geopoint", 7931))
            gtfs_ql_modes = ("_s2", "_cartesian", "_baseline")
            gtfs_feeds = ("delfi_20210924", "delfi_20240603",
                          "vag_2024", "fintraffic_2024")

            for (instance, port), i, mode, feed in itertools.product(
                    gtfs_ql, range(10), gtfs_ql_modes, gtfs_feeds):
                if instance == "gtfs-no-geopoint" and mode != "_cartesian":
                    continue
                expected[(
                    f"curl 'http://localhost:{port}/' --fail -X POST -H " +
                    "'Accept: text/tab-separated-values' -H 'Content-Type: " +
                    f"application/sparql-query' --data-binary @{tmpdir}/" +
                    f"results/qlever_gtfs_max_dist{mode}_{feed}.rq -o " +
                    f"{tmpdir}/results/qlever_{port}_qlever_{instance}_gtfs" +
                    f"_max_dist{mode}_{feed}_{i}.tsv", None
                )] = (
                    0, 'x\n1', '',
                    f'qlever_{instance}_gtfs_max_dist{mode}_{feed}_{i}'
                )

            for (instance, port), i in itertools.product(gtfs_ql, range(10)):
                expected[(
                    f"curl 'http://localhost:{port}/' --fail -X POST -H " +
                    "'Accept: text/tab-separated-values' -H 'Content-Type: " +
                    f"application/sparql-query' --data-binary @{prog_dir}/" +
                    f"reproduction/qlever/dist_to_berlin.rq -o {tmpdir}/" +
                    f"results/qlever_{port}_qlever_{instance}_dist_to_" +
                    f"berlin_{i}.tsv", None
                )] = (
                    0, 'x\n1', '', f'qlever_{instance}_dist_to_berlin_{i}'
                )

            for i in range(10):
                expected[(
                    "curl 'http://localhost:7925/' --fail -X POST -H " +
                    "'Accept: text/tab-separated-values' -H 'Content-Type: " +
                    f"application/sparql-query' --data-binary @{prog_dir}/" +
                    f"reproduction/qlever/big_eval_query.rq -o {tmpdir}/" +
                    f"results/qlever_7925_qlever_osm-de_big_eval_query_{i}" +
                    ".tsv", None
                )] = (
                    0, 'x\ty\tz\n1\t2\t3', '',
                    f'qlever_osm-de_big_eval_query_{i}'
                )

                expected[(
                    "curl 'http://localhost:7925/' --fail -X POST -H " +
                    "'Accept: text/tab-separated-values' -H 'Content-Type: " +
                    f"application/sparql-query' --data-binary @{prog_dir}/" +
                    f"reproduction/qlever/dist_to_berlin.rq -o {tmpdir}/" +
                    f"results/qlever_7925_qlever_osm-de_dist_to_berlin_{i}" +
                    ".tsv", None
                )] = (
                    0, 'x\n1', '', f'qlever_osm-de_dist_to_berlin_{i}'
                )

            ql_instances = tuple(reproduction.QLEVER_INSTANCES.items())
            for instance, port in ql_instances:
                expected |= {
                    (
                        f'cp {prog_dir}/reproduction/qlever/' +
                        f'Qleverfile-{instance}.ini {tmpdir}/{instance}/' +
                        'Qleverfile', Path(prog_dir)
                    ): self.dummy,
                    (
                        "du -bc  *.index.* *-log.txt *.meta-data.json " +
                        "*.settings.json *.vocabulary.* | tail -n 1 | " +
                        "grep -oP '[0-9]+'", Path(tmpdir, instance)
                    ): (0, '5000000000', '', None),
                    (
                        f"curl 'http://localhost:{port}/' -X POST -H " +
                        "'Accept: application/qlever-results+json' -H " +
                        "'Content-Type: application/x-www-form-urlencoded' " +
                        "--data 'cmd=clear-cache'", None
                    ): (0, '{"x":"y"}', '', None),
                    (
                        'rm -rf *.index.* *-log.txt *.meta-data.json ' +
                        '*.settings.json *.vocabulary.* *.partial.*',
                        Path(tmpdir, instance)
                    ): self.dummy,
                }

            ql_commands = (
                ("start", "start"),
                ("index --overwrite-existing", "index"),
                ("get-data", "get_data"),
                ("stop", "stop")
            )
            for (instance, port), (op, n) in itertools.product(
                    ql_instances, ql_commands):
                expected[(
                    f'qlever {op}', Path(f'{tmpdir}/{instance}'),
                    Path(tmpdir, f'log/{n}_{instance}.log')
                )] = self.dummy

                with open(Path(tmpdir, f'log/{n}_{instance}.log'), "w") as f:
                    f.write("INFO: Ok\n")

            expected |= {
                (f'cp {prog_dir}/election/prefixes.ttl ' +
                 f'{tmpdir}/osm-de', None): self.dummy,
                (f'cp {tmpdir}/election/btw21-aux-geo.tsv ' +
                 f'{tmpdir}/osm-de', None): self.dummy,
                (f'cp {tmpdir}/gtfs/delfi21.ttl.bz2 ' +
                 f'{tmpdir}/gtfs-no-geopoint', None): self.dummy,
                (f'cp {tmpdir}/gtfs/delfi24.ttl.bz2 ' +
                 f'{tmpdir}/gtfs-no-geopoint', None): self.dummy,
                (f'cp {tmpdir}/gtfs/vag24.ttl.bz2 ' +
                 f'{tmpdir}/gtfs-no-geopoint', None): self.dummy,
                (f'cp {tmpdir}/gtfs/fintraffic24.ttl.bz2 ' +
                 f'{tmpdir}/gtfs-no-geopoint', None): self.dummy,
            }

            expected |= {
                (f'mkdir -p {tmpdir}/osm-de', Path(tmpdir)): self.dummy,
                (f'mkdir -p {tmpdir}/election', Path(tmpdir)): self.dummy,
                (f'mkdir -p {tmpdir}/gtfs', Path(tmpdir)): self.dummy,
                (f'mkdir -p {tmpdir}/gtfs-no-geopoint', Path(tmpdir)):
                self.dummy,
                (f'mkdir -p {tmpdir}/results', Path(tmpdir)): self.dummy,
                (f'mkdir -p {tmpdir}/log', Path(tmpdir)): self.dummy,
            }

            # Case study
            if not with_bypass:
                expected |= {
                    (
                        "curl 'http://localhost:7926/' --fail -X POST -H " +
                        "'Accept: text/tab-separated-values' -H 'Content-" +
                        "Type: application/sparql-query' --data-binary " +
                        f"@{tmpdir}/results/btw21.rq -o {tmpdir}/results/" +
                        "qlever_7926_btw21.tsv", None
                    ): self.dummy,
                    (
                        "curl 'http://localhost:7926/' --fail -X POST -H " +
                        "'Accept: text/tab-separated-values' -H 'Content-" +
                        "Type: application/sparql-query' --data-binary " +
                        f"@{tmpdir}/results/ew24.rq -o {tmpdir}/results/" +
                        "qlever_7926_ew24.tsv", None
                    ): self.dummy,
                    ('cp qlever_7926_btw21.tsv btw21.tsv',
                     Path(tmpdir, 'results')): self.dummy,
                    ('cp qlever_7926_ew24.tsv ew24.tsv',
                     Path(tmpdir, 'results')): self.dummy,
                }
            expected[(
                f'podman run --rm -v {tmpdir}/results:/data:rw ' +
                'spatial_stat_repro', None)] = self.dummy

            with open(f"{tmpdir}/results/qlever_7926_btw21.tsv", "w") as f:
                f.write(300 * "x\n")
            with open(f"{tmpdir}/results/qlever_7926_ew24.tsv", "w") as f:
                f.write(401 * "x\n")

            # Run the actual check
            override_expected_replaced = {}
            for k, v in override_expected.items():
                if "{tmpdir}" in k[0]:
                    k = (k[0].replace("{tmpdir}", tmpdir), *k[1:])
                override_expected_replaced[k] = v
            mock.side_effect = mock_subproc_run(
                expected | override_expected_replaced)
            self.assertEqual(reproduction.main(
                ["-o", tmpdir, "--verbose"] + additional_cli_args),
                expect_error_message is None)
            mock_print.assert_called()
            if check_logs and hasattr(logs, "output"):
                output = getattr(logs, "output")
                full_log = "\n".join(output)
                if not expect_error_message:
                    self.assertNotIn("ERROR", full_log)
                    self.assertIn("Everything successful.", full_log)
                    for i in range(1, 59):
                        self.assertIn(f"[{i}/58]", full_log)
                else:
                    self.assertTrue(any(
                        "ERROR" in msg and len(re.findall(
                            expect_error_message, msg)) > 0
                        for msg in output
                    ))

                return full_log
            else:
                return ""

    def test_main(self):
        "Test the entire program with all subprocess calls"
        self.simulate_main_with_options(["--tidy"])

    def test_main_bypass(self):
        "Test the entire program with all subprocess calls, but using bypasses"
        self.simulate_main_with_options(
            additional_cli_args=[
                "--bypass-mem",
                "--bypass-disk",
                "--bypass-build",
                "--bypass-get-data",
                "osm-de", "election", "gtfs", "gtfs-no-geopoint", "postgres",
                "--bypass-index-build",
                "osm-de", "election", "gtfs", "gtfs-no-geopoint", "postgres",
                "--bypass-query",
                "osm-de", "election", "gtfs", "gtfs-no-geopoint", "postgres",
                "--bypass-case-study-query"
            ], with_bypass=True)

    def test_main_only(self):
        "Test the entire program, but using --only"
        output = self.simulate_main_with_options(
            additional_cli_args=[
                # Selecting everything using only
                "--only-postgres",
                "--only-qlever-osm-de",
                "--only-qlever-gtfs",
                "--only-qlever-gtfs-no-geopoint",
                "--only-qlever",
                "--only-case-study"
            ]
        )
        self.assertEqual(output.count("Skipping this step."), 8)

    def test_insufficient_mem(self):
        self.simulate_main_with_options(
            override_expected={
                ("cat /proc/meminfo", None): (0, """
                MemAvailable:   80586500 kB
                """, "", None)
            },
            expect_error_message="The free memory .* is insufficient"
        )

    def test_insufficient_mem_bypass(self):
        self.simulate_main_with_options(
            override_expected={
                ("cat /proc/meminfo", None): (0, """
                MemAvailable:   80586500 kB
                """, "", None)
            },
            expect_error_message=None,  # !
            additional_cli_args=["--bypass-mem"]
        )

    def test_main_subprocess_failure(self):
        # Failed steps
        self.simulate_main_with_options(
            override_expected={
                (
                    'podman run --rm -v {tmpdir}/results:/data:rw ' +
                    'spatial_stat_repro', None
                ): (1, "", "", None)
            },
            expect_error_message='Failed steps'
        )

    def test_insufficient_disk(self):
        self.simulate_main_with_options(
            override_expected={
                ("df --output=avail -B 1 {tmpdir} | tail -n 1", None):
                (0, str(5 * (2**30)), "", None),
                ("df --output=avail -B 1 {tmpdir}/podman | tail -n 1", None):
                (0, str(5 * (2**40)), "", None),
            },
            expect_error_message="The directory .* has insufficient " +
            "free space available"
        )
        self.simulate_main_with_options(
            override_expected={
                ("df --output=avail -B 1 {tmpdir} | tail -n 1", None):
                (0, str(5 * (2**40)), "", None),
                ("df --output=avail -B 1 {tmpdir}/podman | tail -n 1", None):
                (0, str(5 * (2**30)), "", None),
            },
            expect_error_message="The directory .* has insufficient " +
            "free space available"
        )

    def test_insufficient_disk_bypass(self):
        self.simulate_main_with_options(
            override_expected={
                ("df --output=avail -B 1 {tmpdir} | tail -n 1", None):
                (0, str(5 * (2**30)), "", None),
                ("df --output=avail -B 1 {tmpdir}/podman | tail -n 1", None):
                (0, str(5 * (2**30)), "", None),
            },
            expect_error_message=None,  # !
            additional_cli_args=["--bypass-disk"]
        )

    def test_check_container_status(self):
        expected = [
            {
                "State": {
                    "Status": "running"
                }
            }
        ]
        with patch('reproduction.SimpleSubproc.run') as mock:
            mock.return_value = reproduction.InternalStep(
                True, json.dumps(expected), "")

            self.assertListEqual(
                list(reproduction.check_container_status(
                    "my-container", "running")),
                [
                    mock.return_value,
                    reproduction.InternalStep(True),
                    reproduction.InternalStep(True)
                ]
            )

            mock.assert_called_once_with(
                "podman container inspect my-container")


if __name__ == '__main__':
    unittest.main()
