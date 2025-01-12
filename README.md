# Efficient Spatial Search for the QLever SPARQL Engine

This repository contains all programs for the workflow presented in the thesis ["Efficient Spatial Search for the QLever SPARQL Engine" (PDF)](https://ullinger.info/bachelor-thesis/Efficient_Spatial_Search_for_the_QLever_SPARQL_Engine.pdf).

## Abstract

We present an end-to-end workflow for efficiently performing geographic searches for nearest neighbors with the QLever SPARQL engine. Our solution significantly reduces the time and the users' effort required to combine and query spatial data from multiple sources.

QLever allows working with points in the standardized *Well-Known Text* format, which it now stores efficiently.
Searching for geographically close points in data sets containing hundreds of millions of points becomes a matter of seconds using QLever's new spatial search capabilities. A fast algorithm based on a spatial index is presented as well as a proof of concept baseline algorithm. The spatial search is carefully integrated into the SPARQL syntax.

We introduce programs for the conversion of data from multiple formats (*Keyhole Markup Language*, *Comma-Separated Values* and *General Transit Feed Specification*) to RDF. Additionally, a new program allows users to construct complex spatial queries for QLever with a graphical user interface.

The retrieval of data, which would otherwise require working with many different data sets individually, is now possible in a single SPARQL query. We demonstrate the usability of our workflow using a current research question from political science.

Furthermore, we show that our efficient spatial search implementation in QLever surpasses the query performance of the popular PostgreSQL system by orders of magnitude for large inputs. Regarding all benchmarks, our implementation shows more stable running times.


## Usage

All contributions to the QLever SPARQL engine can be used directly via [QLever's official source code](https://github.com/ad-freiburg/qlever) or the [QLever docker image](https://hub.docker.com/r/adfreiburg/qlever). The spatial search feature is described on the [QLever Wiki](https://github.com/ad-freiburg/qlever/wiki/GeoSPARQL-support-in-QLever).

The new programs introduced in the thesis can be used as follows:

```bash
# Clone the repository and enter its directory:
git clone https://github.com/ullingerc/spatial-search.git
cd spatial-search
# Build and start the container image:
podman build -t spatial .
podman run --rm -it -v ./output:/output:rw -p 7990:7990 spatial
# Inside the container, get more information:
make help
```

The container provides the following programs:

- Conversion of External Data Sets to RDF Turtle
  - Keyhole Markup Language (KML) - `kml2rdf.py`
  - Comma-separated Values (CSV) - `csv2rdf.py`
  - General Transit Feed Specification (GTFS) - `gtfs2rdf.py`
  - Election Data - `election2rdf.py`
- Generation of Spatial SPARQL queries for QLever
  - Command-Line Interface and Graphical User Interface - `compose_spatial.py`
- Reproduction of the Thesis' Evaluation and Case Study
  - ability to export `run_reproduction` standalone program (to be run on the host system)

The Python programs do not have any external dependencies. They can also be used standalone without a container after cloning the repository. You may get more information using the `--help` option of each program for CLI usage and in the docstrings for library usage.

Unit tests, `coverage.py` and the `flake8` style checker are included in the container. For type checking, `pyright` is used. It needs to be installed manually in the container if required.

## License

All source code and documentation in this repository is licensed under the GNU General Public License version 3 or later. For more details see the `LICENSE` file.

If you use any of the code or results in published works, please cite *Christoph Ullinger, "Efficient Spatial Search for the QLever SPARQL Engine". 2025. <https://ullinger.info/spatial-search-2025>*.
