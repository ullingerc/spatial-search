# Dockerfile for the csv2rdf.py, kml2rdf.py, gtfs2rdf.py, election2rdf.py and compose_spatial.py programs
FROM debian:bookworm
LABEL maintainer="Christoph Ullinger <ullingec@informatik.uni-freiburg.de>"

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y python3 python3-flake8 python3-coverage \
    bash-completion curl sed zip unzip make grep && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /app && mkdir /output && mkdir /data

ENV PATH="$PATH:/app"
ENV SPATIAL_CONTAINER=1
COPY . /app

# Ensure working code
RUN cd /app && make test && make checkstyle && rm -rf __pycache__

WORKDIR /app
CMD ["bash", "--rcfile", "/app/.bashrc"]

# Usage:
# podman build -t spatial .
# podman run --rm -it -v /path/to/output:/output:rw -p 7990:7990 spatial
