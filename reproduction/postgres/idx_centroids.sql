CREATE INDEX idx_centroids ON osm_centroids USING gist (centroid);
