BEGIN;
CREATE TEMPORARY TABLE right_cache (centroid geometry);
INSERT INTO right_cache SELECT centroid FROM osm_centroids WHERE %TAG_RIGHT%;
CREATE INDEX idx_right_cache ON right_cache USING GIST (centroid);

/*EXPLAIN*/
SELECT AVG(left_.centroid::geography <-> right_.centroid::geography) / 1000 AS avg_min_dist
FROM (
  SELECT osm_id, centroid
  FROM osm_centroids
  WHERE %TAG_LEFT%
) AS left_
CROSS JOIN LATERAL (
  SELECT right_.centroid, right_.centroid <-> left_.centroid AS dist
  FROM right_cache AS right_
  ORDER BY dist
  LIMIT 1
) AS right_;

ROLLBACK;
