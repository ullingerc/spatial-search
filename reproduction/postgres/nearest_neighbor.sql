-- This is the official way to do a nearest neighbor join in PostGIS, as described in https://www.postgis.net/workshops/postgis-intro/knn.html#nearest-neighbor-join

/*EXPLAIN*/
SELECT AVG(left_.centroid::geography <-> right_.centroid::geography) / 1000 AS avg_min_dist
FROM (
  SELECT osm_id, centroid FROM osm_centroids WHERE %TAG_LEFT%
) AS left_
CROSS JOIN LATERAL (
  SELECT right_.centroid, right_.centroid <-> left_.centroid AS dist
  FROM osm_centroids AS right_
  WHERE right_.%TAG_RIGHT%
  ORDER BY dist
  LIMIT 1
) AS right_;
