/*EXPLAIN*/
SELECT AVG(min_dist) AS avg_min_dist
FROM (
  SELECT MIN(
    ST_Distance(ST_Transform(left_.centroid, 4326)::geography,
    ST_Transform(right_.centroid, 4326)::geography)) / 1000 AS min_dist
  FROM (
    SELECT left_.osm_id, left_.centroid
    FROM osm_centroids AS left_
    WHERE left_.%TAG_LEFT%
  ) AS left_
  CROSS JOIN (
    SELECT right_.centroid 
    FROM osm_centroids AS right_
    WHERE right_.%TAG_RIGHT%
  ) AS right_
  GROUP BY left_.osm_id
);
