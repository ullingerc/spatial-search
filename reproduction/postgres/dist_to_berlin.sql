SELECT
  AVG(ST_Distance(
    ST_Transform(centroid, 4326)::geography,
    ST_PointFromText('POINT(13.369661 52.524945)')::geography
  )) / 1000 AS dist
FROM osm_centroids;
