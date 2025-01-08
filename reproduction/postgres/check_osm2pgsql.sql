SELECT COUNT(*) AS count FROM planet_osm_point
UNION
SELECT COUNT(*) AS count FROM planet_osm_line
UNION
SELECT COUNT(*) AS count FROM planet_osm_polygon;
