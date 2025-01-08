\COPY (SELECT osm_id, ST_AsText(ST_Union(ST_Transform(way, 4326))) FROM (SELECT osm_id, way FROM planet_osm_point UNION SELECT osm_id, way FROM planet_osm_line UNION SELECT osm_id, way FROM planet_osm_polygon) GROUP BY osm_id) TO '/output/geometries.tsv' CSV DELIMITER E'\t'
-- The ST_Union is required because of multigeometries that are represented as multiple rows in osm2pgsql
-- 22GB for OSM-DE