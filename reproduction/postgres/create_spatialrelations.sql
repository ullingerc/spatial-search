DROP TABLE IF EXISTS spatialrelations;
CREATE TABLE IF NOT EXISTS spatialrelations (
    osm_left BIGINT,
    relation SMALLINT,
    osm_right BIGINT
);