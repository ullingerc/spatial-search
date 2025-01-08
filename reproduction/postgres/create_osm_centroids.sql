DROP TABLE IF EXISTS osm_centroids;
-- The same columns as in the tables from osm2pgsql (colons to underscores) minus zindex and way plus centroids
CREATE TABLE osm_centroids(
    "osm_id" BIGINT PRIMARY KEY, -- Primary Key due to multi-geometries being aggregated into a single centroid
    "access" TEXT,
    "addr_housename" TEXT,
    "addr_housenumber" TEXT,
    "addr_interpolation" TEXT,
    "admin_level" TEXT,
    "aerialway" TEXT,
    "aeroway" TEXT,
    "amenity" TEXT,
    "area" TEXT,
    "barrier" TEXT,
    "bicycle" TEXT,
    "boundary" TEXT,
    "brand" TEXT,
    "bridge" TEXT,
    "building" TEXT,
    "construction" TEXT,
    "covered" TEXT,
    "culvert" TEXT,
    "cutting" TEXT,
    "denomination" TEXT,
    "disused" TEXT,
    "embankment" TEXT,
    "foot" TEXT,
    "generator_source" TEXT,
    "harbour" TEXT,
    "highway" TEXT,
    "historic" TEXT,
    "horse" TEXT,
    "intermittent" TEXT,
    "junction" TEXT,
    "landuse" TEXT,
    "layer" TEXT,
    "leisure" TEXT,
    "lock" TEXT,
    "man_made" TEXT,
    "military" TEXT,
    "motorcar" TEXT,
    "name" TEXT,
    "natural" TEXT,
    "office" TEXT,
    "oneway" TEXT,
    "operator" TEXT,
    "place" TEXT,
    "population" TEXT,
    "power" TEXT,
    "power_source" TEXT,
    "public_transport" TEXT,
    "railway" TEXT,
    "ref" TEXT,
    "religion" TEXT,
    "route" TEXT,
    "service" TEXT,
    "shop" TEXT,
    "sport" TEXT,
    "surface" TEXT,
    "toll" TEXT,
    "tourism" TEXT,
    "tower_type" TEXT,
    "tunnel" TEXT,
    "water" TEXT,
    "waterway" TEXT,
    "wetland" TEXT,
    "width" TEXT,
    "wood" TEXT,
    "centroid" GEOMETRY);

INSERT INTO osm_centroids
    SELECT
        osm_id, ANY_VALUE("access"), ANY_VALUE("addr:housename"), ANY_VALUE("addr:housenumber"), ANY_VALUE("addr:interpolation"), ANY_VALUE("admin_level"), ANY_VALUE("aerialway"), ANY_VALUE("aeroway"), ANY_VALUE("amenity"), ANY_VALUE("area"), ANY_VALUE("barrier"), ANY_VALUE("bicycle"), ANY_VALUE("boundary"), ANY_VALUE("brand"), ANY_VALUE("bridge"), ANY_VALUE("building"), ANY_VALUE("construction"), ANY_VALUE("covered"), ANY_VALUE("culvert"), ANY_VALUE("cutting"), ANY_VALUE("denomination"), ANY_VALUE("disused"), ANY_VALUE("embankment"), ANY_VALUE("foot"), ANY_VALUE("generator:source"), ANY_VALUE("harbour"), ANY_VALUE("highway"), ANY_VALUE("historic"), ANY_VALUE("horse"), ANY_VALUE("intermittent"), ANY_VALUE("junction"), ANY_VALUE("landuse"), ANY_VALUE("layer"), ANY_VALUE("leisure"), ANY_VALUE("lock"), ANY_VALUE("man_made"), ANY_VALUE("military"), ANY_VALUE("motorcar"), ANY_VALUE("name"), ANY_VALUE("natural"), ANY_VALUE("office"), ANY_VALUE("oneway"), ANY_VALUE("operator"), ANY_VALUE("place"), ANY_VALUE("population"), ANY_VALUE("power"), ANY_VALUE("power_source"), ANY_VALUE("public_transport"), ANY_VALUE("railway"), ANY_VALUE("ref"), ANY_VALUE("religion"), ANY_VALUE("route"), ANY_VALUE("service"), ANY_VALUE("shop"), ANY_VALUE("sport"), ANY_VALUE("surface"), ANY_VALUE("toll"), ANY_VALUE("tourism"), ANY_VALUE("tower:type"), ANY_VALUE("tunnel"), ANY_VALUE("water"), ANY_VALUE("waterway"), ANY_VALUE("wetland"), ANY_VALUE("width"), ANY_VALUE("wood"), ST_Centroid(ST_Union(ST_Transform(way, 4326)))
    FROM (
        SELECT
            osm_id, "access", "addr:housename", "addr:housenumber", "addr:interpolation", "admin_level", "aerialway", "aeroway", "amenity", "area", "barrier", "bicycle", "boundary", "brand", "bridge", "building", "construction", "covered", "culvert", "cutting", "denomination", "disused", "embankment", "foot", "generator:source", "harbour", "highway", "historic", "horse", "intermittent", "junction", "landuse", "layer", "leisure", "lock", "man_made", "military", "motorcar", "name", "natural", "office", "oneway", "operator", "place", "population", "power", "power_source", "public_transport", "railway", "ref", "religion", "route", "service", "shop", "sport", "surface", "toll", "tourism", "tower:type", "tunnel", "water", "waterway", "wetland", "width", "wood", way
        FROM planet_osm_point
        UNION
        SELECT
            osm_id, "access", "addr:housename", "addr:housenumber", "addr:interpolation", "admin_level", "aerialway", "aeroway", "amenity", "area", "barrier", "bicycle", "boundary", "brand", "bridge", "building", "construction", "covered", "culvert", "cutting", "denomination", "disused", "embankment", "foot", "generator:source", "harbour", "highway", "historic", "horse", "intermittent", "junction", "landuse", "layer", "leisure", "lock", "man_made", "military", "motorcar", "name", "natural", "office", "oneway", "operator", "place", "population", "power", "power_source", "public_transport", "railway", "ref", "religion", "route", "service", "shop", "sport", "surface", "toll", "tourism", "tower:type", "tunnel", "water", "waterway", "wetland", "width", "wood", way
        FROM planet_osm_line
        UNION
        SELECT
            osm_id, "access", "addr:housename", "addr:housenumber", "addr:interpolation", "admin_level", "aerialway", "aeroway", "amenity", "area", "barrier", "bicycle", "boundary", "brand", "bridge", "building", "construction", "covered", "culvert", "cutting", "denomination", "disused", "embankment", "foot", "generator:source", "harbour", "highway", "historic", "horse", "intermittent", "junction", "landuse", "layer", "leisure", "lock", "man_made", "military", "motorcar", "name", "natural", "office", "oneway", "operator", "place", "population", "power", "power_source", "public_transport", "railway", "ref", "religion", "route", "service", "shop", "sport", "surface", "toll", "tourism", "tower:type", "tunnel", "water", "waterway", "wetland", "width", "wood", way
        FROM planet_osm_polygon
    ) GROUP BY osm_id
ON CONFLICT DO NOTHING;
