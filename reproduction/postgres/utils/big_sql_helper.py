# Helper to produce evaluation sql
print("""
-- Generated using utils/big_sql_helper.py. Please do not change this file.
BEGIN;

-- building

CREATE TEMPORARY TABLE building (centroid geometry);
INSERT INTO building SELECT centroid FROM (
SELECT osm_id, centroid FROM osm_centroids
WHERE building IN ('altenheim', 'apartements', 'apartment',
'apartment_building', 'apartments', 'apartments;civic', 'apartments;house',
'apartmentsq', 'apartments;residential', 'apartments;yes', 'app',
'appartements', 'Appartements', 'appartment', 'appartments', 'block_house',
'building', 'yes', 'civic', 'datached', 'detached;apartments',
'detached_house', 'detached;yes', 'detached;yes;', 'domitory', 'dormitory',
'double_house', 'farm_house', 'farmhouse', 'hall_of_residence', 'home',
'house;', 'house;apartments', 'houseboat', 'housebuilding=semidetached_house',
 'housem', 'house;residential', 'houses', 'house semi', 'house=terraced',
 'house;yes', 'housing', 'Landhaus', 'dwelling_house', 'mobile_home',
 'residential', 'semidetached_house', 'semidetached', 'terraced_house',
 'terrace_house')
EXCEPT SELECT c.osm_id AS osm_id, c.centroid AS centroid
FROM spatialrelations s
JOIN osm_centroids c ON (s.osm_right = c.osm_id AND s.relation = 2)
WHERE osm_left IN (
SELECT osm_id FROM osm_centroids WHERE landuse IN ('industrial', 'cemetery',
'commercial', 'retail', 'landfill', 'garages', 'military', 'religious',
'education', 'highway', 'depot', 'fairground', 'storage', 'motorway',
'harbour', 'garage') OR amenity  IN ('industrial', 'cemetery', 'commercial',
'retail', 'landfill', 'garages', 'military', 'religious', 'education',
'highway', 'depot', 'fairground', 'storage', 'motorway', 'harbour', 'garage'))
-- AND c.building IS NOT NULL
);

-- Index not necessary for left table
-- CREATE INDEX idx_building ON building USING GIST (centroid);

-- transport

CREATE TEMPORARY TABLE transport (centroid geometry);
INSERT INTO transport SELECT centroid FROM osm_centroids WHERE
amenity IN ('bus', 'bus_shelter', 'bus station', 'bus_station', 'school_bus',
'bus_station', 'bus_stop', 'train_station') OR public_transport IN
('platform', 'station', 'halt', 'stop') OR railway IN ('platform',
'station', 'halt', 'stop');
CREATE INDEX idx_transport ON transport USING GIST (centroid);
""")


POIS = {
    "transport": None,  # Manually
    "supermarket": ['supermarket', 'grocery_store', 'convenience'],
    "bakery": ['bakery'],
    "butcher": ['butcher'],
    "fuel": ['fuel'],
    "gastronomy": ['bar', 'biergarten', 'cafe', 'fast_food',
                   'pub', 'restaurant'],
    "hairdresser": ['hairdresser'],
    "hospital": ['hospital', 'clinic'],
    "kindergarten": ['kindergarten'],
    "motorway": ['motorway_link'],
    "pharmacy": ['pharmacy', 'chemist'],
    "school": ['school'],
    "university": ['university', 'college']
}

for p, v in POIS.items():
    if not v:
        continue  # for transport
    sel = ""
    if len(v) == 1:
        sel = f"= '{v[0]}'"
    else:
        sel = "IN (" + ", ".join(f"'{x}'" for x in v) + ")"
    sel_ = f"amenity {sel} OR building {sel} OR shop {sel} OR landuse {sel}"
    if v == ["motorway_link"]:
        sel_ = "highway = 'motorway_link'"
    print(f"""

    -- {p}

    CREATE TEMPORARY TABLE {p} (centroid geometry);
    INSERT INTO {p} SELECT centroid FROM osm_centroids
    WHERE {sel_};
    CREATE INDEX idx_{p} ON {p} USING GIST (centroid);

    """)

print("-----------")
print("SELECT ")

s = []
for p in POIS:
    x = f"left_.centroid::geography <-> right_{p}.centroid::geography"
    s.append(f"AVG({x}) / 1000 AS avg_dist_{p}")
    s.append(f"STDDEV_SAMP({x}) / 1000 AS stdev_dist_{p}")
print(",\n".join(s))

print("""
FROM (
  SELECT centroid
  FROM building
) AS left_
""")

for p in POIS:
    print(f"""
    CROSS JOIN LATERAL (
        SELECT  right_{p}.centroid,
                right_{p}.centroid <-> left_.centroid AS dist_{p}
        FROM {p} AS right_{p}
        ORDER BY dist_{p}
        LIMIT 1
    ) AS right_{p}
    """)

print(";")

print("ROLLBACK;")
