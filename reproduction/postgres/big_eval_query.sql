
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



    -- supermarket

    CREATE TEMPORARY TABLE supermarket (centroid geometry);
    INSERT INTO supermarket SELECT centroid FROM osm_centroids
    WHERE amenity IN ('supermarket', 'grocery_store', 'convenience') OR building IN ('supermarket', 'grocery_store', 'convenience') OR shop IN ('supermarket', 'grocery_store', 'convenience') OR landuse IN ('supermarket', 'grocery_store', 'convenience');
    CREATE INDEX idx_supermarket ON supermarket USING GIST (centroid);

    


    -- bakery

    CREATE TEMPORARY TABLE bakery (centroid geometry);
    INSERT INTO bakery SELECT centroid FROM osm_centroids
    WHERE amenity = 'bakery' OR building = 'bakery' OR shop = 'bakery' OR landuse = 'bakery';
    CREATE INDEX idx_bakery ON bakery USING GIST (centroid);

    


    -- butcher

    CREATE TEMPORARY TABLE butcher (centroid geometry);
    INSERT INTO butcher SELECT centroid FROM osm_centroids
    WHERE amenity = 'butcher' OR building = 'butcher' OR shop = 'butcher' OR landuse = 'butcher';
    CREATE INDEX idx_butcher ON butcher USING GIST (centroid);

    


    -- fuel

    CREATE TEMPORARY TABLE fuel (centroid geometry);
    INSERT INTO fuel SELECT centroid FROM osm_centroids
    WHERE amenity = 'fuel' OR building = 'fuel' OR shop = 'fuel' OR landuse = 'fuel';
    CREATE INDEX idx_fuel ON fuel USING GIST (centroid);

    


    -- gastronomy

    CREATE TEMPORARY TABLE gastronomy (centroid geometry);
    INSERT INTO gastronomy SELECT centroid FROM osm_centroids
    WHERE amenity IN ('bar', 'biergarten', 'cafe', 'fast_food', 'pub', 'restaurant') OR building IN ('bar', 'biergarten', 'cafe', 'fast_food', 'pub', 'restaurant') OR shop IN ('bar', 'biergarten', 'cafe', 'fast_food', 'pub', 'restaurant') OR landuse IN ('bar', 'biergarten', 'cafe', 'fast_food', 'pub', 'restaurant');
    CREATE INDEX idx_gastronomy ON gastronomy USING GIST (centroid);

    


    -- hairdresser

    CREATE TEMPORARY TABLE hairdresser (centroid geometry);
    INSERT INTO hairdresser SELECT centroid FROM osm_centroids
    WHERE amenity = 'hairdresser' OR building = 'hairdresser' OR shop = 'hairdresser' OR landuse = 'hairdresser';
    CREATE INDEX idx_hairdresser ON hairdresser USING GIST (centroid);

    


    -- hospital

    CREATE TEMPORARY TABLE hospital (centroid geometry);
    INSERT INTO hospital SELECT centroid FROM osm_centroids
    WHERE amenity IN ('hospital', 'clinic') OR building IN ('hospital', 'clinic') OR shop IN ('hospital', 'clinic') OR landuse IN ('hospital', 'clinic');
    CREATE INDEX idx_hospital ON hospital USING GIST (centroid);

    


    -- kindergarten

    CREATE TEMPORARY TABLE kindergarten (centroid geometry);
    INSERT INTO kindergarten SELECT centroid FROM osm_centroids
    WHERE amenity = 'kindergarten' OR building = 'kindergarten' OR shop = 'kindergarten' OR landuse = 'kindergarten';
    CREATE INDEX idx_kindergarten ON kindergarten USING GIST (centroid);

    


    -- motorway

    CREATE TEMPORARY TABLE motorway (centroid geometry);
    INSERT INTO motorway SELECT centroid FROM osm_centroids
    WHERE highway = 'motorway_link';
    CREATE INDEX idx_motorway ON motorway USING GIST (centroid);

    


    -- pharmacy

    CREATE TEMPORARY TABLE pharmacy (centroid geometry);
    INSERT INTO pharmacy SELECT centroid FROM osm_centroids
    WHERE amenity IN ('pharmacy', 'chemist') OR building IN ('pharmacy', 'chemist') OR shop IN ('pharmacy', 'chemist') OR landuse IN ('pharmacy', 'chemist');
    CREATE INDEX idx_pharmacy ON pharmacy USING GIST (centroid);

    


    -- school

    CREATE TEMPORARY TABLE school (centroid geometry);
    INSERT INTO school SELECT centroid FROM osm_centroids
    WHERE amenity = 'school' OR building = 'school' OR shop = 'school' OR landuse = 'school';
    CREATE INDEX idx_school ON school USING GIST (centroid);

    


    -- university

    CREATE TEMPORARY TABLE university (centroid geometry);
    INSERT INTO university SELECT centroid FROM osm_centroids
    WHERE amenity IN ('university', 'college') OR building IN ('university', 'college') OR shop IN ('university', 'college') OR landuse IN ('university', 'college');
    CREATE INDEX idx_university ON university USING GIST (centroid);

    
-----------
SELECT 
AVG(left_.centroid::geography <-> right_transport.centroid::geography) / 1000 AS avg_dist_transport,
STDDEV_SAMP(left_.centroid::geography <-> right_transport.centroid::geography) / 1000 AS stdev_dist_transport,
AVG(left_.centroid::geography <-> right_supermarket.centroid::geography) / 1000 AS avg_dist_supermarket,
STDDEV_SAMP(left_.centroid::geography <-> right_supermarket.centroid::geography) / 1000 AS stdev_dist_supermarket,
AVG(left_.centroid::geography <-> right_bakery.centroid::geography) / 1000 AS avg_dist_bakery,
STDDEV_SAMP(left_.centroid::geography <-> right_bakery.centroid::geography) / 1000 AS stdev_dist_bakery,
AVG(left_.centroid::geography <-> right_butcher.centroid::geography) / 1000 AS avg_dist_butcher,
STDDEV_SAMP(left_.centroid::geography <-> right_butcher.centroid::geography) / 1000 AS stdev_dist_butcher,
AVG(left_.centroid::geography <-> right_fuel.centroid::geography) / 1000 AS avg_dist_fuel,
STDDEV_SAMP(left_.centroid::geography <-> right_fuel.centroid::geography) / 1000 AS stdev_dist_fuel,
AVG(left_.centroid::geography <-> right_gastronomy.centroid::geography) / 1000 AS avg_dist_gastronomy,
STDDEV_SAMP(left_.centroid::geography <-> right_gastronomy.centroid::geography) / 1000 AS stdev_dist_gastronomy,
AVG(left_.centroid::geography <-> right_hairdresser.centroid::geography) / 1000 AS avg_dist_hairdresser,
STDDEV_SAMP(left_.centroid::geography <-> right_hairdresser.centroid::geography) / 1000 AS stdev_dist_hairdresser,
AVG(left_.centroid::geography <-> right_hospital.centroid::geography) / 1000 AS avg_dist_hospital,
STDDEV_SAMP(left_.centroid::geography <-> right_hospital.centroid::geography) / 1000 AS stdev_dist_hospital,
AVG(left_.centroid::geography <-> right_kindergarten.centroid::geography) / 1000 AS avg_dist_kindergarten,
STDDEV_SAMP(left_.centroid::geography <-> right_kindergarten.centroid::geography) / 1000 AS stdev_dist_kindergarten,
AVG(left_.centroid::geography <-> right_motorway.centroid::geography) / 1000 AS avg_dist_motorway,
STDDEV_SAMP(left_.centroid::geography <-> right_motorway.centroid::geography) / 1000 AS stdev_dist_motorway,
AVG(left_.centroid::geography <-> right_pharmacy.centroid::geography) / 1000 AS avg_dist_pharmacy,
STDDEV_SAMP(left_.centroid::geography <-> right_pharmacy.centroid::geography) / 1000 AS stdev_dist_pharmacy,
AVG(left_.centroid::geography <-> right_school.centroid::geography) / 1000 AS avg_dist_school,
STDDEV_SAMP(left_.centroid::geography <-> right_school.centroid::geography) / 1000 AS stdev_dist_school,
AVG(left_.centroid::geography <-> right_university.centroid::geography) / 1000 AS avg_dist_university,
STDDEV_SAMP(left_.centroid::geography <-> right_university.centroid::geography) / 1000 AS stdev_dist_university

FROM (
  SELECT centroid
  FROM building
) AS left_


    CROSS JOIN LATERAL (
        SELECT  right_transport.centroid,
                right_transport.centroid <-> left_.centroid AS dist_transport
        FROM transport AS right_transport
        ORDER BY dist_transport
        LIMIT 1
    ) AS right_transport
    

    CROSS JOIN LATERAL (
        SELECT  right_supermarket.centroid,
                right_supermarket.centroid <-> left_.centroid AS dist_supermarket
        FROM supermarket AS right_supermarket
        ORDER BY dist_supermarket
        LIMIT 1
    ) AS right_supermarket
    

    CROSS JOIN LATERAL (
        SELECT  right_bakery.centroid,
                right_bakery.centroid <-> left_.centroid AS dist_bakery
        FROM bakery AS right_bakery
        ORDER BY dist_bakery
        LIMIT 1
    ) AS right_bakery
    

    CROSS JOIN LATERAL (
        SELECT  right_butcher.centroid,
                right_butcher.centroid <-> left_.centroid AS dist_butcher
        FROM butcher AS right_butcher
        ORDER BY dist_butcher
        LIMIT 1
    ) AS right_butcher
    

    CROSS JOIN LATERAL (
        SELECT  right_fuel.centroid,
                right_fuel.centroid <-> left_.centroid AS dist_fuel
        FROM fuel AS right_fuel
        ORDER BY dist_fuel
        LIMIT 1
    ) AS right_fuel
    

    CROSS JOIN LATERAL (
        SELECT  right_gastronomy.centroid,
                right_gastronomy.centroid <-> left_.centroid AS dist_gastronomy
        FROM gastronomy AS right_gastronomy
        ORDER BY dist_gastronomy
        LIMIT 1
    ) AS right_gastronomy
    

    CROSS JOIN LATERAL (
        SELECT  right_hairdresser.centroid,
                right_hairdresser.centroid <-> left_.centroid AS dist_hairdresser
        FROM hairdresser AS right_hairdresser
        ORDER BY dist_hairdresser
        LIMIT 1
    ) AS right_hairdresser
    

    CROSS JOIN LATERAL (
        SELECT  right_hospital.centroid,
                right_hospital.centroid <-> left_.centroid AS dist_hospital
        FROM hospital AS right_hospital
        ORDER BY dist_hospital
        LIMIT 1
    ) AS right_hospital
    

    CROSS JOIN LATERAL (
        SELECT  right_kindergarten.centroid,
                right_kindergarten.centroid <-> left_.centroid AS dist_kindergarten
        FROM kindergarten AS right_kindergarten
        ORDER BY dist_kindergarten
        LIMIT 1
    ) AS right_kindergarten
    

    CROSS JOIN LATERAL (
        SELECT  right_motorway.centroid,
                right_motorway.centroid <-> left_.centroid AS dist_motorway
        FROM motorway AS right_motorway
        ORDER BY dist_motorway
        LIMIT 1
    ) AS right_motorway
    

    CROSS JOIN LATERAL (
        SELECT  right_pharmacy.centroid,
                right_pharmacy.centroid <-> left_.centroid AS dist_pharmacy
        FROM pharmacy AS right_pharmacy
        ORDER BY dist_pharmacy
        LIMIT 1
    ) AS right_pharmacy
    

    CROSS JOIN LATERAL (
        SELECT  right_school.centroid,
                right_school.centroid <-> left_.centroid AS dist_school
        FROM school AS right_school
        ORDER BY dist_school
        LIMIT 1
    ) AS right_school
    

    CROSS JOIN LATERAL (
        SELECT  right_university.centroid,
                right_university.centroid <-> left_.centroid AS dist_university
        FROM university AS right_university
        ORDER BY dist_university
        LIMIT 1
    ) AS right_university
    
;
ROLLBACK;
