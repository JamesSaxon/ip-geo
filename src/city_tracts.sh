#!/bin/bash 

ogr2ogr -f GeoJSON city_tracts.geojson \
  "PG:host=MY_HOST dbname=MY_DB user=MY_USER password=MY_PASSWD" \
  -sql "

WITH u AS (
  SELECT ST_UnaryUnion(ST_Buffer(geom, 1e5)) g
  FROM census_place_2018
  WHERE
    top20 AND
    name IN ('Chicago', 'Philadelphia', 'New York'))
SELECT
  tr.geoid, ST_Transform(tr.geom, 4326)
FROM census_tracts_2018 tr
JOIN u ON ST_Intersects(tr.geom, u.g);

"

