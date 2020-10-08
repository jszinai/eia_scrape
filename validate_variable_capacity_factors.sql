-- this query is run to double check that the hours of 0 variable capacity factors
-- do not occur in the middle of the day, and only occur in the middle of the night or
-- during 'shoulder' hours for solar generators (difficult to assess with wind)
-- since the data is in utc, the output of the query is copied into a CSV and converted
-- to Pacific time for easier inspection.

-- run on db command line: cat validate_variable_capacity_factors.sql | psql -d switch_wecc > analysis

SELECT COUNT(*),
extract(year from a.timestamp_utc) as utc_year,
extract(hour from a.timestamp_utc) as utc_hour, 
MAX(a.timestamp_utc) as max_utc_hour, 
MIN(a.timestamp_utc) as min_utc_hour
FROM variable_capacity_factors a
JOIN generation_plant_scenario_member b ON a.generation_plant_id = b.generation_plant_id
JOIN generation_plant c on c.generation_plant_id = a.generation_plant_id
WHERE b.generation_plant_scenario_id = 19
AND c.energy_source = 'Solar'
AND c.gen_tech = 'PV'
AND a.capacity_factor = 0
GROUP BY utc_year, utc_hour
ORDER BY utc_year, utc_hour;

