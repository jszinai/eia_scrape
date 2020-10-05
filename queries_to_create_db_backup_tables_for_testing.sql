-- Copyright 2020. All rights reserved. Written in 2020 by Julia Szinai
-- Licensed under the Apache License, Version 2.0 which is in LICENSE.txt

-- This script creates backup tables (copies of the original db tables) of all the tables that are modified and/or used in the EIA_scrape scripts. 

-- This script is not meant to be executed as is, but rather is a way to document the queries that were run as part of the process of testing the updated 
-- EIA_scrape scripts. Therefore there is a syntax error on purpose in the first query which will cause the script to break if it is executed as is. 
-- Instead, SSH into the db, copy and run these individual queries in different screens to create backup tables before running and testing the EIA_scrape scripts.
-- It is useful to run the queries in screens because some of the queries take a long time to run (especially query 13 and 14)

-- Broad steps on how to implement:
-- 1. Create backup tables with a prefix in the name (in this case jsz_backup was the prefix used) that are copies of the original db tables that are modified in the 
-- database_interface.py (from https://github.com/RAEL-Berkeley/eia_scrape/)

-- This loop makes it easier to create the queries for all the backup table creation
-- CREATE_BACKUP_TABLES = ';\n'.join(
--         "DROP TABLE {PREFIX}{ORIG_TABLE}; CREATE TABLE {PREFIX}{ORIG_TABLE} (LIKE {ORIG_TABLE} INCLUDING INDEXES INCLUDING DEFAULTS); INSERT INTO {PREFIX}{ORIG_TABLE} SELECT * FROM {ORIG_TABLE}".format(PREFIX=PREFIX, ORIG_TABLE=ORIG_TABLE) for ORIG_TABLE in (
--             'generation_plant',
--             'generation_plant_cost',
--             'generation_plant_existing_and_planned',
--             'generation_plant_scenario_member',
--             'generation_plant_technologies',
--             'hydro_historical_monthly_capacity_factors',
--             'load_zone',
--             'raw_timepoint',
--             'temp_ampl__proposed_projects_v3',
--             'temp_load_scenario_historic_timepoints',
--             'temp_variable_capacity_factors_historical',
--             'us_counties',
--             'us_states',
--             'variable_capacity_factors',
--         )
--     ) + ';'

-- 2. Run the scrape.py script.
-- 3. Run the database_interface.py script on the backup tables and create a new scenario 17 in the backup tables exactly as before but with the updated set of EIA input files out to 2018
-- 3. Compare the new scenario in the backup tables to the original scenario 2 
-- 4. If OK, recreate backup tables that are copies of the original db tables
-- 5. Re-run the database_interface.py script on the original tables to create the same new scenario 18

-- To create backup tables:
-- 1. Open terminal and run these commands:
-- ssh jszinai@switch-db2.erg.berkeley.edu
-- 2. For each query create a screen, so it can run and not have the connection reset:
-- screen -S query_#
-- 3. Connect to the postgres db:
-- psql -d switch_wecc
-- 4. Copy query
-- 5. To detach a “screen” session: 
-- Ctrl-A then D
-- To reattach a “screen” session:
-- screen -r
-- Ctrl-D and then exit to end a screen after a query is done
-- 6. To check status of queries:
-- Create a check_queries screen and run this:
-- Select * from pg_stat_activity

-- This is a dummy query meant to create a syntax error to prevent this script from being run as is:
SELECT 1
FROM ; 

-- Queries to create backup tables:
-- Query 1:
DROP TABLE switch.jsz_backup_generation_plant; CREATE TABLE switch.jsz_backup_generation_plant (LIKE switch.generation_plant INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_generation_plant SELECT * FROM switch.generation_plant;                      	

-- Query 2:
DROP TABLE switch.jsz_backup_generation_plant_cost; CREATE TABLE switch.jsz_backup_generation_plant_cost (LIKE switch.generation_plant_cost INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_generation_plant_cost SELECT * FROM switch.generation_plant_cost;

-- Query 3:
DROP TABLE switch.jsz_backup_generation_plant_existing_and_planned; CREATE TABLE switch.jsz_backup_generation_plant_existing_and_planned (LIKE switch.generation_plant_existing_and_planned INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_generation_plant_existing_and_planned SELECT * FROM switch.generation_plant_existing_and_planned;                

-- Query 4:                     	
DROP TABLE switch.jsz_backup_generation_plant_scenario_member; CREATE TABLE switch.jsz_backup_generation_plant_scenario_member (LIKE switch.generation_plant_scenario_member INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_generation_plant_scenario_member SELECT * FROM switch.generation_plant_scenario_member;

-- Query 5:
DROP TABLE switch.jsz_backup_generation_plant_technologies; CREATE TABLE switch.jsz_backup_generation_plant_technologies (LIKE switch.generation_plant_technologies INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_generation_plant_technologies SELECT * FROM switch.generation_plant_technologies;

-- Query 6:
DROP TABLE switch.jsz_backup_hydro_historical_monthly_capacity_factors; CREATE TABLE switch.jsz_backup_hydro_historical_monthly_capacity_factors (LIKE switch.hydro_historical_monthly_capacity_factors INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_hydro_historical_monthly_capacity_factors SELECT * FROM switch.hydro_historical_monthly_capacity_factors;

-- Query 7:
DROP TABLE switch.jsz_backup_load_zone; CREATE TABLE switch.jsz_backup_load_zone (LIKE switch.load_zone INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_load_zone SELECT * FROM switch.load_zone;

-- Query 8:
DROP TABLE switch.jsz_backup_raw_timepoint; CREATE TABLE switch.jsz_backup_raw_timepoint (LIKE switch.raw_timepoint INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_raw_timepoint SELECT * FROM switch.raw_timepoint;

-- Query 9:
DROP TABLE switch.jsz_backup_temp_ampl__proposed_projects_v3; CREATE TABLE switch.jsz_backup_temp_ampl__proposed_projects_v3 (LIKE switch.temp_ampl__proposed_projects_v3 INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_temp_ampl__proposed_projects_v3 SELECT * FROM switch.temp_ampl__proposed_projects_v3;

-- Query 10:
DROP TABLE switch.jsz_backup_temp_load_scenario_historic_timepoints; CREATE TABLE switch.jsz_backup_temp_load_scenario_historic_timepoints (LIKE switch.temp_load_scenario_historic_timepoints INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_temp_load_scenario_historic_timepoints SELECT * FROM switch.temp_load_scenario_historic_timepoints;

-- Query 11:
DROP TABLE switch.jsz_backup_us_counties; CREATE TABLE switch.jsz_backup_us_counties (LIKE switch.us_counties INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_us_counties SELECT * FROM switch.us_counties;
-- Query 12:
DROP TABLE switch.jsz_backup_us_states; CREATE TABLE switch.jsz_backup_us_states (LIKE switch.us_states INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_us_states SELECT * FROM switch.us_states;

-- Query 13: (this takes a long time to run)
DROP TABLE switch.jsz_backup_variable_capacity_factors; CREATE TABLE switch.jsz_backup_variable_capacity_factors (LIKE switch.variable_capacity_factors INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_variable_capacity_factors SELECT * FROM switch.variable_capacity_factors;

-- Query 14:
DROP TABLE switch.jsz_backup_temp_variable_capacity_factors_historical; CREATE TABLE switch.jsz_backup_temp_variable_capacity_factors_historical (LIKE switch.temp_variable_capacity_factors_historical INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_temp_variable_capacity_factors_historical SELECT * FROM switch.temp_variable_capacity_factors_historical;   	

-- Scenario mapping tables:
-- Query 15:
DROP TABLE switch.jsz_backup_generation_plant_cost_scenario; CREATE TABLE switch.jsz_backup_generation_plant_cost_scenario (LIKE switch.generation_plant_cost_scenario INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_generation_plant_cost_scenario SELECT * FROM switch.generation_plant_cost_scenario;

-- Query 16:
DROP TABLE switch.jsz_backup_generation_plant_existing_and_planned_scenario; CREATE TABLE switch.jsz_backup_generation_plant_existing_and_planned_scenario (LIKE switch.generation_plant_existing_and_planned_scenario INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_generation_plant_existing_and_planned_scenario SELECT * FROM switch.generation_plant_existing_and_planned_scenario;

-- Query 17:
DROP TABLE switch.jsz_backup_hydro_simple_scenario; CREATE TABLE switch.jsz_backup_hydro_simple_scenario (LIKE switch.hydro_simple_scenario INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_hydro_simple_scenario SELECT * FROM switch.hydro_simple_scenario;

-- Query 18:
DROP TABLE switch.jsz_backup_generation_plant_scenario; CREATE TABLE switch.jsz_backup_generation_plant_scenario (LIKE switch.generation_plant_scenario INCLUDING INDEXES INCLUDING DEFAULTS INCLUDING CONSTRAINTS); INSERT INTO switch.jsz_backup_generation_plant_scenario SELECT * FROM switch.generation_plant_scenario;
