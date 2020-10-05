# Copyright 2017. All rights reserved. See AUTHORS.txt
# Licensed under the Apache License, Version 2.0 which is in LICENSE.txt
# Modified 2020 by Julia Szinai
"""
Defines several functions to finish processing EIA data and upload to the
Switch-WECC database. Some functions may be used for other purposes.

"""

import os, sys
import pandas as pd
import numpy as np
import getpass

import matplotlib.pyplot as plt
plt.switch_backend('agg')

from IPython import embed


from utils import connect_to_db_and_run_query, append_historic_output_to_csv, connect_to_db_and_push_df

coal_codes = ['ANT','BIT','LIG','SGC','SUB','WC','RC']
# explanation of coal status codes:
# ANT = Anthracite Coal, BIT = Bituminous Coal, LIG = Lignite Coal, SGC =
# Coal-Derived Synthesis Gas, SUB = Subbituminous Coal, WC = Waste/Other Coal, RC =Recirculating cooling
outputs_directory = 'processed_data'
# Disable false positive warnings from pandas
pd.options.mode.chained_assignment = None

#generation_plant_scenario_id and generation_plant_existing_and_planned_scenario_id including individual generation plants is 19 and aggregated version of the same scenario is 20
new_disaggregated_gen_scenario_id = 19.0
new_aggregated_gen_scenario_id = 20.0

#new hydro_simple_id (old hydro simple scenario id = 2 or 3)
new_disaggregated_hydro_simple_scenario_id = 19.0
new_aggregated_hydro_simple_scenario_id = 20.0

#new generation_plant_cost_id (old generation_plant_cost_id = 2 or 3)
new_disggregated_generation_plant_cost_id = 19.0
new_aggregated_generation_plant_cost_id = 20.0

#if testing code, run the script on backup tables first, which are defined with a PREFIX in the table name, otherwise is run on the main tables
TESTING_ON_BACKUP_TABLES = False

if TESTING_ON_BACKUP_TABLES:
    PREFIX = 'jsz_backup_'
else:
    PREFIX = ''

def pull_generation_projects_data(gen_scenario_id):
    """
    Returns generation plant data for a specific existing and planned scenario id.
    For now, only used to compare the old AMPL dataset with new heat rates.

    """

    print "Reading in existing and planned generation project data from database..."
    query = "SELECT * \
            FROM {PREFIX}generation_plant JOIN {PREFIX}generation_plant_existing_and_planned \
            USING (generation_plant_id) \
            WHERE generation_plant_existing_and_planned_scenario_id = {gen_scenario_id}".format(PREFIX=PREFIX, gen_scenario_id=gen_scenario_id)
    db_gens = connect_to_db_and_run_query(query=query, database='switch_wecc')
    print "======="
    print "Read in {} projects from the database for id {}, with {:.0f} GW of capacity".format(
        len(db_gens), gen_scenario_id, db_gens['capacity'].sum()/1000.0)
    thermal_db_gens = db_gens[db_gens['full_load_heat_rate'] > 0]
    print "Weighted average of heat rate: {:.3f} MMBTU/MWh".format(
        thermal_db_gens['capacity'].dot(thermal_db_gens['full_load_heat_rate'])/thermal_db_gens['capacity'].sum())
    print "======="

    return db_gens

def compare_generation_projects_scenario_data_by_energy_source(old_gen_scenario_id, new_gen_scenario_id):
    """
    Returns generation plant data for a prior existing and planned scenario id and compares with generation plant data for new added scenario,
    grouping by gen_tech and energy source

    Use this function to compare generation_plant_existing_and_planned_scenario_id=2 (2015 EIA data)
    with new generation_plant_existing_and_planned_scenario_id from the 2018 EIA data update

    """
    energy_source_list = ["Bio_Gas", "Wind","Waste_Heat","Coal","Solar","Bio_Solid","DistillateFuelOil","Uranium" ,"Gas" ,"Water","ResidualFuelOil","Geothermal","Bio_Liquid"]
    wecc_states = ['AZ','CA','CO','ID','MT','NV','NM','OR','TX','UT','WA','WY']

    print "Query of existing and planned generation project capacity by energy source from database from generation_plant_existing_and_planned_scenario_id {old_gen_scenario_id}...".format(old_gen_scenario_id=old_gen_scenario_id)

    query = "SELECT SUM(capacity) as total_capacity_limit_mw, energy_source, gen_tech \
            FROM {PREFIX}generation_plant \
            JOIN {PREFIX}generation_plant_existing_and_planned \
            USING (generation_plant_id) \
            WHERE generation_plant_existing_and_planned_scenario_id = {gen_scenario_id} \
            GROUP BY energy_source, gen_tech \
            ORDER BY energy_source, gen_tech".format(PREFIX=PREFIX, gen_scenario_id = old_gen_scenario_id)
    db_compare_gens_old_scenario = connect_to_db_and_run_query(query=query, database='switch_wecc')

    print "Output into CSV the query result of total nameplate capacity by state and energy source for generation_plant_existing_and_planned_scenario_id {old_gen_scenario_id}...".format(old_gen_scenario_id=old_gen_scenario_id)

    fpath = os.path.join('Nameplate capacity by energy source for gen plant scenario {old_gen_scenario_id}.tab').format(old_gen_scenario_id=old_gen_scenario_id)
    with open(fpath, 'w') as outfile:
        db_compare_gens_old_scenario.to_csv(outfile, sep='\t', header=True, index=False)

    print "Query of existing and planned generation project capacity by energy source from database from generation_plant_existing_and_planned_scenario_id {new_gen_scenario_id}...".format(new_gen_scenario_id=old_gen_scenario_id)

    query = "SELECT SUM(capacity) as total_capacity_limit_mw, energy_source, gen_tech \
            FROM {PREFIX}generation_plant \
            JOIN {PREFIX}generation_plant_existing_and_planned \
            USING (generation_plant_id) \
            WHERE generation_plant_existing_and_planned_scenario_id = {gen_scenario_id} \
            GROUP BY energy_source, gen_tech \
            ORDER BY energy_source, gen_tech".format(PREFIX=PREFIX, gen_scenario_id = new_gen_scenario_id)
    db_compare_gens_new_scenario = connect_to_db_and_run_query(query=query, database='switch_wecc')

    print "Output into CSV the query result of total nameplate capacity by state and energy source for generation_plant_existing_and_planned_scenario_id {new_gen_scenario_id}...".format(new_gen_scenario_id=new_gen_scenario_id)

    fpath = os.path.join('Nameplate capacity by energy source for gen plant scenario {new_gen_scenario_id}.tab').format(new_gen_scenario_id=new_gen_scenario_id)
    with open(fpath, 'w') as outfile:
        db_compare_gens_new_scenario.to_csv(outfile, sep='\t', header=True, index=False)

    compare_old_new_scenarios = pd.merge(db_compare_gens_new_scenario, db_compare_gens_old_scenario, how='left', on=['energy_source', 'gen_tech'], suffixes=('_new','_old'))

    compare_old_new_scenarios['scenario_diff_mw'] = compare_old_new_scenarios['total_capacity_limit_mw_new'] - compare_old_new_scenarios['total_capacity_limit_mw_old']

    fpath = os.path.join('Compare capacity by energy source for new and old gen plant scenarios.tab')
    with open(fpath, 'w') as outfile:
        compare_old_new_scenarios.to_csv(outfile, sep='\t', header=True, index=False)


    return db_compare_gens_old_scenario, db_compare_gens_new_scenario

def filter_plants_by_region_id(region_id, year, host='switch-db2.erg.berkeley.edu', area=0.5):
    """
    Filters generation plant data by NERC Region, according to the provided id.
    Generation plants w/o Region get assigned to the NERC Region with which more
    than a certain percentage of its County area intersects (by default, 50%).
    A list is saved with Counties and States belonging to the specified Region.
    Both County and State are necessary to correctly assign plants (some County
    names exist in multiple States).

    Returns a DataFrame with the filtered data.

    """

    state_dict = {
        'Alabama':'AL',
        'Alaska':'AK',
        'Arizona':'AZ',
        'Arkansas':'AR',
        'California':'CA',
        'Colorado':'CO',
        'Connecticut':'CT',
        'Delaware':'DE',
        'Florida':'FL',
        'Georgia':'GA',
        'Hawaii':'HI',
        'Idaho':'ID',
        'Illinois':'IL',
        'Indiana':'IN',
        'Iowa':'IA',
        'Kansas':'KS',
        'Kentucky':'KY',
        'Louisiana':'LA',
        'Maine':'ME',
        'Maryland':'MD',
        'Massachusetts':'MA',
        'Michigan':'MI',
        'Minnesota':'MN',
        'Mississippi':'MS',
        'Missouri':'MO',
        'Montana':'MT',
        'Nebraska':'NE',
        'Nevada':'NV',
        'New Hampshire':'NH',
        'New Jersey':'NJ',
        'New Mexico':'NM',
        'New York':'NY',
        'North Carolina':'NC',
        'North Dakota':'ND',
        'Ohio':'OH',
        'Oklahoma':'OK',
        'Oregon':'OR',
        'Pennsylvania':'PA',
        'Rhode Island':'RI',
        'South Carolina':'SC',
        'South Dakota':'SD',
        'Tennessee':'TN',
        'Texas':'TX',
        'Utah':'UT',
        'Vermont':'VT',
        'Virginia':'VA',
        'Washington':'WA',
        'West Virginia':'WV',
        'Wisconsin':'WI',
        'Wyoming':'WY'
    }

    #getting abbreviated name (regionabr) of NERC region from db (from switch_gis.public schema)
    print "Getting NERC region name from database..."
    query = "SELECT regionabr FROM ventyx_nerc_reg_region WHERE gid={}".format(
        region_id)
    region_name = connect_to_db_and_run_query(query=query,
        database='switch_gis', host=host)['regionabr'][0]

    #read in existing file with list of counties in each state in WECC or if file doesn't exist,
    # assign county to state and WECC region if input % of area falls into region
    counties_path = os.path.join('other_data', '{}_counties.tab'.format(region_name))
    if not os.path.exists(counties_path):
        # assign county if (area)% or more of its area falls in the region
        query = "SELECT name, state\
                 FROM ventyx_nerc_reg_region regions CROSS JOIN us_counties cts\
                 JOIN (SELECT DISTINCT state, state_fips FROM us_states) sts \
                 ON (sts.state_fips=cts.statefp) \
                 WHERE regions.gid={region_id} AND\
                 ST_Area(ST_Intersection(cts.the_geom, regions.the_geom))/\
                 ST_Area(cts.the_geom)>={area}".format(PREFIX=PREFIX, region_id=region_id, area=area)
        print "\nGetting counties and states for the region from database..."
        region_counties = pd.DataFrame(connect_to_db_and_run_query(query=query,
            database='switch_gis', host=host)).rename(columns={'name':'County','state':'State'})
        region_counties.replace(state_dict, inplace=True)
        region_counties.to_csv(counties_path, sep='\t', index=False)
    else:
        print "Reading counties from .tab file..."
        region_counties = pd.read_csv(counties_path, sep='\t', index_col=None)

    #reading in the processed generator project data from scrape.py from EIA 860 forms for each year
    generators = pd.read_csv(
        os.path.join('processed_data','generation_projects_{}.tab'.format(year)), sep='\t')
    generators.loc[:,'County'] = generators['County'].map(lambda c: str(c).title())

    print "\nRead in data for {} generators, of which:".format(len(generators))
    print "--{} are existing".format(len(generators[generators['Operational Status']=='Operable']))
    print "--{} are proposed".format(len(generators[generators['Operational Status']=='Proposed']))

    #if generators don't have a NERC region already from the EIA data, assign region based on join on county and state
    generators_with_assigned_region = generators.loc[generators['Nerc Region'] == region_name]
    generators = generators[generators['Nerc Region'].isnull()]
    generators_without_assigned_region = pd.merge(generators, region_counties, how='inner', on=['County','State'])
    generators = pd.concat([
        generators_with_assigned_region,
        generators_without_assigned_region],
        axis=0)
    generators.replace(
            to_replace={'Energy Source':coal_codes, 'Energy Source 2':coal_codes,
            'Energy Source 3':coal_codes}, value='COAL', inplace=True)
    generators_columns = list(generators.columns)

    existing_gens = generators[generators['Operational Status']=='Operable']
    proposed_gens = generators[generators['Operational Status']=='Proposed']

    print "======="
    print "Filtered to {} projects in the {} region, of which:".format(
        len(generators), region_name)
    print "--{} are existing with {:.0f} GW of capacity".format(
        len(existing_gens), existing_gens['Nameplate Capacity (MW)'].sum()/1000.0)
    print "--{} are proposed with {:.0f} GW of capacity".format(
        len(proposed_gens), proposed_gens['Nameplate Capacity (MW)'].sum()/1000.0)
    print "======="

    return generators


def compare_eia_heat_rates_to_ampl_projs(year):
    """
    Compares calculated 'Best Heat Rates' for EIA plants with full load heat
    rates of previously stored Switch AMPL data (generation scenario id 1) in
    the database.

    Returns the comparison DataFrame and prints it to a tab file.
    """

    db_gen_projects = pull_generation_projects_data(gen_scenario_id=1).rename(
        columns={'name':'Plant Name', 'gen_tech':'Prime Mover'})
    db_gen_projects.loc[:,'Prime Mover'].replace(
        {
        'Coal_Steam_Turbine':'ST',
        'Gas_Steam_Turbine':'ST',
        'Gas_Combustion_Turbine':'GT',
        'Gas_Combustion_Turbine_Cogen':'GT',
        'CCGT':'CC',
        'DistillateFuelOil_Combustion_Turbine':'GT',
        'DistillateFuelOil_Internal_Combustion_Engine':'IC',
        'Geothermal':'ST',
        'Gas_Internal_Combustion_Engine':'IC',
        'Bio_Gas_Internal_Combustion_Engine':'IC',
        'Bio_Gas_Steam_Turbine':'ST'
        },
        inplace=True)
    eia_gen_projects = filter_plants_by_region_id(13, year) #region 13 is WECC

    df = pd.merge(db_gen_projects, eia_gen_projects,
        on=['Plant Name','Prime Mover'], how='left').loc[:,[
        'Plant Name','gen_tech','energy_source','full_load_heat_rate',
        'Best Heat Rate','Prime Mover','Energy Source','Energy Source 2','Operating Year']]
    df = df[df['full_load_heat_rate']>0]

    print "\nPrinting intersection of DB and EIA generation projects that have a specified heat rate to heat_rate_comparison.tab"

    fpath = os.path.join('processed_data','heat_rate_comparison.tab')
    with open(fpath, 'w') as outfile:
        df.to_csv(outfile, sep='\t', header=True, index=False)

    # Added a merge with 'best heat rate column'
    eia_best_historic_heat_rate = pd.read_csv(
        os.path.join('processed_data','historic_heat_rates_WIDE.tab', sep='\t'))
    eia_best_historic_heat_rate = eia_best_historic_heat_rate[eia_best_historic_heat_rate['Year'] == year]

    df2 = pd.merge(db_gen_projects, eia_best_historic_heat_rate,
        on=['Plant Name','Prime Mover'], how='left').loc[:,[
        'Plant Name','gen_tech','energy_source','full_load_heat_rate',
        'Best Heat Rate','Prime Mover','Energy Source','Energy Source 2','Year']]
    df2 = df2[df2['full_load_heat_rate']>0]

    print "\nPrinting intersection of DB and EIA generation projects that have a specified heat rate to heat_rate_comparison.tab"

    fpath = os.path.join('processed_data','heat_rate_comparison_wide_test.tab')
    with open(fpath, 'w') as outfile:
        df2.to_csv(outfile, sep='\t', header=True, index=False)

    return df


def assign_heat_rates_to_projects(generators, year):
    """
    Creates uniform fuel list based on https://www.seia.org/sites/default/files/EIA-860.pdf

    Assigns calculated heat rates based on EIA923 data to plants parsed from
    EIA860 data. Receives a DataFrame with all generators and the year.

    Coal plants with better heat rates than 8.607 MMBTU/MWh (still need to add
    the reference to this best historic heat rate of 2015) and other thermal
    plants with heat rate better (lower) than 6.711 MMBTU/MWh are ignored and get
    assigned an average heat rate, since we assume a report error has taken place.

    Modified to also ignore heat rates that are too high (too bad to be realistic)
    because they are 1 order of magnitude too high (greater than 100 MMBTU/MWh)

    Average HR by energy source in recent years here: https://www.eia.gov/electricity/annual/html/epa_08_01.html

    The top and bottom .5% of heat rates get replaced by the heat rate at the
    top and bottom .5 percentile, respectively. This replaces unrealistic and missing values
    that must have been caused by reporting errors.

    Heat rate averages used to replace unrealistic values and to be assigned to
    projects without heat rate are calculated as the average heat rate of plants
    with the same technology, energy source and vintage. A 4-year window is used
    to identify plants with similar vintage. If fewer than 4 plants fall into this
    window, it is enlarged successively. If no other project with the same
    technology-energy source combination exists, then the technology's average
    heat rate is used. The last two assignments (per technology-energy source-window
    if other projects exist, and per technology is no other projects exist) are
    applied to both existing projects without heat rate data and to new projects.

    Heat rate distributions per technology and energy source are plotted and
    printed to a PDF file in order to visually inspect them.

    Returns the original DataFrame with a Best Heat Rate column.

    """

    fuels = {
        'LFG':'Bio_Gas', #landfill gas
        'OBG':'Bio_Gas', #other biomass gas
        'AB':'Bio_Solid', #agricultural by-product
        'BLQ':'Bio_Liquid', #black liquor
        'NG':'Gas', #natural gas
        'OG':'Gas', #other gas
        'PG':'Gas', #propane
        'DFO':'DistillateFuelOil', #distillate fuel oil
        'JF':'ResidualFuelOil', #jet fuel
        'COAL':'Coal',
        'GEO':'Geothermal', #geothermal
        'NUC':'Uranium', #nuclear
        'PC':'Coal', #Petroleum Coke
        'SUN':'Solar', #solar
        'WDL':'Bio_Liquid', #wood waste liquids
        'WDS':'Bio_Solid', #wood waste solids
        'MSW':'Bio_Solid', #municipal solid waste
        'PUR':'Purchased_Steam', #purchased steam
        'WH':'Waste_Heat', #Waste heat not directly attributed to a fuel source
        'OTH':'Other', #other
        'WAT':'Water', #water (hydro)
        'MWH':'Electricity', #Electricity used for energy storage
        'WND':'Wind' #wind
    }
    generators = generators.replace({'Energy Source':fuels})

    existing_gens = generators[generators['Operational Status']=='Operable']
    print "-------------------------------------"
    print "There are {} existing operable thermal projects that sum up to {:.1f} GW.".format(
        len(existing_gens[existing_gens['Prime Mover'].isin(['CC','GT','IC','ST'])]),
        existing_gens[existing_gens['Prime Mover'].isin(['CC','GT','IC','ST'])][
            'Nameplate Capacity (MW)'].sum()/1000)

    #reading in previously processed historic heat rate
    heat_rate_data = pd.read_csv(
        os.path.join('processed_data','historic_heat_rates_WIDE.tab'), sep='\t').rename(
        columns={'Plant Code':'EIA Plant Code'})
    heat_rate_data = heat_rate_data[heat_rate_data['Year']==year]
    heat_rate_data = heat_rate_data.replace({'Energy Source':fuels})
    thermal_gens = pd.merge(
        existing_gens, heat_rate_data[['EIA Plant Code','Prime Mover','Energy Source','Best Heat Rate']],
        how='left', suffixes=('',''),
        on=['EIA Plant Code','Prime Mover','Energy Source']).drop_duplicates()
    thermal_gens = thermal_gens[thermal_gens['Prime Mover'].isin(['CC','GT','IC','ST'])]

    # Replace null and unrealistic heat rates by average values per technology,
    # fuel, and vintage. Also, set HR of top and bottom .5% to max and min
    null_heat_rates = thermal_gens['Best Heat Rate'].isnull()
    unrealistic_heat_rates = (((thermal_gens['Energy Source'] == 'Coal') &
            (thermal_gens['Best Heat Rate'] < 8.607)) |
        ((thermal_gens['Energy Source'] != 'Coal') &
            (thermal_gens['Best Heat Rate'] < 6.711)) |
            (thermal_gens['Best Heat Rate'] > 100)) # Additional criteria for upper outliers
    print "{} generators don't have heat rate data specified ({:.1f} GW of capacity)".format(
        len(thermal_gens[null_heat_rates]), thermal_gens[null_heat_rates]['Nameplate Capacity (MW)'].sum()/1000.0)
    print "{} generators have better heat rate than the best historical records ({} GW of capacity)".format(
        len(thermal_gens[unrealistic_heat_rates]), thermal_gens[unrealistic_heat_rates]['Nameplate Capacity (MW)'].sum()/1000.0)
    thermal_gens_w_hr = thermal_gens[~null_heat_rates & ~unrealistic_heat_rates]
    thermal_gens_wo_hr = thermal_gens[null_heat_rates | unrealistic_heat_rates]

    # Print fuels and technologies with missing HR to console

    # for fuel in thermal_gens_wo_hr['Energy Source'].unique():
    #     print "{} of these use {} as their fuel".format(
    #         len(thermal_gens_wo_hr[thermal_gens_wo_hr['Energy Source']==fuel]),fuel)
    #     print "Technologies:"
    #     for prime_mover in thermal_gens_wo_hr[thermal_gens_wo_hr['Energy Source']==fuel]['Prime Mover'].unique():
    #         print "\t{} use {}".format(
    #             len(thermal_gens_wo_hr[(thermal_gens_wo_hr['Energy Source']==fuel) &
    #                 (thermal_gens_wo_hr['Prime Mover']==prime_mover)]),prime_mover)

    print "-------------------------------------"
    print "Assigning max/min heat rates per technology and fuel to top .5% / bottom .5%, respectively:"
    n_outliers = int(len(thermal_gens_w_hr)*0.005)
    thermal_gens_w_hr = thermal_gens_w_hr.sort_values('Best Heat Rate')
    min_hr = thermal_gens_w_hr.loc[thermal_gens_w_hr.index[n_outliers],'Best Heat Rate']
    max_hr = thermal_gens_w_hr.loc[thermal_gens_w_hr.index[-1-n_outliers],'Best Heat Rate']
    print "(Total capacity of these plants is {:.1f} GW)".format(
        thermal_gens_w_hr[thermal_gens_w_hr['Best Heat Rate'] < min_hr]['Nameplate Capacity (MW)'].sum()/1000.0 +
        thermal_gens_w_hr[thermal_gens_w_hr['Best Heat Rate'] > max_hr]['Nameplate Capacity (MW)'].sum()/1000.0)
    print "Minimum heat rate is {:.3f}".format(min_hr)
    print "Maximum heat rate is {:.3f}".format(max_hr)
    for i in range(n_outliers):
        thermal_gens_w_hr.loc[thermal_gens_w_hr.index[i],'Best Heat Rate'] = min_hr
        thermal_gens_w_hr.loc[thermal_gens_w_hr.index[-1-i],'Best Heat Rate'] = max_hr

    #window = 2 means the average HR is assigned +/- 2 years, or a 4 year wide window
    def calculate_avg_heat_rate(thermal_gens_df, prime_mover, energy_source, vintage, window=2):
        similar_generators = thermal_gens_df[
            (thermal_gens_df['Prime Mover']==prime_mover) &
            (thermal_gens_df['Energy Source']==energy_source) &
            (thermal_gens_df['Operating Year']>=vintage-window) &
            (thermal_gens_df['Operating Year']<=vintage+window)]
        while len(similar_generators) < 4: # If fewer than 4 plants fall into this window, it is enlarged successively.
            window += 2
            similar_generators = thermal_gens_df[
                (thermal_gens_df['Prime Mover']==prime_mover) &
                (thermal_gens_df['Energy Source']==energy_source) &
                (thermal_gens_df['Operating Year']>=vintage-window) &
                (thermal_gens_df['Operating Year']<=vintage+window)]
            # thermal generator operating years span from 1925 to 2018, so a window of 103 years is the maximum
            if window >= 103:
                break
        if len(similar_generators) > 0:
            return similar_generators['Best Heat Rate'].mean()
        else:
            # If no other similar projects exist, return average of technology
            return thermal_gens_df[thermal_gens_df['Prime Mover']==prime_mover]['Best Heat Rate'].mean()


    print "-------------------------------------"
    print "Assigning average heat rates per technology, fuel, and vintage to projects w/o heat rate..."
    for idx in thermal_gens_wo_hr.index:
        pm = thermal_gens_wo_hr.loc[idx,'Prime Mover']
        es = thermal_gens_wo_hr.loc[idx,'Energy Source']
        v = thermal_gens_wo_hr.loc[idx,'Operating Year']
        #print "{}\t{}\t{}\t{}".format(pm,es,v,calculate_avg_heat_rate(thermal_gens_w_hr, pm, es, v))
        thermal_gens_wo_hr.loc[idx,'Best Heat Rate'] = calculate_avg_heat_rate(
            thermal_gens_w_hr, pm, es, v)

    thermal_gens = pd.concat([thermal_gens_w_hr, thermal_gens_wo_hr], axis=0)
    existing_gens = pd.merge(existing_gens, thermal_gens, on=list(existing_gens.columns), how='left')


    # Plot histograms for resulting heat rates per technology and fuel
    thermal_gens["Technology"] = thermal_gens["Energy Source"].map(str) + ' ' + thermal_gens["Prime Mover"]
    # Commented out because of a pandas update that caused an error with ggplot2. The associated ggplot plotting code (for diagnostics) is also commented out in the script below
    #from ggplot import *
    #import rpy2
    #from pandas import Timestamp
    #p = ggplot(aes(x='Best Heat Rate',fill='Technology'), data=thermal_gens) + geom_histogram(binwidth=0.5) + facet_wrap("Technology")  + ylim(0,30)
    #p.save(os.path.join(outputs_directory,'heat_rate_distributions.pdf'))

    #assigning average heat rate of technology for proposed generation based on calculated average HR of available HR from EIA data (2004-2018)
    proposed_gens = generators[generators['Operational Status']=='Proposed']
    thermal_proposed_gens = proposed_gens[proposed_gens['Prime Mover'].isin(['CC','GT','IC','ST'])]
    other_proposed_gens = proposed_gens[~proposed_gens['Prime Mover'].isin(['CC','GT','IC','ST'])]
    print "There are {} proposed thermal projects that sum up to {:.2f} GW.".format(
        len(thermal_proposed_gens), thermal_proposed_gens['Nameplate Capacity (MW)'].sum()/1000)
    print "Assigning average heat rate of technology and fuel of most recent years from EIA (2004-2018)..."
    for idx in thermal_proposed_gens.index:
        pm = thermal_proposed_gens.loc[idx,'Prime Mover']
        es = thermal_proposed_gens.loc[idx,'Energy Source']
        #print "{}\t{}\t{}\t{}".format(pm,es,v,calculate_avg_heat_rate(thermal_gens_w_hr, pm, es, v))
        thermal_proposed_gens.loc[idx,'Best Heat Rate'] = calculate_avg_heat_rate(
            thermal_gens_w_hr, pm, es, year)

    other_proposed_gens['Best Heat Rate'] = float('nan')
    proposed_gens = pd.concat([thermal_proposed_gens,other_proposed_gens], axis=0)

    return pd.concat([existing_gens, proposed_gens], axis=0)


def finish_project_processing(year):
    """
    Receives a year, and processes the scraped EIA data for that year by using
    previously defined functions.

    The year that should be input is the most recent year of EIA data. At the time
    of updating this script (Aug 2020), 2018 was the the most recent available vintage
    of "final" (not preliminary) EIA data.

    First, plants are read in from the generation_projects_YEAR.tab file, which
    come from the EIA860 form, and filtered by region. For now, region 13 (WECC)
    is hardcoded.

    Second, plants are assigned heat rates from the historic_heat_rates_WIDE.tab
    file, which come from the EIA923 form. Plants with missing heat rates are
    assigned averages, and unrealistic heat rate values are replaced by reasonable
    parameters.

    Prints out 3 tab files with resulting data:
        existing_generation_projects_YEAR.tab
        new_generation_projects_YEAR.tab
        uprates_to_generation_projects_YEAR.tab

    These files are later post-processed and pushed into the Switch-WECC database
    of RAEL (UC Berkeley), though data is formatted in a general-purpose manner,
    so it could be used for any other purpose.

    """
    #assign generators to NERC regions and filter list just to WECC generators in given year
    generators = filter_plants_by_region_id(13, year)
    #assign average heat rates from similar vintage and technology to thermal
    # generators with missing or unrealistic heat rates
    generators = assign_heat_rates_to_projects(generators, year)
    existing_gens = generators[generators['Operational Status']=='Operable']
    proposed_gens = generators[generators['Operational Status']=='Proposed']

    #output to CSV the list of existing generation projects that have been processed for the given year
    fname = 'existing_generation_projects_{}.tab'.format(year)
    with open(os.path.join(outputs_directory, fname),'w') as f:
        existing_gens.to_csv(f, sep='\t', encoding='utf-8', index=False)

    uprates = pd.DataFrame()
    new_gens = pd.DataFrame()
    for idx in proposed_gens.index:
        pc = proposed_gens.loc[idx,'EIA Plant Code']
        pm = proposed_gens.loc[idx,'Prime Mover']
        es = proposed_gens.loc[idx,'Energy Source']
        existing_units_for_proposed_gen = existing_gens[
        (existing_gens['EIA Plant Code'] == pc) &
        (existing_gens['Prime Mover'] == pm) &
        (existing_gens['Energy Source'] == es)]
        if len(existing_units_for_proposed_gen) == 0:
            new_gens = pd.concat([new_gens, pd.DataFrame(proposed_gens.loc[idx,:]).T], axis=0)
        elif len(existing_units_for_proposed_gen) == 1:
            uprates = pd.concat([uprates, pd.DataFrame(proposed_gens.loc[idx,:]).T], axis=0)
        else:
            print "There is more than one option for uprating plant id {}, prime mover {} and energy source {}".format(int(pc), pm, es)

    #output to CSV the list of proposed generation projects that have been processed for the given year
    fname = 'new_generation_projects_{}.tab'.format(year)
    with open(os.path.join(outputs_directory, fname),'w') as f:
        new_gens.to_csv(f, sep='\t', encoding='utf-8', index=False)

    fname = 'uprates_to_generation_projects_{}.tab'.format(year)
    with open(os.path.join(outputs_directory, fname),'w') as f:
        uprates.to_csv(f, sep='\t', encoding='utf-8', index=False)


def upload_generation_projects(year):
    """
    Reads existing and new project data previously processed from the EIA forms
    in order to upload it to the Switch-WECC database of RAEL, at UC Berkeley.

    First, generation project data is read in from the processed tab files.

    Projects using Purchased Steam as their energy source are
    dropped from the generator set.

    Projects using Electricity as their energy source were previously also
    dropped from the generator set. But given the growing share of batteries in
    the capacity mix (presumably to meet the CA storage mandate), Batteries are removed
    from the "ignored" list and included in the list of existing and
    proposed generation.

    The list of retired plants in WECC that are still in the generator list is read in
    from the processed tab files. This list is joined with the processed plant-level list above
    and the retired nameplate capacity is subtracted. If the remaining capacity is 0, the
    plant is dropped from the list before uploading to the database.

    Projects using Other as their energy source are assigned Gas as default.

    Capacity limits are set as total existing and projected capacity for each
    project (e.g. no additional capacity additions will be allowed for
    predetermined projects in Switch).

    Plant-level heat rates are calculated by doing a capacity-weighted average
    over the individual heat rates of each unit in the plant that have the same
    technology and use the same energy source. This allows obtaining a single
    heat rate for plants with units that have different vintages.

    Baseload flags are set for all plants that use Nuclear, Coal, or Geothermal
    as their energy source.

    Variable flags are set for all plants that use Hydro, Photovoltaic, or
    Wind Turbine technologies.

    Cogen flags are set for all plants that declared being Cogen.

    Columns are renamed to match the PSQL database column definitions.

    Resulting generation plant data is uploaded to the database with generation
    plant scenario id 19 for 2018 vintage EIA data (previously was scenario 2 for
    2015 vintage EIA data). A subsequent aggregated set per technology, energy source,
    and load zone is uploaded with id 20 for 2018 vintage EIA data (previously
    was scenario 3 for 2015 vintage EIA data).

    WARNING: The upload process will clean the database from all previous projects
    with the same scenario ids (previously 2 and 3, now 19 and 20). This includes:
        Hydro capacity factors
        Plant cost
        Plant build years
        Plant scenario members
        Plant level data
        But not variable capacity factor data (that was uploaded after finishing
            this part of the code, so its still in the todo list).

    After uploading generation plant data, the geom column is populated with
    the geometric object representing the location of the project, for those
    projects with latitude and longitude defined.

    Then, plants are assigned to load zones:
        Plants with geom data are assgined to zones into which their location
        falls in.
        Plants without lat and long data are assigned to the load zone in which
        their County's centroid falls in.
        Plants with coordinates out of the WECC region (only a few) are assigned
        to the closest WECC load zone if they are within a 100 mile radius from
        its boundary. Otherwise, they are dropped from the data set (for now,
        only a couple of cases in the East Coast, which must have a reporting
        mistake).

    Outage rates, and variable O&M costs are assigned as
    technology-default values. For battery_storage gen_tech these technology-default
    values are copied into the technology default table from that of proposed
    battery storage in another scenario

    For the generators that have planned retirements, the max age is
    set to the planned retirement year - operating year. For all other generators,
    a technology-default value is assigned for the max age.

    Uploaded plants are assigned to generation plant scenario id 19 (was 2).

    The uploaded generation plant ids are recovered, so that build year data
    can be uploaded for existing and new projects.

    Fixed and investment costs are assigned a default value of 0 to all plants.

    Hydro capacity factors are uploaded for each hydro plant, according to
    nameplate capacity. Minimum flows are set to a default of 0.5 times the
    average flow. The hydro scenario id is set to 19 (was 2).

    The plant dataset is then aggregated by technology, energy source, and load
    zone, considering heat rate windows of 1 MMBTU/MWh (so that plants with
    significantly different heat rates are not lumped in together). Heat rates
    are averaged by weighting the capacity of each plant. Other properties,
    such as capacity limit, are simply summed.

    In the 2020 update, the dataset is uploaded with id 20 (was 3 in 2017),
    and build years, hydro capacity factors, and all other data is processed
    in the same way as for id 19 (was 2 in 2017).

    The scenario "mapping" tables (generation_plant_scenario,
    hydro_simple_scenario, generation_plant_cost_scenario, generation_plant_existing_and_planned_scenario)
     are updated to include the new scenario ids and scenario description

    """
    try:
        user = os.environ['SWITCH_USERNAME']
        password = os.environ['SWITCH_PASSWORD']
    except KeyError:
        user = getpass.getpass('Enter username for the database:')
        password = getpass.getpass('Enter database password for user {}:'.format(user))
    def read_output_csv(fname):
        try:
            return pd.read_csv(os.path.join(outputs_directory,fname), sep='\t', index_col=None)
        except:
            print "Failed to read file {}. It will be considered to be empty.".format(fname)
            return None

    existing_gens = read_output_csv('existing_generation_projects_{}.tab'.format(year))
    new_gens = read_output_csv('new_generation_projects_{}.tab'.format(year))
    uprates = read_output_csv('uprates_to_generation_projects_{}.tab'.format(year))
    if uprates is not None:
        print "Read data for {} existing projects, {} new projects, and {} uprates".format(
            len(existing_gens), len(new_gens), len(uprates))
        print "Existing capacity: {:.2f} GW".format(existing_gens['Nameplate Capacity (MW)'].sum()/1000.0)
        print "Proposed capacity: {:.2f} GW".format(new_gens['Nameplate Capacity (MW)'].sum()/1000.0)
        print "Capacity uprates: {:.2f} GW".format(uprates['Nameplate Capacity (MW)'].sum()/1000.0)
    else:
        print "Read data for {} existing projects and {} new projects".format(
            len(existing_gens), len(new_gens))
        print "Existing capacity: {:.2f} GW".format(existing_gens['Nameplate Capacity (MW)'].sum()/1000.0)
        print "Proposed capacity: {:.2f} GW".format(new_gens['Nameplate Capacity (MW)'].sum()/1000.0)

    generators = pd.concat([existing_gens, new_gens], axis=0)

    # Batteries were previously included on the list of ignored energy sources. But there are existing
    # batteries on the system, and as of the 2018 vintage EIA data about 800MW of batteries that are proposed.
    # So I have removed batteries from the list of ignored projects because it is a significant capacity amount
    # (to meet CA storage mandate)

    ignore_energy_sources = ['Purchased_Steam']
    #ignore_energy_sources = ['Purchased_Steam','Electricity']

    print ("Dropping projects that use Purchased Steam, since these"
    " are not modeled in Switch, totalizing {:.2f} GW of capacity").format(
        generators[generators['Energy Source'].isin(
            ignore_energy_sources)]['Nameplate Capacity (MW)'].sum()/1000.0)
    print "Replacing 'Other' for 'Gas' as energy source for {:.2f} GW of capacity".format(
        generators[generators['Energy Source'] == 'Other'][
            'Nameplate Capacity (MW)'].sum()/1000.0)
    generators.drop(generators[generators['Energy Source'].isin(
            ignore_energy_sources)].index, inplace=True)
    generators.replace({'Energy Source':{'Other':'Gas'}}, inplace=True)

    # Reading in the previously processed list of generators in WECC states that are retired or have
    # planned retirements, but are still in the list of existing or planned generation projects in WECC states.
    # This list of retired generators has had its capacity aggregated to the plant level by energy source, prime mover, and
    # operating year.

    retired_gens = read_output_csv('retired_WECC_aggregated_generation_projects_{}.tab'.format(year))

    retired_gens = retired_gens.rename(columns = {'Nameplate Capacity (MW)':'retired_capacity_mw'})

    print "Joining the aggregated capacity by plant with retired capacity by plant..."

    #join the aggregated (by plant) retired generator projects with the aggregated existing generator projects (by plant)
    index_cols = ['EIA Plant Code','Prime Mover', 'State','County', 'Operating Year']

    generators_and_retired = pd.merge(generators, retired_gens, on=index_cols, how='left')

    #subtract out the retired nameplate capacity from the aggregated existing generator capacity
    generators_and_retired['net_operating_capacity_limit_mw'] = generators_and_retired['Nameplate Capacity (MW)']- generators_and_retired['retired_capacity_mw']

    #drop generators entirely if the remaining nameplate capacity = 0 after retirements are subtracted out
    generators_no_retired = generators_and_retired[generators_and_retired['net_operating_capacity_limit_mw'] != 0]

    #for several instances where only a portion of the nameplate capacity is retired, the Nameplate Capcity
    # column is replaced with this difference value of remaining capacity
    generators_no_retired['Nameplate Capacity (MW)'] = np.where(generators_no_retired['net_operating_capacity_limit_mw'] > 0, generators_no_retired['net_operating_capacity_limit_mw'], generators_no_retired['Nameplate Capacity (MW)'])

    print ("Dropping {} projects from generator list that have since been retired, totaling {:.2f} GW of capacity").format(
        len(generators_and_retired) - len(generators_no_retired),sum(generators_and_retired['retired_capacity_mw'])/1000.0)

    #calculating the "max_age" parameter for generators that are still operating but have a planned retirement date as
    #the Planned Retirement Year - Operating Year. If no retirement year not >0, make max age = 0. This will be replaced by techology default values in the database

    #generators_no_retired = generators_no_retired.astype({'Planned Retirement Year': 'int64', 'Operating Year':'int64'})
    generators_no_retired['Planned Retirement Year'][generators_no_retired['Planned Retirement Year'] == ' '] = 0
    generators_no_retired = generators_no_retired.astype({'Planned Retirement Year': 'float'})

    generators_no_retired['max_age'] = np.where(generators_no_retired['Planned Retirement Year'] > 0, generators_no_retired['Planned Retirement Year'] - generators_no_retired['Operating Year'], 0)

    generators_no_retired = generators_no_retired.astype({'max_age': 'int64'})

    #output to CSV the list of generators without retirements
    fname = 'WECC_non_retired_generation_projects_{}.tab'.format(year)
    with open(os.path.join(outputs_directory, fname),'w') as f:
        generators_no_retired.to_csv(f, sep='\t', encoding='utf-8', index=False)
        print "Saved data to {} file.\n".format(fname)

    #output to CSV the list of generators with retirements still flagged
    fname= 'WECC_generators_and_retired_projects_{}.tab'.format(year)
    with open(os.path.join(outputs_directory, fname),'w') as f:
        generators_and_retired.to_csv(f, sep='\t', encoding='utf-8', index=False)
        print "Saved data to {} file.\n".format(fname)

    #Dropping the unnecssary columns and renaming the dataframe back to "generators" now that the capacity of retired generators has been removed
    generators_no_retired = generators_no_retired.rename(columns={'Plant Name_x':'Plant Name'})
    generators_no_retired = generators_no_retired.drop(['Plant Name_y','retired_capacity_mw','Regulatory Status','net_operating_capacity_limit_mw'], axis=1)

    generators = generators_no_retired

    def weighted_avg(group, avg_name, weight_name):
        """
        Plant-level heat rates are calculated by doing a capacity-weighted average
        over the individual heat rates of each unit in the plant that have the same
        technology and use the same energy source. This allows obtaining a single
        heat rate for plants with units that have different vintages.

        http://stackoverflow.com/questions/10951341/pandas-dataframe-aggregate-function-using-multiple-columns
        """
        d = group[avg_name]
        w = group[weight_name]
        try:
            return (d * w).sum() / w.sum()
        except ZeroDivisionError:
            return d.mean()


    index_cols = ['EIA Plant Code','Prime Mover','Energy Source']
    print "Calculating capacity-weighted average heat rates per plant, technology and energy source..."
    generators = pd.merge(generators,
        pd.DataFrame(generators.groupby(index_cols).apply(weighted_avg, 'Best Heat Rate',
        'Nameplate Capacity (MW)')).reset_index().replace(0, float('nan')),
        how='right',
        on=index_cols).drop('Best Heat Rate', axis=1)

    print "Calculating maximum capacity limits per plant, technology and energy source..."
    gb = generators.groupby(index_cols)
    agg_generators = gb.agg({col:sum if col == 'Nameplate Capacity (MW)' else 'max'
                                    for col in generators.columns}).rename(columns=
                                    {'Nameplate Capacity (MW)':'capacity_limit_mw'}).reset_index(drop=True)
    generators = pd.merge(generators, agg_generators[index_cols+['capacity_limit_mw']],
        on=index_cols, how='right').reset_index(drop=True)

    print "Assigning baseload, variable and cogen flags..."
    generators.loc[:,'is_baseload'] = np.where(generators['Energy Source'].isin(
        ['Nuclear','Coal','Geothermal']),True,False)
    generators.loc[:,'is_variable'] = np.where(generators['Prime Mover'].isin(
        ['HY','PV','WT']),True,False)
    if 'Cogen' not in generators.columns:
        generators.loc[:,'is_cogen'] = False
    else:
        generators.loc[:,'is_cogen'] = np.where(generators['Cogen'] == 'Y',True,False)

    database_column_renaming_dict = {
        'EIA Plant Code':'eia_plant_code',
        'Plant Name':'name',
        'Prime Mover':'gen_tech',
        'Energy Source':'energy_source',
        0:'full_load_heat_rate',
        'Operating Year':'build_year',
        'Nameplate Capacity (MW)':'capacity',
        'max_age':'max_age'
        }

    generators.rename(columns=database_column_renaming_dict, inplace=True)

    generators.replace(' ',float('nan'), inplace=True)

    #round full load heat rate column to 3 decimal places
    generators['full_load_heat_rate'] = generators['full_load_heat_rate'].round(decimals=3)

    #rename battery storage gen_tech to match database naming convention
    generators['gen_tech'] = np.where(generators['gen_tech'] == 'BA', 'Battery_Storage', generators['gen_tech'])

    carry_on = getpass.getpass('WARNING: In order to push projects into the DB,'
        'all projects currently in the generation_plant table that are'
        'not present in the generation_plant_scenario_member table will be'
        'removed. Continue? [y/n]')
    while carry_on not in ['y','n']:
        carry_on = getpass.getpass('WARNING: In order to push projects into the DB,'
        'all projects currently in the generation_plant table that are'
        'not present in the generation_plant_scenario_member table will be'
        'removed. Continue? [y/n]')
    if carry_on == 'n':
        sys.exit()

    print "\n-----------------------------"
    print "Pushing generation plants to the DB:\n"

    # Make sure the "switch" schema is on the search path

    # Drop NOT NULL constraint for load_zone_id
    query = 'ALTER TABLE "{PREFIX}generation_plant" ALTER "load_zone_id" DROP NOT NULL;'.format(PREFIX=PREFIX)
    connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True)
    query = 'ALTER TABLE "{PREFIX}generation_plant" ALTER "max_age" DROP NOT NULL;'.format(PREFIX=PREFIX)
    connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True)

    # First, define gen_scenario_id as new_disaggregated_gen_scenario_id and delete previously stored projects for the scenario id
    gen_scenario_id = new_disaggregated_gen_scenario_id
    # Also define hydro simple scenario and generation_plant_cost scenario and delete previously stored projects for these scenario ids
    hydro_scenario_id = new_disaggregated_hydro_simple_scenario_id
    generation_plant_cost_id = new_disggregated_generation_plant_cost_id

    query = 'DELETE FROM {PREFIX}hydro_historical_monthly_capacity_factors\
        WHERE hydro_simple_scenario_id = {hydro_scenario_id}'.format(PREFIX = PREFIX, hydro_scenario_id = hydro_scenario_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    query = 'DELETE FROM {PREFIX}generation_plant_scenario_member\
        WHERE generation_plant_scenario_id = {gen_scenario_id}'.format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    query = 'DELETE FROM {PREFIX}generation_plant_cost\
        WHERE generation_plant_cost_scenario_id = {generation_plant_cost_id}'.format(PREFIX = PREFIX, generation_plant_cost_id = generation_plant_cost_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    query = 'DELETE FROM {PREFIX}generation_plant_existing_and_planned\
        WHERE generation_plant_existing_and_planned_scenario_id = {gen_scenario_id}'.format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    # These queries are for the scenario mapping tables to add descriptions of new scenarios
    query = 'DELETE FROM {PREFIX}hydro_simple_scenario\
        WHERE hydro_simple_scenario_id = {hydro_scenario_id}'.format(PREFIX = PREFIX, hydro_scenario_id = hydro_scenario_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    query = 'DELETE FROM {PREFIX}generation_plant_cost_scenario\
        WHERE generation_plant_cost_scenario_id = {generation_plant_cost_id}'.format(PREFIX = PREFIX, generation_plant_cost_id = generation_plant_cost_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    query = 'DELETE FROM {PREFIX}generation_plant_existing_and_planned_scenario\
        WHERE generation_plant_existing_and_planned_scenario_id = {gen_scenario_id}'.format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    query = 'DELETE FROM {PREFIX}generation_plant_scenario\
        WHERE generation_plant_scenario_id = {gen_scenario_id}'.format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    # It is necessary to temporarily disable triggers when deleting from
    # generation_plant table, because of multiple fkey constraints
    query = 'SET session_replication_role = replica;\
            DELETE FROM {PREFIX}generation_plant\
            WHERE generation_plant_id NOT IN\
            (SELECT generation_plant_id FROM {PREFIX}generation_plant_scenario_member);\
            SET session_replication_role = DEFAULT;'.format(PREFIX = PREFIX)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    print "Deleted previously stored projects for the EIA dataset (id {}). Pushing data...".format(gen_scenario_id)

    query = 'SELECT last_value FROM generation_plant_id_seq'
    first_gen_id = connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True).iloc[0,0] + 1

    # Aded max_age to the list of uploaded columns for generators will planned
    # retirements, if a generator has no planned retirement, default max_age will be assigned in a later step
    generators_to_db = generators[['name','gen_tech','capacity_limit_mw',
        'full_load_heat_rate','max_age','is_variable','is_baseload','is_cogen',
        'energy_source','eia_plant_code', 'Latitude','Longitude','County',
        'State']].drop_duplicates()

    connect_to_db_and_push_df(df=generators_to_db,
        col_formats=("(DEFAULT,%s,%s,NULL,NULL,%s,NULL,NULL,NULL,%s,NULL,%s,NULL,%s,%s,%s,%s,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,%s,%s,%s,%s,%s,NULL,NULL,NULL)"),
        table='{PREFIX}generation_plant'.format(PREFIX = PREFIX),
        database='switch_wecc', user=user, password=password, quiet=True)
    print "Successfully pushed generation plants!"

    query = 'SELECT last_value FROM generation_plant_id_seq'
    last_gen_id = connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True).iloc[0,0]

    # Populate geometry column for GIS work, using coordinate reference system 4326-WGS4 (common projection default)
    query = "UPDATE {PREFIX}generation_plant\
        SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)\
        WHERE longitude IS NOT NULL AND latitude IS NOT NULL AND\
        generation_plant_id BETWEEN {first_gen_id} AND {last_gen_id}".format(PREFIX = PREFIX, first_gen_id = first_gen_id, last_gen_id = last_gen_id)
    connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True)

    # assigning generators to load zones

    # if generator lat-lon is available assign if within load zone boundary
    print "\nAssigning load zones..."
    query = "UPDATE {PREFIX}generation_plant SET load_zone_id = z.load_zone_id\
        FROM {PREFIX}load_zone z\
        WHERE ST_contains(boundary, geom) AND\
        generation_plant_id BETWEEN {first_gen_id} AND {last_gen_id}".format(PREFIX = PREFIX, first_gen_id = first_gen_id, last_gen_id = last_gen_id)
    connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True)
    n_plants_assigned_by_lat_long = connect_to_db_and_run_query("SELECT count(*)\
        FROM {PREFIX}generation_plant WHERE load_zone_id IS NOT NULL AND\
        generation_plant_id BETWEEN {first_gen_id} AND {last_gen_id}".format(PREFIX = PREFIX, first_gen_id = first_gen_id, last_gen_id = last_gen_id),
        database='switch_wecc', user=user, password=password, quiet=True).iloc[0,0]
    print "--Assigned load zone according to lat & long to {} plants".format(
        n_plants_assigned_by_lat_long)

    #if generator lat-lon is not available, assign load zone based on state and county of generator
    query = "UPDATE {PREFIX}generation_plant g SET load_zone_id = z.load_zone_id\
        FROM {PREFIX}us_counties c\
        JOIN {PREFIX}load_zone z ON ST_contains(z.boundary, ST_centroid(c.the_geom))\
        WHERE g.load_zone_id IS NULL AND g.state = c.state_name AND g.county = c.name\
        AND generation_plant_id BETWEEN {first_gen_id} AND {last_gen_id}".format(PREFIX = PREFIX, first_gen_id = first_gen_id, last_gen_id = last_gen_id)
    connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True)
    n_plants_assigned_by_county_state = connect_to_db_and_run_query("SELECT count(*)\
        FROM {PREFIX}generation_plant WHERE load_zone_id IS NOT NULL AND\
        generation_plant_id BETWEEN {first_gen_id} AND {last_gen_id}".format(PREFIX = PREFIX, first_gen_id = first_gen_id, last_gen_id = last_gen_id),
        database='switch_wecc', user=user, password=password, quiet=True
        ).iloc[0,0] - n_plants_assigned_by_lat_long
    print "--Assigned load zone according to county & state to {} plants".format(
        n_plants_assigned_by_county_state)

    # Plants that are located outside of the WECC region boundary get assigned
    # to the nearest load zone, ONLY if they are located less than 100 miles
    # out of the boundary
    query = "UPDATE {PREFIX}generation_plant AS g1 SET load_zone_id = lz1.load_zone_id\
        FROM {PREFIX}load_zone lz1\
        WHERE g1.load_zone_id is NULL AND g1.geom IS NOT NULL\
        AND g1.generation_plant_id between {first_gen_id} AND {last_gen_id}\
        AND ST_Distance(g1.geom::geography,lz1.boundary::geography)/1609 < 100\
        AND ST_Distance(g1.geom::geography,lz1.boundary::geography)/1609 = \
        (SELECT min(ST_Distance(g2.geom::geography,lz2.boundary::geography)/1609)\
        FROM {PREFIX}generation_plant g2\
        CROSS JOIN {PREFIX}load_zone lz2\
        WHERE g2.load_zone_id is NULL AND g2.geom IS NOT NULL\
        AND g2.generation_plant_id = g1.generation_plant_id)".format(PREFIX = PREFIX, first_gen_id = first_gen_id, last_gen_id = last_gen_id)
    connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True)
    n_plants_assigned_to_nearest_lz = connect_to_db_and_run_query("SELECT count(*)\
        FROM {PREFIX}generation_plant WHERE load_zone_id IS NOT NULL AND\
        generation_plant_id BETWEEN {first_gen_id} AND {last_gen_id}".format(PREFIX = PREFIX, first_gen_id = first_gen_id, last_gen_id = last_gen_id),
        database='switch_wecc', user=user, password=password, quiet=True
        ).iloc[0,0] - n_plants_assigned_by_lat_long - n_plants_assigned_by_county_state
    print "--Assigned load zone according to nearest load zone to {} plants".format(
        n_plants_assigned_to_nearest_lz)

    plants_wo_load_zone_count_and_cap = connect_to_db_and_run_query("SELECT count(*),\
        sum(capacity_limit_mw) FROM {PREFIX}generation_plant WHERE load_zone_id IS NULL\
        AND generation_plant_id BETWEEN {first_gen_id} AND {last_gen_id}".format(PREFIX = PREFIX, first_gen_id = first_gen_id, last_gen_id = last_gen_id),
        database='switch_wecc', user=user, password=password, quiet=True)
    if plants_wo_load_zone_count_and_cap.iloc[0,0] > 0:
        print ("--WARNING: There are {:.0f} plants with a total of {:.2f} GW of capacity"
        " w/o an assigned load zone. These will be removed.").format(
        plants_wo_load_zone_count_and_cap.iloc[0,0],
        plants_wo_load_zone_count_and_cap.iloc[0,1]/1000.0)
        connect_to_db_and_run_query("DELETE FROM {PREFIX}generation_plant\
            WHERE load_zone_id IS NULL AND generation_plant_id BETWEEN {first_gen_id}\
            AND {last_gen_id}".format(PREFIX = PREFIX, first_gen_id = first_gen_id, last_gen_id = last_gen_id),
            database='switch_wecc', user=user, password=password, quiet=True)

    # Assign default technology values

    # Note: Outside of this script I had previously copied the default technology parameters of wind for battery storage (max age = 20, variable om, forced outage rate, scheduled outage rate all
    # = 0,  and existing fixed om cost, and existing overnight cost are null. Then updated to have max age of 10, forced out = 0.02, scheduled out = 0.0055)
    #postgres queries:
        # insert into switch.generation_plant_technologies (gen_tech, energy_source, max_age, variable_o_m, forced_outage_rate, scheduled_outage_rate, existing_fixed_o_m_cost, existing_overnight_cost)
        #select 'Battery_Storage' as gen_tech, s.energy_source, s.max_age, s.variable_o_m, s.forced_outage_rate, s.scheduled_outage_rate, s.existing_fixed_o_m_cost, s.existing_overnight_cost
        #from switch.scenario s where
        #gen_tech='WT';

        #UPDATE switch.generation_plant_technologies SET
        #energy_source = 'Electricity',
        #max_age = 10,
        #forced_outage_rate = 0.02,
        #scheduled_outage_rate = 0.0055,
        #WHERE gen_tech='Battery_Storage';

    print "\nAssigning default technology parameter values for forced outages, scheduled outages, and variable O&M..."
    for param in ['forced_outage_rate','scheduled_outage_rate', 'variable_o_m']:
        query = "UPDATE {PREFIX}generation_plant g SET {param} = t.{param}\
                FROM {PREFIX}generation_plant_technologies t\
                WHERE g.energy_source = t.energy_source AND\
                g.gen_tech = t.gen_tech AND generation_plant_id BETWEEN {first_gen_id} AND\
                {last_gen_id}".format(PREFIX = PREFIX, param=param, first_gen_id=first_gen_id, last_gen_id=last_gen_id)
        connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)
        print "--Assigned {}".format(param)

    # Assign default max_age values for plants that don't have planned retirements
    print "\nAssigning default technology max age values..."
    for param in ['max_age']:
        query = "UPDATE {PREFIX}generation_plant g SET {param} = t.{param}\
                FROM {PREFIX}generation_plant_technologies t\
                WHERE g.max_age = 0 AND\
                g.energy_source = t.energy_source AND\
                g.gen_tech = t.gen_tech AND generation_plant_id BETWEEN {first_gen_id} AND\
                {last_gen_id}".format(PREFIX = PREFIX, param=param, first_gen_id=first_gen_id, last_gen_id=last_gen_id)
        connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)
        print "--Assigned {}".format(param)

    # Assign default values for 'storage_efficiency' = 0.75 and 'store_to_release_ratio'= 1 for battery storage
    print "\nAssigning default technology values for battery storage..."
    query = "UPDATE {PREFIX}generation_plant SET\
        storage_efficiency = 0.75,\
        store_to_release_ratio = 1\
        WHERE energy_source = 'Electricity' AND\
        gen_tech = 'Battery_Storage' AND\
        generation_plant_id BETWEEN {first_gen_id} AND\
        {last_gen_id}".format(PREFIX = PREFIX, first_gen_id=first_gen_id, last_gen_id=last_gen_id)
    connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True)
    print "--Assigned battery storage technology parameters."

    print "Adding scenario id numbers and descriptions to scenario mapping tables..."

    # Copying a previous scenario and updating with new scenario id and description to hydro_simple_scenario table
    query = "INSERT into {PREFIX}hydro_simple_scenario (hydro_simple_scenario_id, name, description) \
            SELECT {hydro_scenario_id} as hydro_simple_scenario_id, name, description \
            FROM {PREFIX}hydro_simple_scenario \
            WHERE hydro_simple_scenario_id = 1".format(PREFIX = PREFIX, hydro_scenario_id = hydro_scenario_id)
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)

    query = "UPDATE {PREFIX}hydro_simple_scenario\
            SET name = 'EIA923 datasets 2004 until 2018',\
                description = 'Pumped hydro units are modeled as simple turbines (summing netgen and electricity consumption columns).'\
            WHERE hydro_simple_scenario_id = {hydro_scenario_id}".format(PREFIX = PREFIX,hydro_scenario_id = hydro_scenario_id )
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)
    print "Updated hydro_simple_scenario table with new scenario id"

    # Copying a previous scenario and updating with new scenario id and description to generation_plant_cost_scenario table
    query = "INSERT into {PREFIX}generation_plant_cost_scenario (generation_plant_cost_scenario_id, name, description) \
            SELECT {generation_plant_cost_id} as generation_plant_cost_scenario_id, name, description \
            FROM {PREFIX}generation_plant_cost_scenario \
            WHERE generation_plant_cost_scenario_id = 1".format(PREFIX = PREFIX, generation_plant_cost_id = generation_plant_cost_id)
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)

    query = "UPDATE {PREFIX}generation_plant_cost_scenario\
            SET name = 'EIA-WECC Existing and Proposed 2018',\
                description = 'Dataset from the EIA 860 and EIA 923 forms not aggregated by LZ. Approximately 2602 existing and 142 proposed generators.'\
            WHERE generation_plant_cost_scenario_id = {generation_plant_cost_id}".format(PREFIX = PREFIX,generation_plant_cost_id = generation_plant_cost_id )
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)
    print "Updated generation_plant_cost_scenario table with new scenario id"

    # Copying a previous scenario and updating with new scenario id and description to generation_plant_scenario table
    query = "INSERT into {PREFIX}generation_plant_scenario (generation_plant_scenario_id, name, description) \
            SELECT {gen_scenario_id} as generation_plant_scenario_id, name, description \
            FROM {PREFIX}generation_plant_scenario \
            WHERE generation_plant_scenario_id = 1".format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)

    query = "UPDATE {PREFIX}generation_plant_scenario\
            SET name = 'EIA-WECC Existing and Proposed 2018',\
                description = 'Dataset from the EIA 860 and EIA 923 forms not aggregated by LZ. Approximately 2602 existing and 142 proposed generators.'\
            WHERE generation_plant_scenario_id = {gen_scenario_id}".format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id )
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)
    print "Updated generation_plant_scenario table with new scenario id"

    # Copying a previous scenario and updating with new scenario id and description to generation_plant_existing_and_planned_scenario table
    query = "INSERT into {PREFIX}generation_plant_existing_and_planned_scenario (generation_plant_existing_and_planned_scenario_id, name, description) \
            SELECT {gen_scenario_id} as generation_plant_existing_and_planned_scenario_id, name, description \
            FROM {PREFIX}generation_plant_existing_and_planned_scenario \
            WHERE generation_plant_existing_and_planned_scenario_id = 1".format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)

    query = "UPDATE {PREFIX}generation_plant_existing_and_planned_scenario\
            SET name = 'EIA-WECC Existing and Proposed 2018',\
                description = 'Existing and proposed generators in the WECC region scraped from the EIA860 data form, without aggregation by LZ. Heat rates were processed from the EIA 923 form. The scraping package may be found at: https://github.com/RAEL-Berkeley/eia_scrape.'\
            WHERE generation_plant_existing_and_planned_scenario_id = {gen_scenario_id}".format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id )
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)
    print "Updated generation_plant_existing_and_planned_scenario table with new scenario id"

    # Now, create scenario and assign ids for this scenario
    # Get the actual list of ids in the table, since some rows were deleted
    # because no load zone could be assigned to those projects
    print "\nAssigning all individual plants to scenario id {}...".format(gen_scenario_id)
    query = 'SELECT generation_plant_id FROM {PREFIX}generation_plant\
        WHERE generation_plant_id BETWEEN {first_gen_id} AND {last_gen_id}'.format(PREFIX = PREFIX, first_gen_id = first_gen_id, last_gen_id = last_gen_id)
    gen_plant_ids = connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)
    gen_plant_ids['generation_plant_scenario_id'] = gen_scenario_id

    connect_to_db_and_push_df(df=gen_plant_ids[['generation_plant_scenario_id','generation_plant_id']],
        col_formats="(%s,%s)", table='{PREFIX}generation_plant_scenario_member'.format(PREFIX = PREFIX),
        database='switch_wecc', user=user, password=password, quiet=True)
    print "Successfully assigned pushed generation plants to a scenario!"

    # Restore original NOT NULL constraint
    query = 'ALTER TABLE "{PREFIX}generation_plant" ALTER "load_zone_id" SET NOT NULL;'.format(PREFIX = PREFIX)
    connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True)
    query = 'ALTER TABLE "{PREFIX}generation_plant" ALTER "max_age" SET NOT NULL;'.format(PREFIX = PREFIX)
    connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True)

    # Get the list of indexes of plants actually uploaded
    print "\nAssigning build years to generation plants..."
    query = 'SELECT * FROM {PREFIX}generation_plant\
        JOIN {PREFIX}generation_plant_scenario_member USING (generation_plant_id)\
        WHERE generation_plant_scenario_id = {gen_scenario_id}'.format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    gens_in_db = connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)
    gen_indexes_in_db = gens_in_db[['generation_plant_id','eia_plant_code','energy_source','gen_tech']]

    # Creating the generation_plant_existing_and_planned_scenario_id as the same scenario as generation_plant_scenario_id
    # Uploading the build years of the generators
    build_years_df = pd.merge(generators, gen_indexes_in_db,
        on=['eia_plant_code','energy_source','gen_tech'])[['generation_plant_id',
        'build_year','capacity']]
    build_years_df['generation_plant_existing_and_planned_scenario_id'] = gen_scenario_id
    build_years_df = build_years_df[[
        'generation_plant_existing_and_planned_scenario_id','generation_plant_id',
        'build_year','capacity']]
    connect_to_db_and_push_df(df=build_years_df,
        col_formats="(%s,%s,%s,%s)", table='{PREFIX}generation_plant_existing_and_planned'.format(PREFIX = PREFIX),
        database='switch_wecc', user=user, password=password, quiet=True)
    print "Successfully uploaded build years!"

    # assigning a default 0 for fixed o_m and overnight costs in the generation_plant_cost table
    print "\nAssigning fixed and investment costs to generation plants..."
    cost_df = build_years_df.rename(columns={
        'generation_plant_existing_and_planned_scenario_id':
        'generation_plant_cost_scenario_id'}).drop('capacity', axis=1)
    cost_df['generation_plant_cost_scenario_id'] = generation_plant_cost_id
    cost_df['fixed_o_m'] = 0
    cost_df['overnight_cost'] = 0

    connect_to_db_and_push_df(df=cost_df,
        col_formats="(%s,%s,%s,%s,%s)", table='{PREFIX}generation_plant_cost'.format(PREFIX = PREFIX),
        database='switch_wecc', user=user, password=password, quiet=True)
    print "Successfully uploaded fixed and capital costs!"

    # Read hydro capacity factor data, merge with generators in the database, and upload
    # monthly avg flow = monthly CF * nameplate capacity and monthly minimum flow is half the avg monthly flow
    print "\nUploading hydro capacity factors..."
    hydro_cf = read_output_csv('historic_hydro_capacity_factors_NARROW.tab').rename(
        columns={'Plant Code':'eia_plant_code','Prime Mover':'gen_tech'}).drop_duplicates()
    hydro_cf = pd.merge(hydro_cf,gen_indexes_in_db[['generation_plant_id','eia_plant_code','gen_tech']],
        on=['eia_plant_code','gen_tech'], how='inner')
    hydro_cf.rename(columns={'Month':'month','Year':'year'}, inplace=True)
    hydro_cf.loc[:,'hydro_avg_flow_mw'] = hydro_cf.loc[:,'Capacity Factor'] * hydro_cf.loc[:,'Nameplate Capacity (MW)']
    hydro_cf.loc[:,'hydro_min_flow_mw'] = hydro_cf.loc[:,'hydro_avg_flow_mw'] / 2
    hydro_cf.loc[:,'hydro_simple_scenario_id'] = hydro_scenario_id
    fname = 'full_hydro_data.tab'
    with open(os.path.join(outputs_directory, fname),'w') as f:
        hydro_cf.to_csv(f, sep='\t', encoding='utf-8', index=False)
    hydro_cf = hydro_cf[['hydro_simple_scenario_id','generation_plant_id',
        'year','month','hydro_min_flow_mw','hydro_avg_flow_mw']]
    hydro_cf = hydro_cf.fillna(0.01)
    fname = 'to_explore_weird_hydro_data.tab'
    with open(os.path.join(outputs_directory, fname),'w') as f:
        hydro_cf.to_csv(f, sep='\t', encoding='utf-8', index=False)
    hydro_cf.drop_duplicates(subset=['hydro_simple_scenario_id','generation_plant_id', 'year','month'], inplace=True)
    # drop any duplicates if hydro_cf has duplicates

    connect_to_db_and_push_df(df=hydro_cf,
        col_formats="(%s,%s,%s,%s,%s,%s)", table='{PREFIX}hydro_historical_monthly_capacity_factors'.format(PREFIX = PREFIX),
        database='switch_wecc', user=user, password=password, quiet=True)
    print "Successfully uploaded historical hydro capacity factors for 2004 to 2018!"

    print "\n-----------------------------"
    print "Aggregating projects by load zone..."

    # Creating an aggregated version of the scenario above, aggregated by gen tech, energy source, HR, and load zone

    # First, group by load zone, gen tech, energy source and heat rate
    # (while calculating a capacity-weighted average heat rate)
    # and aggregate the generators
    gens_in_db['hr_group'] = gens_in_db['full_load_heat_rate'].fillna(0).round()
    gens_in_db['full_load_heat_rate'] *= gens_in_db['capacity_limit_mw']
    gens_in_db_cols = gens_in_db.columns
    gb = gens_in_db.groupby(['gen_tech','load_zone_id','energy_source',
        'hr_group'])
    aggregated_gens = gb.agg(
                {col:(sum if col in ['capacity_limit_mw','full_load_heat_rate']
                    else 'max') for col in gens_in_db.columns}).reset_index(drop=True)
    aggregated_gens['full_load_heat_rate'] /= aggregated_gens['capacity_limit_mw']
    aggregated_gens = aggregated_gens[gens_in_db_cols]

    # Now, clean up columns and add a LZ prefix to the name of aggregated generators
    aggregated_gens['name'] = ('LZ_' + aggregated_gens['load_zone_id'].map(str) + '_' +
        aggregated_gens['gen_tech'] + '_' + aggregated_gens['energy_source'] + '_HR_' +
        aggregated_gens['hr_group'].map(int).map(str))
    aggregated_gens.drop(['generation_plant_id','generation_plant_scenario_id',
        'eia_plant_code','latitude','longitude','county','state'],
        axis=1, inplace=True)
    print "Aggregated into {} projects.".format(len(aggregated_gens))

    # First, define gen_scenario_id as new_aggregated_gen_scenario_id, and
    # delete previously stored projects for the aggregated plants
     # Also define hydro simple scenario and generation_plant_cost scenario and delete previously stored projects for these scenario ids

    gen_scenario_id = new_aggregated_gen_scenario_id
    hydro_scenario_id = new_aggregated_hydro_simple_scenario_id
    generation_plant_cost_id = new_aggregated_generation_plant_cost_id

    query = 'DELETE FROM {PREFIX}generation_plant_scenario_member\
        WHERE generation_plant_scenario_id = {gen_scenario_id}'.format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    query = 'DELETE FROM {PREFIX}generation_plant_existing_and_planned\
        WHERE generation_plant_existing_and_planned_scenario_id = {gen_scenario_id}'.format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    query = 'DELETE FROM {PREFIX}generation_plant_cost\
        WHERE generation_plant_cost_scenario_id = {generation_plant_cost_id}'.format(PREFIX = PREFIX, generation_plant_cost_id = generation_plant_cost_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    query = 'DELETE FROM {PREFIX}hydro_historical_monthly_capacity_factors\
        WHERE hydro_simple_scenario_id = {hydro_scenario_id}'.format(PREFIX = PREFIX, hydro_scenario_id = hydro_scenario_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    # Added these queries for the scenario mapping tables to add descriptions of new scenarios
    query = 'DELETE FROM {PREFIX}hydro_simple_scenario\
        WHERE hydro_simple_scenario_id = {hydro_scenario_id}'.format(PREFIX = PREFIX, hydro_scenario_id = hydro_scenario_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    query = 'DELETE FROM {PREFIX}generation_plant_cost_scenario\
        WHERE generation_plant_cost_scenario_id = {generation_plant_cost_id}'.format(PREFIX = PREFIX, generation_plant_cost_id = generation_plant_cost_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    query = 'DELETE FROM {PREFIX}generation_plant_existing_and_planned_scenario\
        WHERE generation_plant_existing_and_planned_scenario_id = {gen_scenario_id}'.format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    query = 'DELETE FROM {PREFIX}generation_plant_scenario\
        WHERE generation_plant_scenario_id = {gen_scenario_id}'.format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    # It is necessary to temporarily disable triggers when deleting from
    # generation_plant table, because of multiple fkey constraints
    query = 'SET session_replication_role = replica;\
            DELETE FROM {PREFIX}generation_plant\
            WHERE generation_plant_id NOT IN\
            (SELECT generation_plant_id FROM {PREFIX}generation_plant_scenario_member);\
            SET session_replication_role = DEFAULT;'.format(PREFIX = PREFIX)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)
    print "\nDeleted previously stored projects for the load zone-aggregated EIA dataset. Pushing data..."

    query = 'SELECT last_value FROM generation_plant_id_seq'
    first_gen_id = connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True).iloc[0,0] + 1

    connect_to_db_and_push_df(df=aggregated_gens.drop(['hr_group','geom', 'substation_connection_geom', 'geom_area'], axis=1),
        col_formats=("(DEFAULT,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
            "%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NULL,NULL,NULL,NULL,NULL)"),
        table='{PREFIX}generation_plant'.format(PREFIX = PREFIX),
        database='switch_wecc', user=user, password=password, quiet=True)
    print "Successfully pushed aggregated project data!"

    query = 'SELECT last_value FROM generation_plant_id_seq'
    last_gen_id = connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True).iloc[0,0]

    print "Adding scenario id numbers and descriptions to scenario mapping tables..."

    # Copying a previous scenario and updating with new scenario id and description to hydro_simple_scenario table
    query = "INSERT into {PREFIX}hydro_simple_scenario (hydro_simple_scenario_id, name, description) \
            SELECT {hydro_scenario_id} as hydro_simple_scenario_id, name, description \
            FROM {PREFIX}hydro_simple_scenario \
            WHERE hydro_simple_scenario_id = 1".format(PREFIX = PREFIX, hydro_scenario_id = hydro_scenario_id)
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)

    query = "UPDATE {PREFIX}hydro_simple_scenario\
            SET name = 'EIA923 datasets 2004 until 2018 Aggregated by LZ',\
                description = 'Same as scenario id 19, but aggregated by load zone.'\
            WHERE hydro_simple_scenario_id = {hydro_scenario_id}".format(PREFIX = PREFIX,hydro_scenario_id = hydro_scenario_id )
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)
    print "Updated hydro_simple_scenario table with new scenario id"

    # Copying a previous scenario and updating with new scenario id and description to generation_plant_cost_scenario table
    query = "INSERT into {PREFIX}generation_plant_cost_scenario (generation_plant_cost_scenario_id, name, description) \
            SELECT {generation_plant_cost_id} as generation_plant_cost_scenario_id, name, description \
            FROM {PREFIX}generation_plant_cost_scenario \
            WHERE generation_plant_cost_scenario_id = 1".format(PREFIX = PREFIX, generation_plant_cost_id = generation_plant_cost_id)
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)

    query = "UPDATE {PREFIX}generation_plant_cost_scenario\
            SET name = 'EIA-WECC Existing and Proposed 2018 Aggregated by LZ',\
                description = 'Dataset from the EIA 860 and EIA 923 forms aggregated by LZ.'\
            WHERE generation_plant_cost_scenario_id = {generation_plant_cost_id}".format(PREFIX = PREFIX,generation_plant_cost_id = generation_plant_cost_id )
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)
    print "Updated generation_plant_cost_scenario table with new scenario id"

    # Copying a previous scenario and updating with new scenario id and description to generation_plant_scenario table
    query = "INSERT into {PREFIX}generation_plant_scenario (generation_plant_scenario_id, name, description) \
            SELECT {gen_scenario_id} as generation_plant_scenario_id, name, description \
            FROM {PREFIX}generation_plant_scenario \
            WHERE generation_plant_scenario_id = 1".format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)

    query = "UPDATE {PREFIX}generation_plant_scenario\
            SET name = 'EIA-WECC Existing and Proposed 2018 Aggregated by LZ',\
                description = 'Dataset from the EIA 860 and EIA 923 forms aggregated by LZ.'\
            WHERE generation_plant_scenario_id = {gen_scenario_id}".format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id )
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)
    print "Updated generation_plant_scenario table with new scenario id"

    # Copying a previous scenario and updating with new scenario id and description to generation_plant_existing_and_planned_scenario table
    query = "INSERT into {PREFIX}generation_plant_existing_and_planned_scenario (generation_plant_existing_and_planned_scenario_id, name, description) \
            SELECT {gen_scenario_id} as generation_plant_existing_and_planned_scenario_id, name, description \
            FROM {PREFIX}generation_plant_existing_and_planned_scenario \
            WHERE generation_plant_existing_and_planned_scenario_id = 1".format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)

    query = "UPDATE {PREFIX}generation_plant_existing_and_planned_scenario\
            SET name = 'EIA-WECC Existing and Proposed 2018 Aggregated by LZ',\
                description = 'Existing and proposed generators in the WECC region scraped from the EIA860 data form, aggregated by LZ. Heat rates were processed from the EIA 923 form. The scraping package may be found at: https://github.com/RAEL-Berkeley/eia_scrape'\
            WHERE generation_plant_existing_and_planned_scenario_id = {gen_scenario_id}".format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id )
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)
    print "Updated generation_plant_existing_and_planned_scenario table with new scenario id"

    #Now assigning all generators and their costs to the scenario
    print "\nAssigning all aggregated plants to scenario id {}...".format(gen_scenario_id)

    query = 'INSERT INTO {PREFIX}generation_plant_scenario_member\
    (SELECT {gen_scenario_id}, generation_plant_id FROM {PREFIX}generation_plant\
        WHERE generation_plant_id BETWEEN {first_gen_id} AND {last_gen_id})'.format(gen_scenario_id = gen_scenario_id, PREFIX = PREFIX, first_gen_id = first_gen_id, last_gen_id = last_gen_id)
    connect_to_db_and_run_query(query,
        database='switch_wecc', user=user, password=password, quiet=True)
    print "Successfully assigned pushed generation plants to a scenario!"

    print "\nAssigning build years to generation plants..."
    query = 'SELECT * FROM {PREFIX}generation_plant\
        JOIN {PREFIX}generation_plant_scenario_member USING (generation_plant_id)\
        WHERE generation_plant_scenario_id = {gen_scenario_id}'.format(PREFIX = PREFIX, gen_scenario_id = gen_scenario_id)
    aggregated_gens_in_db = connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    #assigning same gen_scenario_id to generation_plant_existing_and_planned_scenario_id
    aggregated_gens_in_db['hr_group'] = aggregated_gens_in_db['full_load_heat_rate'].fillna(0).round()
    aggregated_gens_in_db['generation_plant_existing_and_planned_scenario_id'] = gen_scenario_id
    gens_in_db = pd.merge(gens_in_db, generators[['eia_plant_code','energy_source',
        'gen_tech','capacity','build_year']],
        on=['eia_plant_code','energy_source','gen_tech'], suffixes=('','_y'))
    aggregated_gens_bld_yrs = pd.merge(aggregated_gens_in_db, gens_in_db,
        on=['load_zone_id','energy_source','gen_tech','hr_group'], suffixes=('','_y'))[[
        'generation_plant_existing_and_planned_scenario_id',
        'generation_plant_id','build_year','capacity']]
    aggregated_gens_bld_yrs_cols = list(aggregated_gens_bld_yrs.columns)

    #keep the most recent build year of the aggregation of generators
    gb = aggregated_gens_bld_yrs.groupby(aggregated_gens_bld_yrs_cols[:-1])
    aggregated_gens_bld_yrs = gb.agg(
        {col:(sum if col=='capacity' else 'max')
        for col in aggregated_gens_bld_yrs.columns}).reset_index(drop=True)
    aggregated_gens_bld_yrs = aggregated_gens_bld_yrs[aggregated_gens_bld_yrs_cols]

    connect_to_db_and_push_df(df=aggregated_gens_bld_yrs,
        col_formats="(%s,%s,%s,%s)",
        table='{PREFIX}generation_plant_existing_and_planned'.format(PREFIX = PREFIX),
        database='switch_wecc', user=user, password=password, quiet=True)
    print "Successfully pushed aggregated project build years data!"

    print "\nAssigning fixed and investment costs to generation plants..."
    aggregated_gens_costs = aggregated_gens_bld_yrs.rename(columns={
        'generation_plant_existing_and_planned_scenario_id':
        'generation_plant_cost_scenario_id'}).drop('capacity', axis=1)
    aggregated_gens_costs['generation_plant_cost_scenario_id'] = generation_plant_cost_id
    aggregated_gens_costs['fixed_o_m'] = 0
    aggregated_gens_costs['overnight_cost'] = 0

    connect_to_db_and_push_df(df=aggregated_gens_costs,
        col_formats="(%s,%s,%s,%s,%s)", table='{PREFIX}generation_plant_cost'.format(PREFIX = PREFIX),
        database='switch_wecc', user=user, password=password, quiet=True)
    print "Successfully uploaded fixed and capital costs!"

    print "\nAggregating and uploading hydro capacity factors..."
    agg_hydro_cf = read_output_csv('historic_hydro_capacity_factors_NARROW.tab').rename(
        columns={'Plant Code':'eia_plant_code','Prime Mover':'gen_tech',
        'Month':'month','Year':'year'})
    agg_hydro_cf.loc[:,'hydro_avg_flow_mw'] = (agg_hydro_cf.loc[:,'Capacity Factor'] *
        agg_hydro_cf.loc[:,'Nameplate Capacity (MW)'])
    agg_hydro_cf.loc[:,'hydro_min_flow_mw'] = agg_hydro_cf.loc[:,'hydro_avg_flow_mw'] / 2
    # The drop_duplicates command avoids double-counting plants with multiple build_years
    agg_hydro_cf = pd.merge(agg_hydro_cf, gens_in_db[[
        'eia_plant_code','gen_tech','load_zone_id','generation_plant_id']].drop_duplicates(),
        on=['eia_plant_code', 'gen_tech'], how='inner')
    agg_hydro_cf['hydro_simple_scenario_id'] = hydro_scenario_id
    gb = agg_hydro_cf.groupby(['load_zone_id','gen_tech','month','year'])
    agg_hydro_cf = gb.agg(
        {col:(sum if col in ['hydro_min_flow_mw','hydro_avg_flow_mw'] else 'max')
        for col in agg_hydro_cf.columns}).reset_index(drop=True)

    agg_hydro_cf = pd.merge(aggregated_gens_in_db, agg_hydro_cf,
        on=['load_zone_id', 'gen_tech'], how='inner', suffixes=('','_y'))
    agg_hydro_cf = agg_hydro_cf[['hydro_simple_scenario_id','generation_plant_id','year','month',
        'hydro_min_flow_mw','hydro_avg_flow_mw']]
    agg_hydro_cf = agg_hydro_cf.fillna(0.01)

    connect_to_db_and_push_df(df=agg_hydro_cf,
        col_formats="(%s,%s,%s,%s,%s,%s)", table='{PREFIX}hydro_historical_monthly_capacity_factors'.format(PREFIX = PREFIX),
        database='switch_wecc', user=user, password=password, quiet=True)
    print "Successfully uploaded hydro capacity factors!"


def assign_var_cap_factors():
    """
    Variable capacity factors are assigned to all plants with WT and PV
    technology.

    Capacity factors are calculated as the average for all plants from the old
    AMPL dataset for each load zone. These load zone profiles are then assigned
    to all new EIA projects located in that load zone.

    This is completed for only the generation_plants that are members of the new
    scenarios added in this script.

    Note: The capacity factors for residential PV, commercial PV, and
    central (utility-scale) PV are all averaged together for the solar PV capacity
    factor. This assumption may need to be revised later to account for differences
    between residential and utility scale capacity factors.

    Note: The capacity factors for solar in the AMPL data is in UTC (7 hrs ahead).
    This is consistent with the load data, which is also in UTC.
    In the previous verion of this script, there is a correction applied to the data
    by shifting all new capacity factors 7 hours earlier. However, this appears to have been
    already completed for the AMPL data and therefore, this shift is no longer necessary and
    the associated part of the code has been deleted in the 2020 update.

    All these processes take significant time, so it is recommended to run
    this script through a sturdy SSH tunnel (or use a screen and SSH directly into
     db to run the queries).

    """

    user = getpass.getpass('Enter username for the database:')
    password = getpass.getpass('Enter database password for user {}:'.format(user))
    print "\nWill assign variable capacity factors for WIND projects"
    print "(May take significant time)\n"

    # Assign average AMPL wind profile of each load zone to all projects in that zone
    # Technology id 4 is for on-shore wind (5 is for offshore wind but isn't used here)

    for zone in range(1,51):
        print "Load zone {}...".format(zone)
        print '-- Assign average AMPL wind profile for zone {}'.format(zone)
        query = "INSERT INTO {PREFIX}variable_capacity_factors\
                (SELECT generation_plant_id, timepoint_id, timestamp_utc, cap_factor, 1\
                FROM {PREFIX}generation_plant\
                JOIN {PREFIX}generation_plant_scenario_member USING (generation_plant_id)\
                JOIN(\
                SELECT area_id, timepoint_id, timestamp_utc, avg(cap_factor) AS cap_factor, 1\
                FROM {PREFIX}temp_ampl__proposed_projects_v3\
                JOIN {PREFIX}temp_variable_capacity_factors_historical USING (project_id)\
                JOIN {PREFIX}temp_load_scenario_historic_timepoints ON (hour=historic_hour)\
                JOIN {PREFIX}raw_timepoint ON (timepoint_id = raw_timepoint_id)\
                WHERE area_id = {zone} AND technology_id = 4\
                GROUP BY 1,2,3\
                ORDER BY 1,2\
                ) AS factors ON (area_id = load_zone_id)\
                WHERE gen_tech = 'WT' \
                AND generation_plant_scenario_id IN ({gen_scenario_id1},{gen_scenario_id2}));\n\n".format(PREFIX = PREFIX,
                gen_scenario_id1 = new_disaggregated_gen_scenario_id,
                gen_scenario_id2 = new_aggregated_gen_scenario_id,
                zone = zone)
        print query
        connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)
        print "Successfully assigned cap factors to wind projects in load zone {}.".format(zone)

    # Technology id 6 is for residential solar, 25 is for commercial PV, and 26 is for central PV

    print "\nWill assign variable capacity factors for SOLAR PV projects"
    print "(May take significant time)\n"
    for zone in range(1,51):
        print "Load zone {}...".format(zone)
        print '-- Assign variable capacity factors for solar PV projects in zone {}'.format(zone)
        query = "INSERT INTO {PREFIX}variable_capacity_factors\
                (SELECT generation_plant_id, timepoint_id, timestamp_utc, cap_factor, 1\
                FROM {PREFIX}generation_plant\
                JOIN {PREFIX}generation_plant_scenario_member USING (generation_plant_id)\
                JOIN(\
                SELECT area_id, timepoint_id, timestamp_utc, avg(cap_factor) AS cap_factor, 1\
                FROM {PREFIX}temp_ampl__proposed_projects_v3\
                JOIN {PREFIX}temp_variable_capacity_factors_historical USING (project_id)\
                JOIN {PREFIX}temp_load_scenario_historic_timepoints ON (hour=historic_hour)\
                JOIN {PREFIX}raw_timepoint ON (timepoint_id = raw_timepoint_id)\
                WHERE area_id = {zone} AND technology_id IN (6,25,26)\
                GROUP BY 1,2,3\
                ORDER BY 1,2\
                ) AS factors ON (area_id = load_zone_id)\
                WHERE gen_tech = 'PV'\
                AND generation_plant_scenario_id IN ({gen_scenario_id1},{gen_scenario_id2}));\n\n".format(PREFIX = PREFIX,
                gen_scenario_id1 = new_disaggregated_gen_scenario_id,
                gen_scenario_id2 = new_aggregated_gen_scenario_id,
                zone = zone)
        print query
        connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)
        print "Successfully assigned cap factors to solar projects in load zone {}.".format(zone)


def others():
    """
    Miscellaneous processing to finish preparing the EIA dataset for Switch runs.

    Fuell cell technologies are dropped from the dataset, because heat rates
    were mistakenly not calculated (though only amount to 60 MW).

    Other (OT) technologies were assigned a default gas energy source, but were
    not calculated heat rates, so they are assigned the average heat rate for
    gas plants (OT only amounts to around 40 MW).

    Nan hydro capacity factors are replaced by 0.01.

    Nan generation plant parameters are replaced by Nulls.

    Null connection cost parameters are replaced by 0.

    """
    try:
        user = os.environ['SWITCH_USERNAME']
        password = os.environ['SWITCH_PASSWORD']
    except KeyError:
        user = getpass.getpass('Enter username for the database:')
        password = getpass.getpass('Enter database password for user {}:'.format(user))
    # Fuel cells ('FC') were not calculated and assigned heat rates
    # These sum up to about 60 MW of capacity in WECC
    # Cleanest option is to remove them from the current runs:
    query = "INSERT INTO {PREFIX}fuel_cell_generation_plant_backup\
        (SELECT * FROM {PREFIX}generation_plant WHERE gen_tech = 'FC');\
        DELETE FROM {PREFIX}generation_plant_scenario_member gpsm USING {PREFIX}generation_plant gp\
        WHERE gp.generation_plant_id = gpsm.generation_plant_id\
        AND gen_tech = 'FC';\
        DELETE FROM {PREFIX}generation_plant_cost gpc USING {PREFIX}generation_plant gp\
        WHERE gp.generation_plant_id = gpc.generation_plant_id\
        AND gen_tech = 'FC';\
        DELETE FROM {PREFIX}generation_plant_existing_and_planned gpep USING {PREFIX}generation_plant gp\
        WHERE gp.generation_plant_id = gpep.generation_plant_id\
        AND gen_tech = 'FC';\
        DELETE FROM {PREFIX}generation_plant WHERE gen_tech = 'FC';".format(PREFIX = PREFIX)
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    # Others ('OT') also do not have an assigned heat rate. Assign an average.
    query = "UPDATE {PREFIX}generation_plant set full_load_heat_rate = \
        (select avg(full_load_heat_rate)\
        from {PREFIX}generation_plant\
        join {PREFIX}generation_plant_scenario_member using (generation_plant_id)\
        where energy_source = 'Gas'\
        and generation_plant_scenario_id IN ({gen_scenario_id1},{gen_scenario_id2}))\
        where gen_tech = 'OT' and energy_source = 'Gas'".format(PREFIX = PREFIX, gen_scenario_id1 = new_disaggregated_gen_scenario_id, gen_scenario_id2 = new_aggregated_gen_scenario_id )
    connect_to_db_and_run_query(query,
            database='switch_wecc', user=user, password=password, quiet=True)

    # Replace 'NaN's with 'Null's
    # (NaNs result from the aggregation process)
    # Added full_load_hr to this list becauase there are NaNs for renewable sources
    cols_to_replace_nans = ['connect_cost_per_mw','full_load_heat_rate','hydro_efficiency','min_build_capacity',
                            'unit_size','storage_efficiency','store_to_release_ratio',
                            'min_load_fraction','startup_fuel','startup_om',
                            'ccs_capture_efficiency', 'ccs_energy_load']
    for col in cols_to_replace_nans:
        query = "UPDATE {PREFIX}generation_plant SET {c} = Null WHERE {c} = 'NaN'".format(PREFIX = PREFIX, c=col)
        connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)
        print "Replaced NaNs in column '{}'".format(col)

    # Replace Nulls with zeros where Switch expects a number
    query = "UPDATE {PREFIX}generation_plant\
            SET connect_cost_per_mw = 0.0\
            WHERE connect_cost_per_mw is Null".format(PREFIX = PREFIX)
    connect_to_db_and_run_query(query,
                database='switch_wecc', user=user, password=password, quiet=True)


if __name__ == "__main__":
    finish_project_processing(2018)
    upload_generation_projects(2018)
    assign_var_cap_factors()
    others()
    compare_generation_projects_scenario_data_by_energy_source(2.0, 19.0)
    compare_generation_projects_scenario_data_by_energy_source(3.0, 20.0)


def assign_states_to_counties():
    state_dict = {
        'AL': 'Alabama',
        'AK': 'Alaska',
        'AZ': 'Arizona',
        'AR': 'Arkansas',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'HI': 'Hawaii',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'IA': 'Iowa',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'ME': 'Maine',
        'MD': 'Maryland',
        'MA': 'Massachusetts',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MS': 'Mississippi',
        'MO': 'Missouri',
        'MT': 'Montana',
        'NE': 'Nebraska',
        'NV': 'Nevada',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NY': 'New York',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VT': 'Vermont',
        'VA': 'Virginia',
        'WA': 'Washington',
        'WV': 'West Virginia',
        'WI': 'Wisconsin',
        'WY': 'Wyoming'
    }

    query = 'UPDATE {PREFIX}us_counties uc SET state_name = cs.state\
        FROM (SELECT DISTINCT c.name, state, statefp, state_fips, c.gid\
        FROM {PREFIX}us_counties c join {PREFIX}us_states s ON c.statefp=s.state_fips) cs\
        WHERE cs.gid = uc.gid'.format(PREFIX = PREFIX)
    connect_to_db_and_run_query(query, database='switch_wecc', user=user, password=password)


    for state_abr, state_name in state_dict.iteritems():
        query = "UPDATE {PREFIX}us_counties SET state_name = '{state_abr}' WHERE state_name = '{state_name}'".format(
            PREFIX = PREFIX, state_abr = state_abr, state_name = state_name)
        connect_to_db_and_run_query(query, database='switch_wecc', user=user, password=password)
