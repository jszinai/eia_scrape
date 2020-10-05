 # Copyright 2020. All rights reserved. See AUTHORS.txt
# Licensed under the Apache License, Version 2.0 which is in LICENSE.txt
# Edited in 2020 from original 2017 version by Julia Szinai
"""
Scrape data on existing and planned generators in the United States from the
Energy Information Agency's EIA860 and EIA923 forms (and their older versions).

Enables sequential aggregation of generator data by multiple criteria and
filtering projects of specific NERC Regions.

Extracts monthly capacity factors for each hydroelectric generation plant.

Extracts monthly heat rate factors for each thermal generation plant.

All data is scrapped and parsed from 2004 onwards.

To Do:
Calculate hydro outputs previous to 2004 with nameplate capacities of that year,
but first check that uprating is not significant for hydro plants.


"""

import csv, os, re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from simpledbf import Dbf5
from calendar import monthrange

from utils import download_file, download_metadata_fields, unzip, append_historic_output_to_csv

unzip_directory = 'downloads'
pickle_directory = 'pickle_data'
other_data_directory = 'other_data'
outputs_directory = 'processed_data'
download_log_path = os.path.join(unzip_directory, 'download_log.csv')
REUSE_PRIOR_DOWNLOADS = True
CLEAR_PRIOR_OUTPUTS = False
REWRITE_PICKLES = False
AGGREGATE_COAL = True
start_year, end_year = 2004, 2018
end_month = 'may'
fuel_prime_movers = ['ST','GT','IC','CA','CT','CS','CC']
#ST = Steam turbine,
# GT = Gas turbine (includes jet engine),
# IC = Internal Combustion Engine (diesel, piston, reciprocating)
#CA = Combined Cycle Steam Part, CT = Combined Cycle Combustion Turbine Part,
#CS = Combined Cycle Single Shaft (combustion turbine and steam turbine share single generator)
#CC = Combined Cycle Total Unit (use only for plants/generators that are in
#planning stage, for which specific generator details cannot be provided)
wecc_states = ['WA','OR','CA','AZ','NV','NM','UT','ID','MT','WY','CO','TX']
accepted_status_codes = ['OP','SB','CO','SC','OA','OZ','TS','L','T','U','V']
# from: https://www.seia.org/sites/default/files/EIA-860.pdf
# generator status codes:
# OP = operating, in service, SB = standby/backup, CO = New unit under construction, SC =
# Cold Standby (Reserve); deactivated. OA = Out of service, was not used for some or all
# of the reporting period but was
# either returned to service on December 31 or will be returned to service in the next calendar year,
# OZ = Operated during the ozone season,
# TS = construction complete but not yet in commercial operation,
# L=regulatory approvals pending, not under construction but site prep underway
# T= regulatory approvals received, not under construction but site prep underway,
# U = Under construction, less than or equal to 50 percent complete,
# V = Under construction, more than 50 percent complete
coal_codes = ['ANT','BIT','LIG','SGC','SUB','WC','RC']
# from: https://www.seia.org/sites/default/files/EIA-860.pdf
# coal status codes:
# ANT = Anthracite Coal, BIT = Bituminous Coal, LIG = Lignite Coal, SGC =
# Coal-Derived Synthesis Gas, SUB = Subbituminous Coal, WC = Waste/Other Coal, RC =Recirculating cooling
gen_relevant_data = ['Plant Code', 'Plant Name', 'Status', 'Nameplate Capacity (MW)',
                    'Prime Mover', 'Energy Source', 'Energy Source 2',
                    'Energy Source 3', 'County', 'State', 'Nerc Region',
                    'Operating Year', 'Planned Retirement Year',
                    'Generator Id', 'Unit Code', 'Operational Status']
#Nerc region and operational status are in the "Plant file, the rest are in the "Generator" file
gen_data_to_be_summed = ['Nameplate Capacity (MW)']
gen_aggregation_lists = [
                            ['Plant Code','Unit Code'],
                            ['Plant Code', 'Prime Mover', 'Energy Source',
                            'Operating Year']
                        ]
#multiple generator units (in Generator file) may be at the same plant location and are aggregated by plant code
gen_relevant_data_for_last_year = ['Time From Cold Shutdown To Full Load',
                        'Latitude','Longitude','Balancing Authority Name',
                        'Grid Voltage (kV)', 'Carbon Capture Technology', 'Cogen']
gen_data_to_be_summed_for_last_year = ['Minimum Load (MW)']


def uniformize_names(df):
    df.columns = [str(col).title().replace('_',' ') for col in df.columns]
    df.columns = [str(col).replace('\n',' ').replace(
                    '(Mw)','(MW)').replace('(Kv)','(kV)') for col in df.columns]
    df.rename(columns={
        'Sector':'Sector Number',
        'Carboncapture': 'Carbon Capture Technology',
        'Associated With Combined Heat And Power System':'Cogen',
        'Carbon Capture Technology?':'Carbon Capture Technology',
        'Nameplate':'Nameplate Capacity (MW)',
        'Plant Id':'Plant Code',
        'Reported Prime Mover':'Prime Mover',
        'Reported Fuel Type Code':'Energy Source',
        'Energy Source 1':'Energy Source',
        'Plntname':'Plant Name',
        'Plntcode':'Plant Code',
        'Gencode':'Generator Id',
        'Primemover':'Prime Mover',
        'Current Year':'Operating Year',
        'Utilcode':'Utility Id',
        'Utility ID': 'Utility Id',
        'Nerc':'Nerc Region',
        'Insvyear':'Operating Year',
        'Retireyear':'Planned Retirement Year',
        'Cntyname':'County',
        'Proposed Nameplate':'Nameplate Capacity (MW)',
        'Proposed Status':'Status',
        'Eia Plant Code':'Plant Code',
        'Entity ID' : 'Entity Id',
        'Prime Mover Code':'Prime Mover',
        'Plant State':'State',
        }, inplace=True)
    return df


def main():
    for directory in (unzip_directory, other_data_directory, outputs_directory, pickle_directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    if CLEAR_PRIOR_OUTPUTS:
        for f in os.listdir(outputs_directory):
            os.remove(os.path.join(outputs_directory,f))

    #download and process annual EIA860 data (generator and plant project information)
    zip_file_list = scrape_eia860()
    unzip(zip_file_list)
    eia860_directory_list = [os.path.splitext(f)[0] for f in zip_file_list]
    for eia860_annual_filing in eia860_directory_list:
        parse_eia860_data(eia860_annual_filing)

    #download and process EIA923 data (monthly generation and heat rate information)
    zip_file_list = scrape_eia923()
    unzip(zip_file_list)
    eia923_directory_list = [os.path.splitext(f)[0] for f in zip_file_list]
    for eia923_annual_filing in eia923_directory_list:
       parse_eia923_data(eia923_annual_filing)

    #download and process latest cumulative generator retirement data from monthly EIA860 data and
    # reconcile with latest annual EIA860 (as of end_year)
    zip_file_list = scrape_eia860_monthly()
    unzip(zip_file_list)
    eia860_annual_input_dir_name = 'eia860' + str(end_year)
    eia860_annual_input_dir = os.path.join(unzip_directory,eia860_annual_input_dir_name)
    eia860_monthly_input_dir = unzip_directory
    parse_most_recent_eia860M_data(eia860_annual_input_dir, eia860_monthly_input_dir)

def scrape_eia860():
    """
    Downloads EIA860 forms for each year between start_year and end_year.

    """

    if not os.path.exists(unzip_directory):
        os.makedirs(unzip_directory)
    log_dat = []
    file_list = ['eia860{}.zip'.format(year) for year in range(start_year, end_year+1)]
    for filename in file_list:
        local_path = os.path.join(unzip_directory, filename)
        if REUSE_PRIOR_DOWNLOADS and os.path.isfile(local_path):
            print "Skipping " + filename + " because it was already downloaded."
            continue
        if '2018' in filename: #this needs to be changed to the most recent year data is available if the code is updated later
            base_path = 'http://www.eia.gov/electricity/data/eia860/xls/{}'
        else: #years prior to 2018 have "archive" in the path name
            base_path = 'http://www.eia.gov/electricity/data/eia860/archive/xls/{}'

        url = base_path.format(filename)
        print "Downloading {} from {}".format(local_path, url)
        meta_data = download_file(url, local_path)
        log_dat.append(meta_data)

    # Only write the log file header if we are starting a new log
    write_log_header = not os.path.isfile(download_log_path)
    with open(download_log_path, 'ab') as logfile:
        logwriter = csv.writer(logfile, delimiter='\t',
                               quotechar="'", quoting=csv.QUOTE_MINIMAL)
        if write_log_header:
            logwriter.writerow(download_metadata_fields)
        logwriter.writerows(log_dat)

    return [os.path.join(unzip_directory, f) for f in file_list]

def scrape_eia860_monthly():
    """
    New added function that downloads the most recent preliminary monthly EIA-860M form for the most recent end_year+2 because
    it contains the cumulative list of retired generators. (The most recent monthly EIA-860M form tends to be 2 years more recent
    than the end_year of the annual EIA-860 forms.) The annual EIA-860 retired list only includes those retired generators which
    were reported in the most current data cycle and is not a comprehensive list.  Starting with March 2017 data,
    Preliminary Monthly Electric Generator Inventory table (https://www.eia.gov/electricity/data/eia860m/) includes
    a comprehensive list of generators which retired since 2002. The list can be found on the 'Retired' tab of the data file.
    """
    #File path of most recent monthly 860 form
    #https://www.eia.gov/electricity/data/eia860m/xls/may_generator2020.xlsx

    if not os.path.exists(unzip_directory):
        os.makedirs(unzip_directory)
    log_dat = []
    file_list = ['{}_generator{}.xlsx'.format(end_month, end_year+2)]
    for filename in file_list:
        local_path = os.path.join(unzip_directory, filename)
        if REUSE_PRIOR_DOWNLOADS and os.path.isfile(local_path):
            print "Skipping " + filename + " because it was already downloaded."
            continue
        else:
            base_path = 'https://www.eia.gov/electricity/data/eia860m/xls/{}'

        url = base_path.format(filename)
        print "Downloading {} from {}".format(local_path, url)
        meta_data = download_file(url, local_path)
        log_dat.append(meta_data)

    # Only write the log file header if we are starting a new log
    write_log_header = not os.path.isfile(download_log_path)
    with open(download_log_path, 'ab') as logfile:
        logwriter = csv.writer(logfile, delimiter='\t',
                               quotechar="'", quoting=csv.QUOTE_MINIMAL)
        if write_log_header:
            logwriter.writerow(download_metadata_fields)
        logwriter.writerows(log_dat)

    return [os.path.join(unzip_directory, f) for f in file_list]

def scrape_eia923():
    """
    Downloads EIA923 forms for each year between start_year and end_year.

    """

    if not os.path.exists(unzip_directory):
        os.makedirs(unzip_directory)
    log_dat = []
    file_list = ['f923_{}.zip'.format(year) if year >= 2008
                    else 'f906920_{}.zip'.format(year)
                        for year in range(start_year, end_year+1)]
    for filename in file_list:
        local_path = os.path.join(unzip_directory, filename)
        if REUSE_PRIOR_DOWNLOADS and os.path.isfile(local_path):
            print "Skipping " + filename + " because it was already downloaded."
            continue
        print "Downloading " + local_path
        if '2019' in filename:
            base_path = 'https://www.eia.gov/electricity/data/eia923/xls/{}'
        else: #years prior to 2018 have "archive" in the path name
            base_path = 'https://www.eia.gov/electricity/data/eia923/archive/xls/{}'
        url = base_path.format(filename)
        meta_data = download_file(url, local_path)
        log_dat.append(meta_data)

    # Only write the log file header if we are starting a new log
    write_log_header = not os.path.isfile(download_log_path)
    with open(download_log_path, 'ab') as logfile:
        logwriter = csv.writer(logfile, delimiter='\t',
                               quotechar="'", quoting=csv.QUOTE_MINIMAL)
        if write_log_header:
            logwriter.writerow(download_metadata_fields)
        logwriter.writerows(log_dat)

    return [os.path.join(unzip_directory, f) for f in file_list]


def parse_eia860_data(directory):
    """
    Processes EIA860 Form data.

    First, data for existing and proposed plants and units are merged together.
    Some information is only specified per plant and not unit (i.e. NERC region).

    Proposed units are filtered according to status, as defined in accepted_status_codes.
    For now, all status up to units with regulatory approvals pending are accepted
    as certain. If a unit has not initiated regulatory approval processes, then
    it is filtered out.

    Gas (CT) and steam (CA) turbines of combined cycle plants (CC) are considered indistinct,
    treated as 'CC' technologies.

    Generator data is aggregated according to the lists defined in
    gen_aggregation_lists; mainly by summing up their capacities.
    First, units with the same code belonging to the same plant are aggregated
    together. This is usually the case for gas and steam turbines belonging to the
    same combined cycle (though there are some other cases). Secondly, units are
    aggregated by plant, technology, energy source and vintage. Both aggregations
    reduce the generator set without any loss of precision if no integer unit
    commitment will be performed.

    """

    year = int(directory[-4:])
    print "============================="
    print "Processing data for year {}.".format(year)

    # First, try saving data as pickle if it hasn't been done before
    # Reading pickle files is orders of magnitude faster than reading Excel
    # files directly. This saves tons of time when re-running the script.
    pickle_path_plants = os.path.join(pickle_directory,'eia860_{}_plants.pickle'.format(year))
    pickle_path_existing_generators = os.path.join(pickle_directory,'eia860_{}_existing.pickle'.format(year))
    pickle_path_proposed_generators = os.path.join(pickle_directory,'eia860_{}_proposed.pickle'.format(year))

    if not os.path.exists(pickle_path_plants) \
        or not os.path.exists(pickle_path_existing_generators) \
            or not os.path.exists(pickle_path_proposed_generators) \
                or REWRITE_PICKLES:
        print "Pickle files have to be written for this EIA860 form. Creating..."
        # Different number of blank header rows depending on year
        if year <= 2010:
            rows_to_skip = 0
        else:
            rows_to_skip = 1

        for f in os.listdir(directory):
            path = os.path.join(directory, f)
            f = f.lower()
            # Use a simple for loop, since for years previous to 2008, there are
            # multiple ocurrences of "GenY" in files. Haven't found a clever way
            # to do a pattern search with Glob that excludes unwanted files.
            # In any case, all files have to be read differently, so I'm not
            # sure that the code would become any cleaner by using Glob.

            # From 2009 onwards, look for files with "Plant" and "Generator"
            # in their name.
            # Avoid trying to read a temporal file if any Excel workbook is open
            if 'plant' in f and '~' not in f:
                #different file type (.dbf) from 2003 backwards
                if f.endswith('.dbf'):
                    dataframe = Dbf5(path).to_dataframe()
                else:
                    dataframe = pd.read_excel(path, sheet_name=0, skiprows=rows_to_skip)
                plants = uniformize_names(dataframe)
            if 'generator' in f and '~' not in f:
                dataframe = pd.read_excel(path, sheet_name=0, skiprows=rows_to_skip)
                existing_generators = uniformize_names(dataframe)
                existing_generators['Operational Status'] = 'Operable'
                dataframe = pd.read_excel(path, sheet_name=1, skiprows=rows_to_skip)
                proposed_generators = uniformize_names(dataframe)
                proposed_generators['Operational Status'] = 'Proposed'
            # Different names from 2008 backwards (proposed generators are in separate file rather
            # than different sheet in same file)
            if f.startswith('prgeny'):
                if f.endswith('.dbf'):
                    dataframe = Dbf5(path).to_dataframe()
                else:
                    dataframe = pd.read_excel(path, sheet_name=0, skiprows=rows_to_skip)
                proposed_generators = uniformize_names(dataframe) #is this case sensitive?
                proposed_generators['Operational Status'] = 'Proposed'
            if f.startswith('geny'):
                if f.endswith('.dbf'):
                    dataframe = Dbf5(path).to_dataframe()
                else:
                    dataframe = pd.read_excel(path, sheet_name=0, skiprows=rows_to_skip)
                existing_generators = uniformize_names(dataframe)
                existing_generators['Operational Status'] = 'Operable'

        plants.to_pickle(pickle_path_plants)
        existing_generators.to_pickle(pickle_path_existing_generators)
        proposed_generators.to_pickle(pickle_path_proposed_generators)
    else:
        print "Pickle files exist for this EIA860. Reading..."
        plants = pd.read_pickle(pickle_path_plants)
        existing_generators = pd.read_pickle(pickle_path_existing_generators)
        proposed_generators = pd.read_pickle(pickle_path_proposed_generators)
    #join the existing generator project and existing plant level data, and append list of proposed generators
    generators = pd.merge(existing_generators, plants,
        on=['Utility Id','Plant Code', 'Plant Name','State'],
        suffixes=('_units', ''))
    generators = generators.append(proposed_generators)
    print "Read in data for {} existing and {} proposed generation units in "\
        "the US.".format(len(existing_generators), len(proposed_generators))

    # Filter projects according to status (operable or proposed and far along in regulatory and/or construction process)
    generators = generators.loc[generators['Status'].isin(accepted_status_codes)]
    print "Filtered to {} existing and {} proposed generation units by removing inactive "\
        "and planned projects not yet started.".format(
            len(generators[generators['Operational Status']=='Operable']),
            len(generators[generators['Operational Status']=='Proposed']))

    # Replace chars in numeric columns with null values
    for col in gen_data_to_be_summed:
        generators[col].replace(' ', float('nan'), inplace=True)
        generators[col].replace('.', float('nan'), inplace=True)

    # Manually set Prime Mover of combined cycle plants before aggregation because CA, CT, and CS all
    # describe different components of a combined cycle (CC) plant
    generators.loc[generators['Prime Mover'].isin(['CA','CT','CS']),'Prime Mover'] = 'CC'

    # Aggregate according to user criteria (default setting is to sum nameplate capacity across all generator units and take
    # the maximum of all other parameters, grouping by generator plant)
    # last year of data has some additional columns aggregated
    for agg_list in gen_aggregation_lists:
        # Assign unique values to empty cells in columns that will be aggregated upon
        for col in agg_list:
            if generators[col].dtype == np.float64:
                generators[col].fillna(
                    {i:10000000+i for i in generators.index}, inplace=True)
            else:
                generators[col].fillna(
                    {i:'None'+str(i) for i in generators.index}, inplace=True)
        gb = generators.groupby(agg_list)
        # Some columns will be summed and all others will get the 'max' value
        # Columns are reordered after aggregation for easier inspection
        if year != end_year:
            generators = gb.agg({datum:('max' if datum not in gen_data_to_be_summed else sum)
                            for datum in gen_relevant_data}).loc[:,gen_relevant_data]
        else:
            generators = gb.agg({datum:('max' if datum not in gen_data_to_be_summed else sum)
                            for datum in gen_relevant_data+gen_relevant_data_for_last_year}).loc[
                            :,gen_relevant_data+gen_relevant_data_for_last_year]
        generators.reset_index(drop=True, inplace=True)
        print "Aggregated to {} existing and {} new generation units by aggregating "\
            "through {}.".format(len(generators[generators['Operational Status']=='Operable']),
            len(generators[generators['Operational Status']=='Proposed']), agg_list)

    # Drop columns that are no longer needed (aggegation is across generator units in a plant)
    generators = generators.drop(['Unit Code','Generator Id'], axis=1)
    # Add EIA prefix to be explicit about plant code number origin
    generators = generators.rename(columns={'Plant Code':'EIA Plant Code'})

    fname = 'generation_projects_{}.tab'.format(year)
    with open(os.path.join(outputs_directory, fname),'w') as f:
        generators.to_csv(f, sep='\t', encoding='utf-8', index=False)
    print "Saved data to {} file.\n".format(fname)

def parse_most_recent_eia860M_data(eia860_annual_input_dir, eia860_monthly_input_dir):
    """

    The cumulative list of retired generators (since 2002) is part of the EIA860 Monthly form, which
    is available for more recent months (typically 3 monthls behind the current month) than the annual
    EIA860 form (typically 2 years behind the current year). Therefore, this function compares the
    cumulative list of retired generators from the most recent EIA860 Monthly form
    with the list of propposed and existing generators from most reccent EIA860 annual
    form to make sure that the annual data doesn't include any generators that have since been retired.
    This comparison is done by generator ID, before the data is later aggregated to the plant level,
    because some generator units may still be operational in the same plant where some have been retired.

    Similar to the annual EIA860 parsing function, first, data for existing and proposed plants and
    units are merged together from the annual EIA860 form.

    Proposed units are filtered according to status, as defined in accepted_status_codes.
    For now, all status up to units with regulatory approvals pending are accepted
    as certain. If a unit has not initiated regulatory approval processes, then
    it is filtered out.

    Gas (CT) and steam (CA) turbines of combined cycle plants (CC) are considered indistinct,
    treated as 'CC' technologies.

    Then, the EIA860 Monthly Form data is processesd to get cumulative list of retired generators by
    Generator ID (before aggregating up to Plant level because some generators may have been
    retired in the same plant where some generators remain operational). As with the annual 860 form,
    the Gas and steam turbines of CC plants are similarly treated as CC technologies.

    Then the annual 860 existing and proposed generator list is joined in an inner join with the retired
    monthly 860 form by generator ID.

    The data is filtered for WECC states and any matches in the join are output as a CSV for analysis.

    The data on retired generator units in WECC states is also aggregated up to the plant level and output as a CSV.

    """

    #only run this function for the last year of the data, which is 2018 as of this writing
    year = int(2018)

    if year == end_year:

        print "============================="
        print "Processing data for year {}.".format(year)

        rows_to_skip = 1

        for f in os.listdir(eia860_annual_input_dir):
            path = os.path.join(eia860_annual_input_dir, f)
            f = f.lower()

            # look for files with "Plant" and "Generator" in their name.

            if 'plant' in f and '~' not in f:
                dataframe = pd.read_excel(path, sheet_name=0, skiprows=rows_to_skip)
                plants = uniformize_names(dataframe)
            if 'generator' in f and '~' not in f:
                dataframe = pd.read_excel(path, sheet_name=0, skiprows=rows_to_skip)
                existing_generators = uniformize_names(dataframe)
                try:
                    existing_generators = existing_generators.astype({'Utility Id': 'int64'})
                except ValueError:
                    # The data frame may have an extra information row. If so, drop it.
                    existing_generators.drop(existing_generators.tail(1).index,inplace=True)
                    existing_generators = existing_generators.astype({'Utility Id': 'int64'})
                existing_generators['Operational Status'] = 'Operable'

                dataframe = pd.read_excel(path, sheet_name=1, skiprows=rows_to_skip)
                proposed_generators = uniformize_names(dataframe)
                proposed_generators['Operational Status'] = 'Proposed'
        #join the existing generator and existing plant level data, and append list of proposed generators to dataframe
        generators = pd.merge(existing_generators, plants,
            on=['Utility Id','Plant Code', 'Plant Name','State'],
            suffixes=('_units', ''))
        generators = generators.append(proposed_generators)
        print "Read in data for {} existing and {} proposed generation units in "\
            "the US.".format(len(existing_generators), len(proposed_generators))

        # Filter projects according to status (operable or proposed and far along in regulatory and/or construction process)
        generators = generators.loc[generators['Status'].isin(accepted_status_codes)]
        print "Filtered to {} existing and {} proposed generation units by removing inactive "\
            "and planned projects not yet started.".format(
                len(generators[generators['Operational Status']=='Operable']),
                len(generators[generators['Operational Status']=='Proposed']))

        # Manually set Prime Mover of combined cycle plants before aggregation because CA, CT, and CS all
        # describe different components of a combined cycle (CC) plant
        generators.loc[generators['Prime Mover'].isin(['CA','CT','CS']),'Prime Mover'] = 'CC'

        #reading in list of retired plants from monthly EIA 860 form which is 2 years ahead of annual EIA 860 form
        print "============================="
        print "Processing cumulative retired plant data as of {} {}.".format(end_month, end_year+2)

        for f in os.listdir(eia860_monthly_input_dir):

            path = os.path.join(eia860_monthly_input_dir, f)
            f = f.lower()
            rows_to_skip = 1

            # Look for files with End month and "Generator" in their name. Note that monthly data is 2 years ahead of annual data, hence you need to add 2 below
            if 'generator' in f and str(end_month) in f and str(year+2) in f and f.endswith('xlsx'):

                dataframe = pd.read_excel(path, sheet_name=2, skiprows=rows_to_skip)

                retired_generators = uniformize_names(dataframe)

        # Manually set Prime Mover of combined cycle plants before aggregation because CA, CT, and CS all
        # describe different components of a combined cycle (CC) plant
        retired_generators.loc[retired_generators['Prime Mover'].isin(['CA','CT','CS']),'Prime Mover'] = 'CC'

        #join the existing and proposed generator list from most recent annual 860 list with the most recent monthly 860 retired
        # generator list by generator

        retired_generators_in_project_list = pd.merge(generators[['Cogen','County',
        'Energy Source','Generator Id','Nameplate Capacity (MW)','Nerc Region',
        'Operating Year','Operational Status','Plant Code','Plant Name',
        'Prime Mover','Regulatory Status','State','Technology','Unit Code','Utility Id','Utility Name']],
        retired_generators[['Entity Id','Plant Code','Generator Id','State','Prime Mover','Nameplate Capacity (MW)',
        'Retirement Month','Retirement Year','Operating Year']],
            left_on=['Utility Id','Plant Code','Generator Id','State','Prime Mover','Operating Year','Nameplate Capacity (MW)'],
            right_on = ['Entity Id','Plant Code','Generator Id','State','Prime Mover','Operating Year','Nameplate Capacity (MW)'],
            how = 'inner')

        print "There are {} retired generation units as of {} {} that are still in the most recent {} annual generation project list "\
            "in the US.".format(len(retired_generators_in_project_list), end_month, end_year+2, end_year)

        retired_generators_in_project_list = retired_generators_in_project_list.rename(columns={'Plant Code':'EIA Plant Code'})

        #filtering out just generators in WECC states
        wecc_filter = retired_generators_in_project_list['State'].isin(wecc_states)
        wecc_retired_generators_in_project_list = retired_generators_in_project_list[wecc_filter]

        print "There are {} retired generation units as of {} {} that are still in the most recent {} annual generation project list "\
            "in the WECC states.".format(len(wecc_retired_generators_in_project_list), end_month, end_year+2, end_year)

        #Only keep subset of columns
        wecc_retired_generators_in_project_list_condensed = wecc_retired_generators_in_project_list[['EIA Plant Code', 'Plant Name', 'Nameplate Capacity (MW)', 'Operating Year',
        'Prime Mover', 'Energy Source', 'State','County','Retirement Year','Generator Id', 'Unit Code', 'Regulatory Status']]

        #output to CSV list of retired (or planned retired) WECC generator units still in generator project list
        fname = 'retired_WECC_generation_units_still_in_generator_projects_{}.tab'.format(end_year)
        with open(os.path.join(outputs_directory, fname),'w') as f:
            wecc_retired_generators_in_project_list_condensed.to_csv(f, sep='\t', encoding='utf-8', index=False)
        print "Saved data to {} file.\n".format(fname)

        wecc_retired_generators_in_project_list = wecc_retired_generators_in_project_list.rename(columns={'EIA Plant Code':'Plant Code', 'Operational Status':'Status'})

        gen_relevant_data2 = ['Plant Code', 'Plant Name', 'Nameplate Capacity (MW)', 'Operating Year','Prime Mover', 'Energy Source', 'State','County',
        'Retirement Year','Generator Id', 'Unit Code', 'Regulatory Status']

        # Aggregate retired plants according to user criteria (same as operating plants)
        agg_list = ['Plant Code', 'Prime Mover', 'Energy Source','Operating Year']
        # Assign unique values to empty cells in columns that will be aggregated upon
        for col in agg_list:
            if wecc_retired_generators_in_project_list[col].dtype == np.float64:
                wecc_retired_generators_in_project_list[col].fillna(
                    {i:10000000+i for i in wecc_retired_generators_in_project_list.index}, inplace=True)
            else:
                wecc_retired_generators_in_project_list[col].fillna(
                    {i:'None'+str(i) for i in wecc_retired_generators_in_project_list.index}, inplace=True)
        wecc_retired_gb = wecc_retired_generators_in_project_list.groupby(agg_list)

        # Nameplate capacity will be summed and all others will get the 'max' value
        # Columns are reordered after aggregation for easier inspection
        wecc_retired_agg = wecc_retired_gb.agg({datum:('max' if datum not in gen_data_to_be_summed else sum)
                            for datum in gen_relevant_data2}).loc[:,gen_relevant_data2]
        wecc_retired_agg.reset_index(drop=True, inplace=True)
        print "Aggregated to {} retired generation units by aggregating "\
            "through {}.".format(len(wecc_retired_agg[wecc_retired_agg['Retirement Year']>=2017]), agg_list)

        # Drop columns that are no longer needed
        wecc_retired_agg = wecc_retired_agg.drop(['Unit Code','Generator Id','Energy Source'], axis=1)

        wecc_retired_agg = wecc_retired_agg.rename(columns={'Plant Code':'EIA Plant Code'})

        #export aggregated list of retired plants still in dataset into csv for analyis
        fname = 'retired_WECC_aggregated_generation_projects_{}.tab'.format(year)
        with open(os.path.join(outputs_directory, fname),'w') as f:
            wecc_retired_agg.to_csv(f, sep='\t', encoding='utf-8', index=False)
            print "Saved data to {} file.\n".format(fname)

def parse_eia923_data(directory):
    """
    Processes EIA923 Form data.

    Some fictional plant data ("State-Fuel Level Increment") is filtered out.

    Combined cycle gas and steam turbines are treated indistinctly.

    Monthly energy consumption for generation of electricity ('elec_mmbtu'
    columns) and monthly net generation of electricity ('netgen' columns) are
    aggregated per plant, technology and energy source, to match the level
    of aggregation of the processed EIA860 data.

    Hydro projects are identified by selecting units which use 'WAT' fuel.
    Fuel-based projects are identified by technology, selecting units that
    use any of the fuel_prime_movers.
    ---
    NOTE: (Benjamin) After this part of the project was finished, I noticed that
    I did not include fuel cell technology (code: 'FC') in the fuel_prime_movers
    list, so they are not calculated heat rates. I resolved this by removing those
    units from the database (they summed up to only 63 MW in 2015). If the dataset
    gets updated, the 'FC' code should be considered for heat rate calculation
    and so those units do not have to be removed.
    ---

    EIA923 consumption and generation data is specified per plant and not unit,
    so it is not possible to calculate historic heat rates for each unit in a
    plant, even if they have different vintages. So, previously processed EIA860
    data is aggregated by plant, technology and energy source to obtain plant-level
    nameplate capacities to calculate heat rates.

    Not every plant in the EIA860 form has consumption/generation data specified
    in the EIA923 form, and viceversa. Mismatches are detected and printed to
    csv files, and summaries are printed to the console. For 2015, mismatching
    plants are a tiny fraction of overall capacity/generation. Mismatching plants
    are not outputted in the final files.

    Both hydro capacity factors and fuel consumption/heat rates are outputted
    in WIDE and in NARROW formats. The WIDE format is usually easier for visual
    inspection and spreadsheet exploration, whereas NARROW formats allow and
    easier merging with relational databases.

    Hydro Profiles:
        Electricity consumption is summed (in positive values) to the net
        electricity generation columns, so that the calculated capacity factors
        reflect only generation of electricity. This allows obtaining useful
        capacity factors for pumped hydro plants, which both consume and produce
        electricity.

    Heat rates:
        Usage of different types of coal get aggregated together per plant and
        technology (types are defined in the coal_codes list).
        4 metrics are calculated and reported for each month: net electricity
        generation, capacity factor, heat rate, and fraction of total fuel use
        that each reported fuel represents (each plant-fuel combination is
        outputted as a different row).
        Records with consistent negative heat rates (in all months of the year)
        are removed and reported in a separate file.
        A 'best heat rate' is reported per record, by selecting the second-best
        monthly heat rate of the year. We use the second-best and not best, due
        to several plants having singular outstanding heat rates in some months
        with very small fuel consumption (the error band of measurement and
        reporting becomes relevant).
        Plants that use multiple fuels (generate more than 5% of the time with
        their secondary fuel) are printed to a separate file and a summary is
        printed to console.



    """

    year = int(directory[-4:])
    print "============================="
    print "Processing data for year {}.".format(year)

    # First, try saving data as pickle if it hasn't been done before
    # Reading pickle files is orders of magnitude faster than reading Excel
    # files directly. This saves tons of time when re-running the script.
    pickle_path = os.path.join(pickle_directory,'eia923_{}.pickle'.format(year))
    if not os.path.exists(pickle_path) or REWRITE_PICKLES:
        print "Pickle file has to be written for this EIA923 form. Creating..."
        # Name of the relevant spreadsheet is not consistent throughout years
        # Read largest file in the directory instead of looking by name
        largest_file = max([os.path.join(directory, f)
            for f in os.listdir(directory)], key=os.path.getsize)
        # Different number of blank rows depending on year
        if year >= 2011:
            rows_to_skip = 5
        else:
            rows_to_skip = 7
        generation = uniformize_names(pd.read_excel(largest_file,
            sheet_name='Page 1 Generation and Fuel Data', skiprows=rows_to_skip))
        generation.to_pickle(pickle_path)
    else:
        print "Pickle file exists for this EIA923. Reading..."
        generation = pd.read_pickle(pickle_path)

    generation.loc[:,'Year'] = year
    # Get column order for easier month matching later on
    column_order = list(generation.columns)
    # Remove "State-Fuel Level Increment" fictional plants
    generation = generation.loc[generation['Plant Code']!=99999]
    print ("Read in EIA923 fuel and generation data for {} generation units "
           "and plants in the US.").format(len(generation))

    # Replace characters with proper nan values
    numeric_columns = [col for col in generation.columns if
        re.compile('(?i)elec[_\s]mmbtu').match(col) or re.compile('(?i)netgen').match(col)]
    for col in numeric_columns:
        generation[col].replace(' ', float('nan'), inplace=True)
        generation[col].replace('.', float('nan'), inplace=True)

    # Aggregated generation of plants. First assign CC as prime mover for combined cycles.
    # Flag hydropower generators with WAT as prime mover, and fuel based gneration
    generation.loc[generation['Prime Mover'].isin(['CA','CT','CS']),'Prime Mover']='CC'
    gb = generation.groupby(['Plant Code','Prime Mover','Energy Source'])
    generation = gb.agg({datum:('max' if datum not in numeric_columns else sum)
                                    for datum in generation.columns})
    hydro_generation = generation[generation['Energy Source']=='WAT']
    fuel_based_generation = generation[generation['Prime Mover'].isin(fuel_prime_movers)]
    print ("Aggregated generation data to {} generation plants through Plant "
           "Code, Prime Mover and Energy Source.").format(len(generation))
    print "\tHydro projects:{}".format(len(hydro_generation))
    print "\tFuel based projects:{}".format(len(fuel_based_generation))
    print "\tOther projects:{}\n".format(
        len(generation) - len(fuel_based_generation) - len(hydro_generation))

    # Reload a summary of generation projects for nameplate capacity.
    generation_projects = pd.read_csv(os.path.join(outputs_directory,
        'generation_projects_{}.tab').format(year), sep='\t')
    generation_projects_columns = generation_projects.columns
    print ("Read in processed EIA860 plant data for {} generation units in "
           "the US").format(len(generation_projects))
    print ("---\nGeneration project data processed from the EIA860 form will be "
        "aggregated by Plant, Prime Mover and Energy Source for consistency with EIA923 data (ignoring vintages).\n---")
    gb = generation_projects.groupby(['EIA Plant Code','Prime Mover','Energy Source','Operational Status'])
    generation_projects = gb.agg({datum:('max' if datum not in gen_data_to_be_summed else sum)
                                    for datum in generation_projects.columns})
    hydro_gen_projects = generation_projects[
        (generation_projects['Operational Status']=='Operable') &
        (generation_projects['Energy Source']=='WAT')].rename(
            columns={'EIA Plant Code':'Plant Code'}).reset_index(drop=True)
    fuel_based_gen_projects = generation_projects[
        (generation_projects['Operational Status']=='Operable') &
        (generation_projects['Prime Mover'].isin(fuel_prime_movers))].rename(
            columns={'EIA Plant Code':'Plant Code'}).reset_index(drop=True)
    print "Aggregated plant data into {} records".format(len(generation_projects))
    print "\tHydro projects:{}".format(len(hydro_gen_projects))
    print "\tFuel based projects:{}".format(len(fuel_based_gen_projects))
    print "\tOther projects:{}".format(
        len(generation_projects) - len(fuel_based_gen_projects) - len(hydro_gen_projects))

    # Cross-check data and print console messages with gaps.
    def check_overlap_proj_and_production(projects, production, gen_type, log_path):
        """
        Look for generation projects from EIA860 that don't have production
        data available from form EIA923 and vice versa. Print console messages
        with summaries.
        """
        # Projects with plant data, but no production data
        #projects_missing_production = np.where(projects['Plant Code'].isin(production['Plant Code']), null, projects)

        filter = projects['Plant Code'].isin(production['Plant Code'])
        projects_missing_production = projects[~filter].reset_index(drop=True)
        missing_MW = projects_missing_production['Nameplate Capacity (MW)'].sum()
        total_MW = projects['Nameplate Capacity (MW)'].sum()
        print ("{} of {} {} generation projects in the EIA860 plant form "
               "are not in the EIA923 form, {:.4f}% total {} capacity "
               "({:.0f} of {:.0f} MW)."
              ).format(
                len(projects_missing_production),
                len(projects),
                gen_type,
                100 * (missing_MW / total_MW),
                gen_type,
                missing_MW, total_MW,
              )
        #summary.index.name = None
        summary = projects_missing_production.groupby(
                                            ['Plant Code', 'Plant Name']).sum()
        summary['Net Generation (Megawatthours)'] = float('NaN')
        summary.to_csv(log_path,
            columns=['Nameplate Capacity (MW)', 'Net Generation (Megawatthours)'])

        # Projects with generation data, but no plant data
        filter = production['Plant Code'].isin(projects['Plant Code'])
        production_missing_project = production[~filter].reset_index(drop=True)
        missing_MWh = production_missing_project['Net Generation (Megawatthours)'].sum()
        total_MWh = production['Net Generation (Megawatthours)'].sum()
        print ("{} of {} {} generation projects in the EIA923 generation form "
               "are not in the EIA860 plant form: {:.4f}% "
               "total annual {} production ({:.0f} of {:.0f} MWh)."
              ).format(
                len(production_missing_project), len(production),
                gen_type,
                100 * (missing_MWh / total_MWh),
                gen_type,
                missing_MWh, total_MWh,
              )
        summary = production_missing_project.groupby(
                                            ['Plant Code', 'Plant Name']).sum()
        summary['Nameplate Capacity (MW)'] = float('NaN')
        summary.to_csv(log_path, mode='a', header=False,
            columns=['Nameplate Capacity (MW)', 'Net Generation (Megawatthours)'])
        print ("Summarized {} plants with missing data to {}."
              ).format(gen_type, log_path)


    # Check for projects that have plant data but no generation data, and vice versa
    log_path = os.path.join(outputs_directory,
        'incomplete_data_hydro_{}.csv'.format(year))
    check_overlap_proj_and_production(hydro_gen_projects, hydro_generation,
                                      'hydro', log_path)
    log_path = os.path.join(outputs_directory,
        'incomplete_data_thermal_{}.csv'.format(year))
    check_overlap_proj_and_production(fuel_based_gen_projects, fuel_based_generation,
                                      'thermal', log_path)

    # Recover original column order
    hydro_generation = hydro_generation[column_order]
    fuel_based_generation = fuel_based_generation[column_order]


    #############################
    # Save hydro profiles

    def df_to_long_format(df, col_name, month, index_cols):
        """
        Transforms a DataFrame from 'WIDE' (SHORT) to 'NARROW' (LONG) format.
        """
        return pd.melt(df, index_cols, '{} Month {}'.format(col_name, month)
            ).drop('variable',axis=1).rename(columns={'value':col_name})

    ###############
    # WIDE format
    #getting both net generation and electric generation "consumed" to calculate gross hydropower generation
    #calculating the monthly capacity factor for hydropower = monthly generation (MWh)/(hours in month * MW capacity)
    hydro_outputs=pd.concat([
        hydro_generation[['Year','Plant Code','Plant Name','Prime Mover']],
        hydro_generation.filter(regex=r'(?i)netgen'),
        hydro_generation.filter(regex=r'(?i)elec quantity')
        ], axis=1).reset_index(drop=True)
    hydro_outputs=pd.merge(hydro_outputs, hydro_gen_projects[['Plant Code',
        'Prime Mover', 'Nameplate Capacity (MW)', 'County', 'State']],
        on=['Plant Code','Prime Mover'], suffixes=('','')).reset_index(drop=True)
    for month in range(1,13):
        hydro_outputs.rename(
            columns={hydro_outputs.columns[3+month]:
                'Net Electricity Generation (MWh) Month {}'.format(month)},
            inplace=True)
        hydro_outputs.rename(
            columns={hydro_outputs.columns[15+month]:
                'Electricity Consumed (MWh) Month {}'.format(month)},
            inplace=True)
        hydro_outputs.loc[:,'Net Electricity Generation (MWh) Month {}'.format(month)] += \
            hydro_outputs.loc[:,'Electricity Consumed (MWh) Month {}'.format(month)].replace(to_replace='.', value=0)
        hydro_outputs.loc[:,'Capacity Factor Month {}'.format(month)] = \
            hydro_outputs.loc[:,'Net Electricity Generation (MWh) Month {}'.format(month)].replace(to_replace='.', value=0).div(
            monthrange(int(year),month)[1]*24*hydro_outputs['Nameplate Capacity (MW)'])

    append_historic_output_to_csv(
        os.path.join(outputs_directory,'historic_hydro_capacity_factors_WIDE.tab'), hydro_outputs)
    print "\nSaved hydro capacity factor data in wide format for {}.".format(year)

    ###############
    # NARROW format
    index_columns = [
            'Year',
            'Plant Code',
            'Plant Name',
            'Prime Mover',
            'Nameplate Capacity (MW)',
            'State',
            'County']
    hydro_outputs_narrow = pd.DataFrame(columns=['Month'])
    for month in range(1,13):
        hydro_outputs_narrow = pd.concat([
            hydro_outputs_narrow,
            pd.merge(
                df_to_long_format(hydro_outputs,
                    'Capacity Factor', month, index_columns),
                df_to_long_format(hydro_outputs,
                    'Net Electricity Generation (MWh)', month, index_columns),
                on=index_columns)
            ], axis=0)
        hydro_outputs_narrow.loc[:,'Month'].fillna(month, inplace=True)

    # Get friendlier output
    hydro_outputs_narrow = hydro_outputs_narrow[['Month', 'Year',
            'Plant Code', 'Plant Name', 'State','County','Prime Mover',
            'Nameplate Capacity (MW)', 'Capacity Factor', 'Net Electricity Generation (MWh)']]
    hydro_outputs_narrow = hydro_outputs_narrow.astype(
            {c: int for c in ['Month', 'Year', 'Plant Code']})

    append_historic_output_to_csv(
        os.path.join(outputs_directory,'historic_hydro_capacity_factors_NARROW.tab'), hydro_outputs_narrow)
    print "Saved {} hydro capacity factor records in narrow format for {}.\n".format(
        len(hydro_outputs_narrow), year)

    #############################
    # Save heat rate profiles

    ###############
    # WIDE format
    heat_rate_outputs=pd.concat([
        fuel_based_generation[
            ['Plant Code','Plant Name','Prime Mover','Energy Source','Year']],
            fuel_based_generation.filter(regex=r'(?i)elec[_\s]mmbtu'),
            fuel_based_generation.filter(regex=r'(?i)netgen')
        ], axis=1).reset_index(drop=True)

    # Aggregate consumption/generation of/by different types of coal in a same plant
    if AGGREGATE_COAL:
        fuel_based_gen_projects.loc[:,'Energy Source'].replace(
            to_replace=coal_codes, value='COAL', inplace=True)
        heat_rate_outputs_columns = list(heat_rate_outputs.columns)
        heat_rate_outputs.loc[:,'Energy Source'].replace(
            to_replace=coal_codes, value='COAL', inplace=True)
        gb = heat_rate_outputs.groupby(
            ['Plant Code','Prime Mover','Energy Source'])
        heat_rate_outputs = gb.agg(
            {col:('max' if col in ['Plant Code','Plant Name','Prime Mover',
                                    'Energy Source','Year']
                else sum) for col in heat_rate_outputs_columns}).reset_index(drop=True)
        heat_rate_outputs = heat_rate_outputs[heat_rate_outputs_columns]
        print "Aggregated coal power plant consumption.\n"

    # Merge with project data
    heat_rate_outputs = pd.merge(heat_rate_outputs,
        fuel_based_gen_projects[['Plant Code','Prime Mover','Energy Source',
        'Energy Source 2', 'Energy Source 3', 'State','County','Nameplate Capacity (MW)']],
        on=['Plant Code','Prime Mover','Energy Source'], suffixes=('',''))

    # Get total fuel consumption per plant and prime mover
    total_fuel_consumption = pd.concat([
            fuel_based_generation[['Plant Code','Prime Mover']],
            fuel_based_generation.filter(regex=r'(?i)elec[_\s]mmbtu')
            ], axis=1).reset_index(drop=True)
    total_fuel_consumption.rename(columns={
        total_fuel_consumption.columns[1+m]:
        'Fraction of Total Fuel Consumption Month {}'.format(m)
            for m in range(1,13)}, inplace=True)
    total_fuel_consumption_columns = list(total_fuel_consumption.columns)
    gb = total_fuel_consumption.groupby(['Plant Code','Prime Mover'])
    total_fuel_consumption = gb.agg({col:('max' if col in ['Plant Code','Prime Mover'] else sum)
                                for col in total_fuel_consumption_columns}).reset_index(drop=True)
    total_fuel_consumption = total_fuel_consumption[total_fuel_consumption_columns]
    heat_rate_outputs = pd.merge(heat_rate_outputs, total_fuel_consumption,
            on=['Plant Code','Prime Mover'], suffixes=('',''))

    # Calculate fraction total use of each fuel in the year
    heat_rate_outputs.loc[:,'Fraction of Yearly Fuel Use'] = \
        heat_rate_outputs.filter(regex=r'(?i)elec[_\s]mmbtu').sum(axis=1).div(
        heat_rate_outputs.filter(regex=r'Fraction of Total').sum(axis=1))
    # To Do: Use regex filtering for this in case number of columns changes
    for month in range(1,13):
        heat_rate_outputs.rename(
            columns={heat_rate_outputs.columns[4+month]:
                'Heat Rate Month {}'.format(month)},
            inplace=True)
        heat_rate_outputs.rename(
            columns={heat_rate_outputs.columns[16+month]:
                'Net Electricity Generation (MWh) Month {}'.format(month)},
            inplace=True)
        # Calculate fraction of total fuel use
        heat_rate_outputs.loc[:,'Fraction of Total Fuel Consumption Month {}'.format(month)] = \
            heat_rate_outputs.loc[:,'Heat Rate Month {}'.format(month)].div(
            heat_rate_outputs.loc[:,'Fraction of Total Fuel Consumption Month {}'.format(month)])
        # Monthly heat rates
        heat_rate_outputs.loc[:,'Heat Rate Month {}'.format(month)] = \
            heat_rate_outputs.loc[:,'Heat Rate Month {}'.format(month)].div(
                heat_rate_outputs.loc[:,'Net Electricity Generation (MWh) Month {}'.format(month)])
        # Monthly capacity factors
        heat_rate_outputs['Capacity Factor Month {}'.format(month)] = \
            heat_rate_outputs.loc[:,'Net Electricity Generation (MWh) Month {}'.format(month)].div(
                monthrange(int(year),month)[1]*24*heat_rate_outputs['Nameplate Capacity (MW)'])

    # Filter records of consistently negative heat rates throughout the year
    negative_filter = (heat_rate_outputs <= 0).filter(regex=r'Heat Rate').all(axis=1)
    negative_heat_rate_outputs = heat_rate_outputs[negative_filter]
    append_historic_output_to_csv(
        os.path.join(outputs_directory,'negative_heat_rate_outputs.tab'), negative_heat_rate_outputs)
    heat_rate_outputs = heat_rate_outputs[~negative_filter]
    print ("Removed {} records of consistently negative heat rates and saved"
        " them to negative_heat_rate_outputs.tab".format(
        len(negative_heat_rate_outputs)))

    # Get the second best heat rate in a separate column (best heat rate may be too good to be true or data error)
    heat_rate_outputs.loc[:,'Best Heat Rate'] = pd.DataFrame(
        np.sort(heat_rate_outputs.replace([0,float('inf')],float('nan'))[
            heat_rate_outputs>0].filter(regex=r'Heat Rate'))).iloc[:,1]

    append_historic_output_to_csv(
        os.path.join(outputs_directory,'historic_heat_rates_WIDE.tab'), heat_rate_outputs)
    print "\nSaved heat rate data in wide format for {}.".format(year)

    ###############
    # NARROW format
    index_columns = [
            'Year',
            'Plant Code',
            'Plant Name',
            'Prime Mover',
            'Energy Source',
            'Energy Source 2',
            'Energy Source 3',
            'Nameplate Capacity (MW)',
            'State',
            'County'
        ]
    heat_rate_outputs_narrow = pd.DataFrame(columns=['Month'])
    for month in range(1,13):
        # To Do: Collapse the mergers into a more compact function
        heat_rate_outputs_narrow = pd.concat([
            heat_rate_outputs_narrow,
            pd.merge(
                pd.merge(
                    pd.merge(
                        df_to_long_format(heat_rate_outputs,
                            'Heat Rate', month, index_columns),
                        df_to_long_format(heat_rate_outputs,
                            'Capacity Factor', month, index_columns),
                    on=index_columns),
                    df_to_long_format(heat_rate_outputs,
                        'Net Electricity Generation (MWh)', month, index_columns),
                    on=index_columns),
                df_to_long_format(heat_rate_outputs,
                    'Fraction of Total Fuel Consumption', month, index_columns),
                on=index_columns)
            ], axis=0)
        heat_rate_outputs_narrow.loc[:,'Month'].fillna(month, inplace=True)

    # Get friendlier output
    heat_rate_outputs_narrow = heat_rate_outputs_narrow[['Month', 'Year',
            'Plant Code', 'Plant Name', 'State', 'County', 'Prime Mover',
            'Energy Source', 'Energy Source 2', 'Energy Source 3',
            'Nameplate Capacity (MW)', 'Heat Rate', 'Capacity Factor',
            'Fraction of Total Fuel Consumption', 'Net Electricity Generation (MWh)']]
    heat_rate_outputs_narrow = heat_rate_outputs_narrow.astype(
            {c: int for c in ['Month', 'Year', 'Plant Code']})

    append_historic_output_to_csv(
        os.path.join(outputs_directory,'historic_heat_rates_NARROW.tab'),
        heat_rate_outputs_narrow)
    print "Saved {} heat rate records in narrow format for {}.".format(
        len(heat_rate_outputs_narrow), year)

    # Save plants that present multiple fuels in separate file
    multi_fuel_heat_rate_outputs = heat_rate_outputs[
        (heat_rate_outputs['Fraction of Yearly Fuel Use'] >= 0.05) &
        (heat_rate_outputs['Fraction of Yearly Fuel Use'] <= 0.95)]
    # Don't identify as multi-fuel plants that use different fuels in different units
    indices_to_drop = []
    for row in multi_fuel_heat_rate_outputs.iterrows():
        try:
            if len(fuel_based_gen_projects.loc[row[1]['Plant Code'],row[1]['Prime Mover']]) > 1:
                indices_to_drop.append(int(row[0]))
        except KeyError:
            # Plant Code and Prime Mover combo don't exist, so no need to drop an index
            pass
    multi_fuel_heat_rate_outputs = multi_fuel_heat_rate_outputs.drop(indices_to_drop)

    append_historic_output_to_csv(
        os.path.join(outputs_directory,'multi_fuel_heat_rates.tab'),
        multi_fuel_heat_rate_outputs)
    print ("\n{} records show use of multiple fuels (more than 5% of the secondary fuel in the year). "
            "Saved them to multi_fuel_heat_rates.tab".format(len(multi_fuel_heat_rate_outputs)))
    print "{} correspond to plants located in WECC states and totalize {} MW of capacity".format(
        len(multi_fuel_heat_rate_outputs[multi_fuel_heat_rate_outputs['State'].isin(wecc_states)]),
        multi_fuel_heat_rate_outputs[multi_fuel_heat_rate_outputs['State'].isin(wecc_states)]['Nameplate Capacity (MW)'].sum())

    for i in [0.05,0.1,0.15]:
        multi_fuel_heat_rate_outputs = multi_fuel_heat_rate_outputs[
            (multi_fuel_heat_rate_outputs['Fraction of Yearly Fuel Use'] >= 0.05+i) &
            (multi_fuel_heat_rate_outputs['Fraction of Yearly Fuel Use'] <= 0.95-i)]

        print "{} records show use of more than {}% of the secondary fuel in the year".format(len(multi_fuel_heat_rate_outputs),(i+0.05)*100)
        print "{} correspond to plants located in WECC states and totalize {} MW of capacity".format(
            len(multi_fuel_heat_rate_outputs[multi_fuel_heat_rate_outputs['State'].isin(wecc_states)]),
            multi_fuel_heat_rate_outputs[multi_fuel_heat_rate_outputs['State'].isin(wecc_states)]['Nameplate Capacity (MW)'].sum())


# Generator costs from schedule 5 are hidden for individual generators,
# but published in aggregated form. 2015 data is expected to be available
# in Feb 2017. Data only goes back to 2013; I don't know how to get good
# estimates of costs of older generators.
# http://www.eia.gov/electricity/generatorcosts/


if __name__ == "__main__":
    main()