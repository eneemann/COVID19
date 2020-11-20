# -*- coding: utf-8 -*-
"""
Created on Thu Jun 25 15:31:43 2020
@author: eneemann
25 Jun 2020: Created initial code to update AGOL layer (EMN).
"""

import os, sys
import time
import getpass
import requests
import random
import arcpy
import pandas as pd
import numpy as np
import datetime as dt

print(f'Current date and time: {dt.datetime.now()}')

# Start timer and print start time in UTC
start_time = time.time()
readable_start = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script start time is {}".format(readable_start))


# Set variables, get AGOL username and password
portal_url = arcpy.GetActivePortalURL()
print(portal_url)

user = getpass.getpass(prompt='    Enter arcgis.com username:\n')
pw = getpass.getpass(prompt='    Enter arcgis.com password:\n')
arcpy.SignInToPortal(portal_url, user, pw)
del pw

###################
# Geocoding Tools #
###################

# Create Geocoder class and function -- from AGRC GitHub
class Geocoder(object):

    _api_key = None
    _url_template = "http://api.mapserv.utah.gov/api/v1/geocode/{}/{}"

    def __init__(self, api_key):
        """
        Create your api key at
        https://developer.mapserv.utah.gov/secure/KeyManagement
        """
        self._api_key = api_key

    def locate(self, street, zone, **kwargs):
        kwargs["apiKey"] = self._api_key
        # r = requests.get(self._url_template.format(street, zone), params=kwargs)
        r = requests.get(self._url_template.format(street, zone), params=kwargs, headers={'referer': 'http://ltcf-covid-updates.com'})

        response = r.json()

        if r.status_code is not 200 or response["status"] is not 200:
            print("{} {} was not found. {}".format(street, zone, response["message"]))
            return None

        result = response["result"]

        print("match: {} score [{}]".format(result["score"], result["matchAddress"]))
        return result["location"]


# Function to send fields to geocoder, get x/y values back
def geocode(row):
    self = 'AGRC-XXXXXXXXXXXXX'     # insert correct API token here (home)
    # result = Geocoder(self).locate(row['Address'], row['ZIP_Code'],
    result = Geocoder(self).locate(row['Address'], row['City'],
                                        **{"acceptScore": 70, "spatialReference": 3857})
    print(result)
#    if result['status'] == '404':
    if result is None:
        row['x'] = '0'
        row['y'] = '0'
        row['status'] = 'failed'
    else:
        row['x'] = result['x']
        row['y'] = result['y']
        row['status'] = 'succeeded'
        
    time.sleep(random.random()*.3)
    return row



###############
# Main Script #
###############

# Updated LTCF_Data is downloaded as CSV from Google Sheet 'https://docs.google.com/spreadsheets/d/1kzowz5CnFqTqzlbuZDec6JgFvLitG20q9C4iKiWpluU/edit#gid=0'
# CSV file with updates should be named 'COVID_LTCF_Data_latest.csv'
# Update this 'work_dir' variable with the folder you store the updated CSV in
work_dir = r'C:\COVID19'

# TEST layer
# ltcf_service = r'https://services1.arcgis.com/99lidPhWCzftIe9K/arcgis/rest/services/EMN_LTCF_Data_TEST/FeatureServer/0'

# LTCF Development layer (version 2)
ltcf_service = r'https://services6.arcgis.com/KaHXE9OkiB9e63uE/arcgis/rest/services/LTCF_Data_Development_V2/FeatureServer/0'



# 1) Load CSV data with updates, prep, and clean up the data
# Read in updates from CSV that was exported from Google Sheet (LTCF_Data)
updates = pd.read_csv(os.path.join(work_dir, 'COVID_LTCF_Data_latest.csv'))
updates.sort_values('ID', inplace=True)

# Drop updates columns that aren't needed
# Facility_Type will be dropped, then recreated from 'Dashboard Facility Type')
# Renamed 'Unnamed: 16' to 'COVID_Unit_Positive_Patients_Onsite'
updates.drop(columns=['Facility_Type', 'Notes'],  inplace=True)

# Reaname updates columns to match service
col_renames = {'ID': 'OID',
               'Dashboard Facility Type': 'Facility_Type',
               'Positive Patients': 'Positive_Patients',
               'Active Positive Patients': 'Active_Positive_Patients', # not in ltcf data originally, but currently part of the AGOL schema
               'Deceased Patients': 'Deceased_Patients',
               'Positive HCWs': 'Positive_HCWs',
               'Positive Patient Description': 'Positive_Patients_Desc'}

updates.rename(col_renames, axis='columns', inplace=True)

# Strip whitespace from string fields
# updates = updates.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
updates = updates.applymap(lambda x: x.strip() if type(x) == str else x)

# Convert empty spaces to NaNs
updates = updates.applymap(lambda x: np.nan if isinstance(x, str) and not x else x)


# Convert columns to appropriate type for comparisons
int_fields = ['OID', 'UniqueID', 'Positive_Patients', 'Deceased_Patients',
              'Positive_HCWs', 'Active_Positive_Patients']
str_fields = ['Positive_Patients_Desc']
dt_fields = ['Notification_Date']


# Intermediate step: convert NaNs to 9999 for integers, to 'N' for Resolved_Y_N
updates[int_fields] = updates[int_fields].fillna(9999)
updates['Resolved_Y_N'] = updates['Resolved_Y_N'].fillna('N')
updates['COVID_Unit_Positive_Patients_Onsite'] = updates['COVID_Unit_Positive_Patients_Onsite'].fillna('N')

# Cast columns as proper data types
updates[int_fields] = updates[int_fields].astype(int)
updates[str_fields] = updates[str_fields].astype(str)
updates[dt_fields] = updates[dt_fields].astype('datetime64[ns]')



# 2) Load LTCF_Data from feature layer, prep, and clean up the data
keep_fields = ['OID', 'UniqueID', 'Facility_Name', 'Address',
                'City', 'ZIP_Code', 'Facility_Type', 'LHD',
                'Resolved_Y_N', 'Date_Resolved', 'Longitude',
                'Latitude', 'Notification_Date', 'Positive_Patients',
                'Deceased_Patients', 'Positive_HCWs', 'Positive_Patients_Desc', 'COVID_Unit_Positive_Patients_Onsite']

# Reoder columns to updates to match ltcf data
cols_reorder = keep_fields.copy()
cols_reorder.append('Active_Positive_Patients')
updates = updates[cols_reorder]

# Delete in-memory table that will be used (if it already exists)
if arcpy.Exists('in_memory\\temp_table'):
    print("Deleting 'in_memory\\temp_table' ...")
    arcpy.Delete_management('in_memory\\temp_table')
    time.sleep(3)

# Convert LTCF_Data feature layer into pandas dataframe (table --> numpy array --> dataframe)
arcpy.conversion.TableToTable(ltcf_service, 'in_memory', 'temp_table')
table_fields = [f.name for f in arcpy.ListFields('in_memory\\temp_table')]

# Replace Nones in UniqueID field with 0s 
count = 0
fields = ['UniqueID']
with arcpy.da.UpdateCursor('in_memory\\temp_table', fields) as ucursor:
    print("Looping through rows in temp_table to make updates ...")
    for row in ucursor:
        if row[0] == None:
            row[0] = 0
            count += 1
        ucursor.updateRow(row)
# print(f'Total count of "None" updates is: {count}')

# Correct spelling typo in LTCF_Data feature layer
arcpy.AlterField_management('in_memory\\temp_table', 'Postive_Patients_Desc', 'Positive_Patients_Desc')

# Convert in-memory table to numpy array and then pandas dataframe
ltcf_arr = arcpy.da.TableToNumPyArray('in_memory\\temp_table', keep_fields)
ltcf_df = pd.DataFrame(data=ltcf_arr)

# Strip whitespace from string fields
# ltcf_df = ltcf_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
ltcf_df = ltcf_df.applymap(lambda x: x.strip() if type(x) == str else x)



# 3) Add new spreadsheet rows to LTCF_Data feature layer
# Subset new rows into separate dataframe and replace 9999s with 0s
current_ids = list(ltcf_df['UniqueID'])
# Finds UniqueIDs not in list of current_ids and greater than the max value in current_ids
updates_geo = updates.loc[(~updates['UniqueID'].isin(current_ids)) & (updates['UniqueID'] > max(current_ids))]
updates_geo = updates_geo.replace(9999, 0)


# Check for need to geocode new rows, either geocode or proceed with change detection
if updates_geo.shape[0] > 0:
    # Send new rows to geocoder
    section_time = time.time()
    updates_geo = updates_geo.apply(geocode, axis=1)
    print(f'Time to geocode new rows: {time.time() - section_time}')
    
    # Filter down to successful and failed results
    good_geo = updates_geo.loc[updates_geo['status'] == 'succeeded']
    bad_geo = updates_geo.loc[updates_geo['status'] == 'failed']
    
    # Print out facilities that failed to geocode
    if not bad_geo.empty:
        print(f'Number of facilities that failed to geocode:  {bad_geo.shape[0]}')
        print('Failed facilities:')
        output = [print(f'    {row[0]}:  {row[1]}, {row[2]}, {row[3]}, {row[4]}') for row in bad_geo[['UniqueID', 'Facility_Name', 'Address', 'City', 'ZIP_Code']].to_numpy()]
    else:
        print(f'\n All facilities ({good_geo.shape[0]}) were successfully geocoded! \n')
    
    # Prompt user to continue or abort
    resp = input("Would you like to continue?    (y/n) \n")
    if resp.lower() == 'n':
        sys.exit(0)
    
    # Append successfully geocoded facilities to LTCF_Data feature layer
    # Get AGOL username
    desc = arcpy.GetPortalDescription()
    username = desc['user']['username']
    
    insert_fields = ['UniqueID', 'Facility_Name', 'Address',
                    'City', 'ZIP_Code', 'Facility_Type', 'LHD',
                    'Resolved_Y_N', 'Date_Resolved', 'Notification_Date', 'Positive_Patients',
                    'Deceased_Patients', 'Positive_HCWs', 'CreationDate', 'Creator',
                    'EditDate', 'Editor', 'SHAPE@XY', 'Active_Positive_Patients', 'COVID_Unit_Positive_Patients_Onsite']
        
    def insert_row(row):
        xy = (row['x'], row['y'])
        values = [row['UniqueID'],
                  row['Facility_Name'],
                  row['Address'],
                  row['City'],
                  row['ZIP_Code'],
                  row['Facility_Type'],
                  row['LHD'],
                  row['Resolved_Y_N'],
                  row['Date_Resolved'],
                  row['Notification_Date'],
                  row['Positive_Patients'],
                  row['Deceased_Patients'],
                  row['Positive_HCWs'],
                  dt.datetime.now(),
                  f'Python Script by {username}',
                  dt.datetime.now(),
                  f'Python Script by {username}',
                  xy,
                  row['Active_Positive_Patients'],
                  row['COVID_Unit_Positive_Patients_Onsite']]
        
        print(f"Adding {row['UniqueID']}:  {row['Facility_Name']} ...")
        with arcpy.da.InsertCursor(ltcf_service, insert_fields) as insert_cursor:
            insert_cursor.insertRow(values)
    
    # Run insert cursor on each row of good_geo dataframe
    good_geo.apply(insert_row, axis=1)

else:
    # Prompt user to continue to change detection
    resp2 = input("\n    No new rows to geocode. Continue to change detection?    (y/n) \n")
    if resp2.lower() == 'n':
        sys.exit(0)


# 4) Check for differences in key field and update their attributes accordingly
# Set up a counter variable and lists for UniqueIDs that were changed
ltcf_count = 0
unique_updates = []
res_updates = []
resdate_updates = []
pospat_updates = []
decpat_updates = []
poshcw_updates = []
actpospat_updates = []
patonsitestatus_updates = []
covidunitpatonsite_updates = []

#                   0             1                2                3               4
ltcf_fields = ['UniqueID', 'Facility_Name', 'Facility_Type', 'Resolved_Y_N', 'Date_Resolved',
          #        5                    6                  7                     8
          'Positive_Patients', 'Deceased_Patients', 'Positive_HCWs', 'Postive_Patients_Desc', 
          #         9                           10                              11                              12                  13
          'Active_Positive_Patients', 'Patient_Onsite_Status', 'COVID_Unit_Positive_Patients_Onsite', 'Dashboard_Display', 'Dashboard_Display_Cat']
cursor_time = time.time()
with arcpy.da.UpdateCursor(ltcf_service, ltcf_fields) as ucursor:
    print("Looping through ltcf rows to make updates ...")
    for row in ucursor:
        if row[0] is None:
            print(f'Found row without UniqueID: {row[1]}, skipping...')
            continue
        used = False
        # select row of updates dataframe where UniqueID == UniqueID in hosted feature layer
        temp_df = updates.loc[updates['UniqueID'] == row[0]]
        
        # Check if resolved status has changed
        if row[3] != temp_df.iloc[0]['Resolved_Y_N']:
            print(f"    {row[0]}:    'Resolved_Y_N' field does not match    {row[3]}   {temp_df.iloc[0]['Resolved_Y_N']}")
            row[3] = temp_df.iloc[0]['Resolved_Y_N']
            ltcf_count += 1; used = True
            res_updates.append(row[0])
        
        # Check if resolved date has changed
        if row[4] != str(temp_df.iloc[0]['Date_Resolved']):
            if row[4] is None and str(temp_df.iloc[0]['Date_Resolved']) == 'nan':
                pass
            else:
                print(f"    {row[0]}:    'Date_Resolved' field does not match   {row[4]}   {temp_df.iloc[0]['Date_Resolved']}")
                row[4] = temp_df.iloc[0]['Date_Resolved']
                ltcf_count += 1; used = True
                resdate_updates.append(row[0])
        
        # Check if positive patients have changed
        if row[5] != temp_df.iloc[0]['Positive_Patients']:
            if row[5] == 0 and temp_df.iloc[0]['Positive_Patients'] == 9999:
                pass
            elif row[5] != 0 and temp_df.iloc[0]['Positive_Patients'] == 9999:
                print(f"    {row[0]}:    'Positive_Patients' field does not match   {row[5]}   {temp_df.iloc[0]['Positive_Patients']}   setting value to 0")
                row[5] = 0
                ltcf_count += 1; used = True
                pospat_updates.append(row[0])
            else:
                print(f"    {row[0]}:    'Positive_Patients' field does not match   {row[5]}   {temp_df.iloc[0]['Positive_Patients']}")
                row[5] = temp_df.iloc[0]['Positive_Patients']
                ltcf_count += 1; used = True
                pospat_updates.append(row[0])
                
        # Check if deceased patients have changed
        if row[6] != temp_df.iloc[0]['Deceased_Patients']:
            if row[6] == 0 and temp_df.iloc[0]['Deceased_Patients'] == 9999:
                pass
            elif row[6] != 0 and temp_df.iloc[0]['Deceased_Patients'] == 9999:
                print(f"    {row[0]}:    'Deceased_Patients' field does not match   {row[6]}   {temp_df.iloc[0]['Deceased_Patients']}   setting value to 0")
                row[6] = 0
                ltcf_count += 1; used = True
                pospat_updates.append(row[0])
            else:
                print(f"    {row[0]}:    'Deceased_Patients' field does not match   {row[6]}   {temp_df.iloc[0]['Deceased_Patients']}")
                row[6] = temp_df.iloc[0]['Deceased_Patients']
                ltcf_count += 1; used = True
                decpat_updates.append(row[0])
        
        # Check if positive HCWs have changed
        if row[7] != temp_df.iloc[0]['Positive_HCWs']:
            if row[7] == 0 and temp_df.iloc[0]['Positive_HCWs'] == 9999:
                pass
            elif row[7] != 0 and temp_df.iloc[0]['Positive_HCWs'] == 9999:
                print(f"    {row[0]}:    'Positive_HCWs' field does not match   {row[7]}   {temp_df.iloc[0]['Positive_HCWs']}   setting value to 0")
                row[7] = 0
                ltcf_count += 1; used = True
                pospat_updates.append(row[0])
            else:
                print(f"    {row[0]}:    'Positive_HCWs' field does not match   {row[7]},   {temp_df.iloc[0]['Positive_HCWs']}")
                row[7] = temp_df.iloc[0]['Positive_HCWs']
                ltcf_count += 1; used = True
                poshcw_updates.append(row[0])
        
        # Check if positive patient description needs updated
        # This updated function removes the 'COVID-only' and 'COVID-unit' facility types and bases category on cumulative resident cases and not just those housed on site
        if (temp_df.iloc[0]['Positive_Patients'] in (0, 9999) and temp_df.iloc[0]['Positive_HCWs'] in (0, 9999)):
            row[8] = 'Zero cases'
        elif temp_df.iloc[0]['Positive_Patients'] >= 21 and temp_df.iloc[0]['Positive_Patients'] < 9999:
            row[8] = 'More than 20'
        elif temp_df.iloc[0]['Positive_Patients'] >= 11 and temp_df.iloc[0]['Positive_Patients'] <= 20:
            row[8] = '11 to 20'
        elif temp_df.iloc[0]['Positive_Patients'] >= 5 and temp_df.iloc[0]['Positive_Patients'] <= 10:
            row[8] = '5 to 10'
        elif temp_df.iloc[0]['Positive_Patients'] >= 1 and temp_df.iloc[0]['Positive_Patients'] < 5:
            row[8] = '1 to 4'
        elif temp_df.iloc[0]['Positive_Patients'] in (0, 9999) and temp_df.iloc[0]['Positive_HCWs'] not in (0, 9999):
            row[8] = 'No Resident Cases'
        else:
            print(f"    {row[0]}:    Unable to determine 'Postive_Patients_Desc', current value: {row[8]}    active positive patients: {temp_df.iloc[0]['Active_Positive_Patients']}")
        
        #### In current development, 'Active_Positive_Patients', 'Patient_Onsite_Status' and 'COVID_Unit_Positive_Patients_Onsite' are not tracked
        # # Check if active positive patients have changed
        # if row[9] != temp_df.iloc[0]['Active_Positive_Patients']:
        #     if row[9] == 0 and temp_df.iloc[0]['Active_Positive_Patients'] == 9999:
        #         pass
        #     elif row[9] != 0 and temp_df.iloc[0]['Active_Positive_Patients'] == 9999:
        #         print(f"    {row[0]}:    'Active_Positive_Patients' field does not match   {row[9]}   {temp_df.iloc[0]['Active_Positive_Patients']}   setting value to 0")
        #         row[9] = 0
        #         ltcf_count += 1; used = True
        #         actpospat_updates.append(row[0])
        #     else:
        #         print(f"    {row[0]}:    'Active_Positive_Patients' field does not match   {row[9]},   {temp_df.iloc[0]['Active_Positive_Patients']}")
        #         row[9] = temp_df.iloc[0]['Active_Positive_Patients']
        #         ltcf_count += 1; used = True
        #         actpospat_updates.append(row[0])

        # # Check if patient onsite status needs to be updated
        # # Facilities with active positive patients onsite
        # if (temp_df.iloc[0]['Active_Positive_Patients'] > 0 and temp_df.iloc[0]['Active_Positive_Patients'] < 9999) or temp_df.iloc[0]['COVID_Unit_Positive_Patients_Onsite'] == 'Y':
        #     row[10] = 'Onsite'
        # # Facilities with positive patients who have been moved offsite
        # elif temp_df.iloc[0]['Active_Positive_Patients'] in (0, 9999) and temp_df.iloc[0]['Positive_Patients'] not in (0, 9999):
        #     print(f"    {row[0]}:    'Patient_Onsite_Status' set to:  Offsite  ")
        #     row[10] = 'Offsite'
        # # Facilities with no positive residents
        # elif temp_df.iloc[0]['Positive_Patients'] in (0, 9999) and temp_df.iloc[0]['Active_Positive_Patients'] in (0, 9999):
        #     row[10] = 'Not Applicable'
        # else:
        #     # If unable to determine, default to onsite
        #     row[10] = 'Onsite'
        #     print(f"    {row[0]}:    Unable to determine 'Patient_Onsite_Status', current value: {row[10]}    active positive patients: {temp_df.iloc[0]['Active_Positive_Patients']}")

        # # Check if COVID-only or COVID-unit facilities have patients onsite
        # if row[11] != temp_df.iloc[0]['COVID_Unit_Positive_Patients_Onsite']:
        #     print(f"    {row[0]}:    'COVID_Unit_Positive_Patients_Onsite' field does not match    {row[11]}   {temp_df.iloc[0]['COVID_Unit_Positive_Patients_Onsite']}")
        #     row[11] = temp_df.iloc[0]['COVID_Unit_Positive_Patients_Onsite']
        #     ltcf_count += 1; used = True
        #     covidunitpatonsite_updates.append(row[0])

        # Check if the facility needs to be displayed on the dashboard
        if temp_df.iloc[0]['Facility_Type'] in ('Assisted Living', 'Nursing Home', 'Intermed Care/Intel Disabled', 'COVID-unit', 'COVID-only'):
            if (temp_df.iloc[0]['Positive_Patients'] not in (0, 9999) or temp_df.iloc[0]['Positive_HCWs'] not in (0, 9999)) and temp_df.iloc[0]['Resolved_Y_N'] == 'N':
                row[12] = 'Y'
                print(f"    {row[0]}: has positive patients or HCWs, adding to dashboard display")
            else:
                row[12] = 'N'
        else:
            row[12] = 'N'

        # Check if dashboard display category needs to be updated, used for sorting the list of facilities with active outbreaks
        if (temp_df.iloc[0]['Positive_Patients'] in (0, 9999) and temp_df.iloc[0]['Positive_HCWs'] in (0, 9999)):
            row[13] = 9999
        elif temp_df.iloc[0]['Positive_Patients'] >= 21 and temp_df.iloc[0]['Positive_Patients'] < 9999:
            row[13] = 1
        elif temp_df.iloc[0]['Positive_Patients'] >= 11 and temp_df.iloc[0]['Positive_Patients'] <= 20:
            row[13] = 2
        elif temp_df.iloc[0]['Positive_Patients'] >= 5 and temp_df.iloc[0]['Positive_Patients'] <= 10:
            row[13] = 3
        elif temp_df.iloc[0]['Positive_Patients'] >= 1 and temp_df.iloc[0]['Positive_Patients'] < 5:
            row[13] = 4
        elif temp_df.iloc[0]['Positive_Patients'] in (0, 9999) and temp_df.iloc[0]['Positive_HCWs'] not in (0, 9999):
            row[13] = 5
        else:
            print(f"    {row[0]}:    Unable to determine 'Dashboard_Display_Cat'")

        ucursor.updateRow(row)
        if used:
            unique_updates.append(row[0])

# Print out information about updates
print("Time elapsed in update cursor: {:.2f}s".format(time.time() - cursor_time))
print(f'Total count of LTCF Data updates is: {ltcf_count}')
print(f'Resolved_Y_N updates: {len(res_updates)}    {res_updates}')
print(f'Date_Resolved updates: {len(resdate_updates)}    {resdate_updates}')
print(f'Positive_Patients updates: {len(pospat_updates)}    {pospat_updates}')
print(f'Deceased_Patients updates: {len(decpat_updates)}    {decpat_updates}')
print(f'Positive_HCWs updates: {len(poshcw_updates)}    {poshcw_updates}')
print(f'Active_Positive_Patients updates: {len(actpospat_updates)}    {actpospat_updates}')
print(f'COVID_Unit_Positive_Patients_Onsite updates: {len(covidunitpatonsite_updates)}        {covidunitpatonsite_updates}')

# Print out dashboard totals based on this update
#               0               1                   2                   3                   4                   5                           6
fields = ['Facility_Type', 'Resolved_Y_N', 'Positive_Patients', 'Deceased_Patients', 'Positive_HCWs', 'Postive_Patients_Desc', 'Dashboard_Display_Cat']
query = '"Facility_Type" IN (\'Nursing Home\', \'Assisted Living\', \'Intermed Care/Intel Disabled\')'
def find_daily_values(ltcf_fc):
    with arcpy.da.SearchCursor(ltcf_fc, fields, query) as cursor:
        facility_types = ['Nursing Home', 'Assisted Living', 'Intermed Care/Intel Disabled']
        investigations = 0
        outbreaks = 0
        positive_patients = 0
        deceased_patients = 0
        positive_hcws = 0
        more_than_20 = 0
        eleven_to_20 = 0
        five_to_ten = 0
        one_to_four = 0
        no_resident_cases = 0
        resolved = 0
        for row in cursor:
            if row[0] in facility_types:
                investigations += 1
            if row[6] != 9999:
                outbreaks += 1
            if row[1] == 'Y' and row[6] != 9999:
                resolved += 1
            positive_patients += row[2]
            deceased_patients += row[3]
            positive_hcws += row[4]
            if row[1] == 'N' and row[5] == 'More than 20':
                more_than_20 += 1
            elif row[1] == 'N' and row[5] == '11 to 20':
                eleven_to_20 += 1
            elif row[1] == 'N' and row[5] == '5 to 10':
                five_to_ten += 1
            elif row[1] == 'N' and row[5] == '1 to 4':
                one_to_four += 1
            elif row[1] == 'N' and row[5] == 'No Resident Cases':
                no_resident_cases += 1
        print('Total investigations:      ' + str(investigations))
        print('Total outbreaks:        ' + str(outbreaks))
        print('Total resolved:        ' + str(resolved))
        print('Total positive patients:      ' + str(positive_patients))
        print('Total deceased patients:      ' + str(deceased_patients))
        print('Total positive HCWs:    ' + str(positive_hcws))
        print('Total facilities with active cases:     ' + str(more_than_20 + eleven_to_20 + five_to_ten + one_to_four + no_resident_cases))
        print('Total more than 20:    ' + str(more_than_20))
        print('Total 11 to 20:     ' + str(eleven_to_20))
        print('Total 5 to 10:    ' + str(five_to_ten))
        print('Total 1 to 4:    ' + str(one_to_four))
        print('Total No Resident Cases:     ' + str(no_resident_cases) + '\n')
        arcpy.management.SelectLayerByAttribute(ltcf_fc, 'CLEAR_SELECTION')

find_daily_values(ltcf_service)
  
print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))