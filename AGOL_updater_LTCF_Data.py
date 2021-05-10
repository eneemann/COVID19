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

# LIVE data layer (LTCF_Data)
ltcf_service = r'https://services6.arcgis.com/KaHXE9OkiB9e63uE/arcgis/rest/services/LTCF_Data/FeatureServer/273'
# LTCF Events by Day
ltcf_events_by_day = r'https://services6.arcgis.com/KaHXE9OkiB9e63uE/arcgis/rest/services/LTCF_Events_by_Day/FeatureServer/0'

# 1) Load CSV data with updates, prep, and clean up the data
# Read in updates from CSV that was exported from Google Sheet (LTCF_Data)
updates = pd.read_csv(os.path.join(work_dir, 'COVID_LTCF_Data_latest.csv'))
updates.sort_values('ID', inplace=True)

# Drop updates columns that aren't needed
# Facility_Type will be dropped, then recreated from 'Dashboard Facility Type')
updates.drop(columns=['Facility_Type', 'Notes'],  inplace=True)

# Reaname updates columns to match service
col_renames = {'ID': 'OID',
               'Dashboard Facility Type': 'Facility_Type',
               'Positive Patients': 'Positive_Patients',
               'Deceased Patients': 'Deceased_Patients',
               'Positive HCWs': 'Positive_HCWs',
               'Positive Patient Description': 'Positive_Patients_Desc',
               'Last Positive Resident': 'LastPos_Resident'}# *** JULIA ADD 1/24 ***

updates.rename(col_renames, axis='columns', inplace=True)

# Strip whitespace from string fields
# updates = updates.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
updates = updates.applymap(lambda x: x.strip() if type(x) == str else x)

# Convert empty spaces to NaNs
updates = updates.applymap(lambda x: np.nan if isinstance(x, str) and not x else x)


# Convert columns to appropriate type for comparisons
int_fields = ['OID', 'UniqueID', 'Positive_Patients', 'Deceased_Patients',
              'Positive_HCWs']
str_fields = ['Positive_Patients_Desc']
dt_fields = ['Notification_Date', 'LastPos_Resident']# *** JULIA EDIT 1/24 ***


# Intermediate step: convert NaNs to 9999 for integers, to 'N' for Resolved_Y_N
updates[int_fields] = updates[int_fields].fillna(9999)
updates['Resolved_Y_N'] = updates['Resolved_Y_N'].fillna('N')

# Cast columns as proper data types
updates[int_fields] = updates[int_fields].astype(int)
updates[str_fields] = updates[str_fields].astype(str)
updates[dt_fields] = updates[dt_fields].astype('datetime64[ns]')



# 2) Load LTCF_Data from feature layer, prep, and clean up the data
keep_fields = ['OID', 'UniqueID', 'Facility_Name', 'Address',
                'City', 'ZIP_Code', 'Facility_Type', 'LHD',
                'Resolved_Y_N', 'Date_Resolved', 'Longitude',
                'Latitude', 'Notification_Date', 'Positive_Patients',
                'Deceased_Patients', 'Positive_HCWs', 'Positive_Patients_Desc', 'LastPos_Resident'] # *** JULIA ADD 1/24 ***

# Reoder columns to updates to match ltcf data
cols_reorder = keep_fields.copy()
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
                    'EditDate', 'Editor', 'SHAPE@XY']
        
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
                  ]
        
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
lastpos_updates = [] # *** JULIA ADD 1/24 ***

#                   0             1                2                3               4
ltcf_fields = ['UniqueID', 'Facility_Name', 'Facility_Type', 'Resolved_Y_N', 'Date_Resolved',
          #        5                    6                  7                     8
          'Positive_Patients', 'Deceased_Patients', 'Positive_HCWs', 'Postive_Patients_Desc', 
          #         9                    10                      11
          'Dashboard_Display', 'Dashboard_Display_Cat', 'LastPos_Resident']
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
        resolved_status = temp_df.iloc[0]['Resolved_Y_N']
        status_check = resolved_status.upper()
        if row[3] != status_check:
            print(f"    {row[0]}:    'Resolved_Y_N' field does not match    {row[3]}   {temp_df.iloc[0]['Resolved_Y_N']}")
            row[3] = status_check
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
                
        # *** JULIA ADD 1/24 ***     
        # Check if last positive resident has changed
        if row[11] != temp_df.iloc[0]['LastPos_Resident']:
            if row[11] is None and str(temp_df.iloc[0]['LastPos_Resident']) == 'NaT':
                pass
            #elif  row[11] < datetime.datetime(2020,1,1,23,59,59,99):
                #row[11] = None 
            else:
                print(f"    {row[0]}:    'LastPos_Resident' field does not match   {row[11]}   {temp_df.iloc[0]['LastPos_Resident']}")
                row[11] = temp_df.iloc[0]['LastPos_Resident']
                ltcf_count += 1; used = True
                lastpos_updates.append(row[0])
        
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
            print(f"    {row[0]}:    Unable to determine 'Postive_Patients_Desc', current value: {row[8]}")

        # Check if the facility needs to be displayed on the dashboard
        if temp_df.iloc[0]['Facility_Type'] in ('Assisted Living', 'Nursing Home', 'Intermed Care/Intel Disabled', 'COVID-unit', 'COVID-only'):
            if (temp_df.iloc[0]['Positive_Patients'] not in (0, 9999) or temp_df.iloc[0]['Positive_HCWs'] not in (0, 9999)) and temp_df.iloc[0]['Resolved_Y_N'] == 'N':
                row[9] = 'Y'
                print(f"    {row[0]}: has positive patients or HCWs, adding to dashboard display")
            else:
                row[9] = 'N'
        else:
            row[9] = 'N'

        # Check if dashboard display category needs to be updated, used for sorting the list of facilities with active outbreaks
        if (temp_df.iloc[0]['Positive_Patients'] in (0, 9999) and temp_df.iloc[0]['Positive_HCWs'] in (0, 9999)):
            row[10] = 9999
        elif temp_df.iloc[0]['Positive_Patients'] >= 21 and temp_df.iloc[0]['Positive_Patients'] < 9999:
            row[10] = 1
        elif temp_df.iloc[0]['Positive_Patients'] >= 11 and temp_df.iloc[0]['Positive_Patients'] <= 20:
            row[10] = 2
        elif temp_df.iloc[0]['Positive_Patients'] >= 5 and temp_df.iloc[0]['Positive_Patients'] <= 10:
            row[10] = 3
        elif temp_df.iloc[0]['Positive_Patients'] >= 1 and temp_df.iloc[0]['Positive_Patients'] < 5:
            row[10] = 4
        elif temp_df.iloc[0]['Positive_Patients'] in (0, 9999) and temp_df.iloc[0]['Positive_HCWs'] not in (0, 9999):
            row[10] = 5
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
        facilities_with_active_cases = more_than_20 + eleven_to_20 + five_to_ten + one_to_four + no_resident_cases
        print('Total facilities with active cases:     ' + str(facilities_with_active_cases))
        print('Total more than 20:    ' + str(more_than_20))
        print('Total 11 to 20:     ' + str(eleven_to_20))
        print('Total 5 to 10:    ' + str(five_to_ten))
        print('Total 1 to 4:    ' + str(one_to_four))
        print('Total No Resident Cases:     ' + str(no_resident_cases) + '\n')
        arcpy.management.SelectLayerByAttribute(ltcf_fc, 'CLEAR_SELECTION')
        return investigations, outbreaks, resolved, positive_patients, deceased_patients, positive_hcws, facilities_with_active_cases, more_than_20, eleven_to_20, five_to_ten, one_to_four, no_resident_cases

total_investigations, total_outbreaks, total_outbreaks_resolved, total_positive_residents, total_deceased_residents, total_positive_HCWs, total_facilities_with_active_cases, count_more_than_20, count_11_to_20, count_5_to_10, count_1_to_4, count_no_resident_cases = find_daily_values(ltcf_service)

# 5) APPEND MOST RECENT VALUES TO THE LTCF EVENTS BY DAY TABLE
insert_fields = ['Date', 'Total_Investigations', 'Total_Outbreaks', 'Total_Outbreaks_Resolved',
                'Total_Positive_Residents', 'Total_Deceased_Residents', 'Total_Positive_HCWs',
                'Today_Facilities_Active_Cases', 'Today_Count_More_than_20', 'Today_Count_11_to_20',
                'Today_Count_5_to_10', 'Today_Count_1_to_4', 'Today_Count_No_Res_Cases', 'SHAPE@XY']
events_by_day_xy = (40, -111)
insert_values = [(dt.datetime.now(), total_investigations, total_outbreaks, total_outbreaks_resolved,
                total_positive_residents, total_deceased_residents, total_positive_HCWs,
                total_facilities_with_active_cases, count_more_than_20, count_11_to_20,
                count_5_to_10, count_1_to_4, count_no_resident_cases, events_by_day_xy)]
with arcpy.da.InsertCursor(ltcf_events_by_day, insert_fields) as cursor:
  for row in insert_values:
      cursor.insertRow(row)
print('Inserted values into LTCF events by day table...')


# 6) CALCULATE DAILY AND CUMULATIVE NUBMERS IN PANDAS DATAFRAME
ltcf_events_by_day_keep_fields = ['Date', 'Total_Investigations', 
                    'Total_Positive_Residents', 'Total_Deceased_Residents', 'Total_Positive_HCWs',
                    'Total_Outbreaks', 'Total_Outbreaks_Resolved',
                    'Today_Facilities_Active_Cases', 
                    'Today_Count_More_than_20', 'Today_Count_11_to_20',
                    'Today_Count_5_to_10', 'Today_Count_1_to_4', 'Today_Count_No_Res_Cases',
                    'Today_Positive_Residents', 'Today_Deceased_Residents', 'Today_Positive_HCWs', 'Today_Outbreaks',
                    'Today_Outbreaks_Resolved']

# Delete in-memory table that will be used (if it already exists)
if arcpy.Exists('in_memory\\temp_table'):
    print("Deleting 'in_memory\\temp_table' ...")
    arcpy.Delete_management('in_memory\\temp_table')
    time.sleep(3)

# Convert counts_by_day into pandas dataframe (table --> numpy array --> dataframe)
arcpy.conversion.TableToTable(ltcf_events_by_day, 'in_memory', 'temp_table')
day_arr = arcpy.da.TableToNumPyArray('in_memory\\temp_table', ltcf_events_by_day_keep_fields, null_value=0)
day_df = pd.DataFrame(data=day_arr)

# Convert string entries of 'None' to zeros ('0')
mask = day_df.applymap(lambda x: x == 'None')
cols = day_df.columns[(mask).any()]
for col in day_df[cols]:
    day_df.loc[mask[col], col] = '0'

# Sort data ascending so most recent dates are at the bottom (highest index)
day_df.head()
day_df.sort_values('Date', inplace=True, ascending=True)
day_df.head().to_string()
day_df['Date'] = pd.to_datetime(day_df['Date']).dt.normalize()

# Load test data during the test process
# Rename variables below back to day_df after done testing
# test_df = pd.read_csv(os.path.join(work_dir, 'by_day_testing.csv'))

# Calculate daily increases using the pandas diff function
# If any daily increases are negative (due to false positives or double-counting),
# set increase to 0
day_df['Today_Positive_Residents'] = day_df['Total_Positive_Residents'].diff().apply(lambda x: 0 if x < 0 else x)
day_df['Today_Deceased_Residents'] = day_df['Total_Deceased_Residents'].diff().apply(lambda x: 0 if x < 0 else x)
day_df['Today_Positive_HCWs'] = day_df['Total_Positive_HCWs'].diff().apply(lambda x: 0 if x < 0 else x)
day_df['Today_Outbreaks'] = day_df['Total_Outbreaks'].diff().apply(lambda x: 0 if x < 0 else x)
day_df['Today_Outbreaks_Resolved'] = day_df['Total_Outbreaks_Resolved'].diff().apply(lambda x: 0 if x < 0 else x)
day_df['Today_Investigations'] = day_df['Total_Investigations'].diff().apply(lambda x: 0 if x < 0 else x)
day_df['Today_Fac_Active_Cases_7_Day_Av'] = day_df['Today_Facilities_Active_Cases'].rolling(window=7).mean()
day_df['Today_Outbreaks_7_Day_Avg'] = day_df['Today_Outbreaks'].rolling(window=7).mean()
day_df['Today_Outbreaks_Res_7_Day_Avg'] = day_df['Today_Outbreaks_Resolved'].rolling(window=7).mean()
day_df['Total_Positive_Res_7_Day_Avg'] = day_df['Total_Positive_Residents'].rolling(window=7).mean()
day_df['Total_Deceased_Res_7_Day_Avg'] = day_df['Total_Deceased_Residents'].rolling(window=7).mean()
day_df['Total_Positive_HCWs_7_Day_Avg'] = day_df['Total_Positive_HCWs'].rolling(window=7).mean()
day_df['Today_Positive_Res_7_Day_Avg'] = day_df['Today_Positive_Residents'].rolling(window=7).mean()
day_df['Today_Deceased_Res_7_Day_Avg'] = day_df['Today_Deceased_Residents'].rolling(window=7).mean()
day_df['Today_Positive_HCWs_7_Day_Avg'] = day_df['Today_Positive_HCWs'].rolling(window=7).mean()
day_df['Fac_More_than_20_7_Day_Avg'] = day_df['Today_Count_More_than_20'].rolling(window=7).mean()
day_df['Fac_11_to_20_7_Day_Avg'] = day_df['Today_Count_11_to_20'].rolling(window=7).mean()
day_df['Fac_5_to_10_7_Day_Avg'] = day_df['Today_Count_5_to_10'].rolling(window=7).mean()
day_df['Fac_1_to_4_7_Day_Avg'] = day_df['Today_Count_1_to_4'].rolling(window=7).mean()
day_df['Fac_No_Res_Cases_7_Day_Avg'] = day_df['Today_Count_No_Res_Cases'].rolling(window=7).mean()

print(day_df)


# 7a) UPDATE ***ONLY TODAY'S ROW*** IN COUNTS BY DAY TABLE WITH NEW NUMBERS
start_time = time.time()
table_count = 0
#                   0           1                           2
table_fields = ['Date', 'Today_Positive_Residents', 'Today_Deceased_Residents', 
                #       3                   4                   5                               6
                'Today_Positive_HCWs', 'Today_Outbreaks', 'Today_Outbreaks_Resolved', 'Today_Investigations',
                #             7                               8                              9
                'Today_Fac_Active_Cases_7_Day_Av','Today_Outbreaks_7_Day_Avg','Today_Outbreaks_Res_7_Day_Avg',
                #               10                                  11                                   12
                'Total_Positive_Res_7_Day_Avg','Total_Deceased_Res_7_Day_Avg','Total_Positive_HCWs_7_Day_Avg',
                #            13                               14                            15
                'Today_Positive_Res_7_Day_Avg','Today_Deceased_Res_7_Day_Avg','Today_Positive_HCWs_7_Day_Avg',
                #               16                      17                          18
                'Fac_More_than_20_7_Day_Avg', 'Fac_11_to_20_7_Day_Avg', 'Fac_5_to_10_7_Day_Avg',
                #           19                      20
                'Fac_1_to_4_7_Day_Avg', 'Fac_No_Res_Cases_7_Day_Avg']

with arcpy.da.UpdateCursor(ltcf_events_by_day, table_fields) as ucursor:
    print("Looping through rows to make updates ...")
    for row in ucursor:
        if dt.datetime.now().date() == row[0].date():
            print(row[0])
            # select row of dataframe where date == date in hosted 'ltcf events by day' table
            d = row[0].strftime('%Y-%m-%d')
            temp_df = day_df.loc[day_df['Date'] == d].reset_index()
            row[1] = temp_df.iloc[0]['Today_Positive_Residents']
            row[2] = temp_df.iloc[0]['Today_Deceased_Residents']
            row[3] = temp_df.iloc[0]['Today_Positive_HCWs']
            row[4] = temp_df.iloc[0]['Today_Outbreaks']
            row[5] = temp_df.iloc[0]['Today_Outbreaks_Resolved']
            row[6] = temp_df.iloc[0]['Today_Investigations']
            row[7] = temp_df.iloc[0]['Today_Fac_Active_Cases_7_Day_Av']
            row[8] = temp_df.iloc[0]['Today_Outbreaks_7_Day_Avg']
            row[9] = temp_df.iloc[0]['Today_Outbreaks_Res_7_Day_Avg']
            row[10] = temp_df.iloc[0]['Total_Positive_Res_7_Day_Avg']
            row[11] = temp_df.iloc[0]['Total_Deceased_Res_7_Day_Avg']
            row[12] = temp_df.iloc[0]['Total_Positive_HCWs_7_Day_Avg']
            row[13] = temp_df.iloc[0]['Today_Positive_Res_7_Day_Avg']
            row[14] = temp_df.iloc[0]['Today_Deceased_Res_7_Day_Avg']
            row[15] = temp_df.iloc[0]['Today_Positive_HCWs_7_Day_Avg']
            row[16] = temp_df.iloc[0]['Fac_More_than_20_7_Day_Avg']
            row[17] = temp_df.iloc[0]['Fac_11_to_20_7_Day_Avg']
            row[18] = temp_df.iloc[0]['Fac_5_to_10_7_Day_Avg']
            row[19] = temp_df.iloc[0]['Fac_1_to_4_7_Day_Avg']
            row[20] = temp_df.iloc[0]['Fac_No_Res_Cases_7_Day_Avg']
            table_count += 1
            ucursor.updateRow(row)
print(f'Total count of LTCF Events By Day Table updates is: {table_count}')

# # 7b) UPDATE ***ALL ROWS*** IN COUNTS BY DAY TABLE WITH NEW NUMBERS
# # Should only need to run this once to make the calculations for all previous rows
# start_time = time.time()
# table_count = 0
# #                   0           1                           2
# table_fields = ['Date', 'Today_Positive_Residents', 'Today_Deceased_Residents', 
#                 #       3                   4                   5                               6
#                 'Today_Positive_HCWs', 'Today_Outbreaks', 'Today_Outbreaks_Resolved', 'Today_Investigations',
#                 #               7                               8                              9
#                 'Today_Fac_Active_Cases_7_Day_Av','Today_Outbreaks_7_Day_Avg','Today_Outbreaks_Res_7_Day_Avg',
#                 #               10                                  11                                   12
#                 'Total_Positive_Res_7_Day_Avg','Total_Deceased_Res_7_Day_Avg','Total_Positive_HCWs_7_Day_Avg',
#                 #            13                               14                            15
#                 'Today_Positive_Res_7_Day_Avg','Today_Deceased_Res_7_Day_Avg','Today_Positive_HCWs_7_Day_Avg',
#                 #               16                      17                          18
#                 'Fac_More_than_20_7_Day_Avg', 'Fac_11_to_20_7_Day_Avg', 'Fac_5_to_10_7_Day_Avg',
#                 #           19                      20
#                 'Fac_1_to_4_7_Day_Avg', 'Fac_No_Res_Cases_7_Day_Avg']

# with arcpy.da.UpdateCursor(ltcf_events_by_day, table_fields) as ucursor:
#     print("Looping through rows to make updates ...")
    
#     for row in ucursor:
#         print(row[0])
#         # select row of dataframe where date == date in hosted 'ltcf events by day' table
#         d = row[0].strftime('%Y-%m-%d')
#         temp_df = day_df.loc[day_df['Date'] == d].reset_index()
#         # row[1] = temp_df.iloc[0]['Today_Positive_Residents']
#         # row[2] = temp_df.iloc[0]['Today_Deceased_Residents']
#         # row[3] = temp_df.iloc[0]['Today_Positive_HCWs']
#         # row[4] = temp_df.iloc[0]['Today_Outbreaks']
#         # row[5] = temp_df.iloc[0]['Today_Outbreaks_Resolved']
#         # row[6] = temp_df.iloc[0]['Today_Investigations']
#         row[7] = temp_df.iloc[0]['Today_Fac_Active_Cases_7_Day_Av']
#         row[8] = temp_df.iloc[0]['Today_Outbreaks_7_Day_Avg']
#         row[9] = temp_df.iloc[0]['Today_Outbreaks_Res_7_Day_Avg']
#         row[10] = temp_df.iloc[0]['Total_Positive_Res_7_Day_Avg']
#         row[11] = temp_df.iloc[0]['Total_Deceased_Res_7_Day_Avg']
#         row[12] = temp_df.iloc[0]['Total_Positive_HCWs_7_Day_Avg']
#         row[13] = temp_df.iloc[0]['Today_Positive_Res_7_Day_Avg']
#         row[14] = temp_df.iloc[0]['Today_Deceased_Res_7_Day_Avg']
#         row[15] = temp_df.iloc[0]['Today_Positive_HCWs_7_Day_Avg']
#         row[16] = temp_df.iloc[0]['Fac_More_than_20_7_Day_Avg']
#         row[17] = temp_df.iloc[0]['Fac_11_to_20_7_Day_Avg']
#         row[18] = temp_df.iloc[0]['Fac_5_to_10_7_Day_Avg']
#         row[19] = temp_df.iloc[0]['Fac_1_to_4_7_Day_Avg']
#         row[20] = temp_df.iloc[0]['Fac_No_Res_Cases_7_Day_Avg']
#         table_count += 1
#         ucursor.updateRow(row)
# print(f'Total count of LTCF Events By Day Table updates is: {table_count}')

# 8) Update the Case Fatality Ratio for LTCFs compared with statewide cases and deaths
# Download the HAI Case Fatality Rates spreadsheet as a Microsoft Excel (.xlsx)
# This spreadsheet as a 'Utah' tab and a 'Resident' tab, so downloading Excel (instead of CSV)
# allows pandas to access both as data frames
print("Begin case fatality rate data update...")
utah_cases = pd.read_excel('Case_Fatality_Rates_latest.xlsx', sheet_name='Utah')
ltcf_cases = pd.read_excel('Case_Fatality_Rates_latest.xlsx', sheet_name='Resident')

# Change the column headers for the resident cases to remove underscores
ltcf_cases.columns = ['date', 'LTCF_Daily_Deaths', 'LTCF_Cumulative_Deaths']
# Remove the first row from the ltcf_cases data frame with a blank data
ltcf_cases.dropna(subset=['date'], inplace=True)
# Remove the 'Grand Total' entry at the end of the ltcf_cases data frame
ltcf_cases = ltcf_cases[ltcf_cases.date != 'Grand Total']
# the existing 'date' column for the ltcf_cases comes in as a string because of the Grand Total
# calculate a new column to join dates in the statewide case data with the ltcf case data
ltcf_cases['join_date'] = ltcf_cases['date'].astype('datetime64')

# The statewide case data has data for every day since the first confirmed case, while the 
# ltcf data only records dates where ltcf residents died.  Join the two data frames so that
# the dates line up.
combined = pd.merge(utah_cases, ltcf_cases, left_on='date', right_on='join_date', how='left')

# Forward fill the ltcf cumulative deaths to account for all of the days not included in the
# ltcf data (days where an ltcf resident didn't die).  Forward fill carries the previous cumulative
# total to the next entry if the next entry is null.
combined['LTCF_Cumulative_Deaths'].fillna(method='ffill', inplace=True)

# Fill in all of the null values for ltcf daily deaths with 0 (days a resident did not die)
combined['LTCF_Daily_Deaths'].fillna(0, inplace=True)

# Get a list of dates from the combined date frame to use for comparison against dates in ArcGIS
dates = combined['date_x']

# Use the datetime isoformat() method to convert date timestamps into a date format like 'yyyy-mm-dd'
iso_dates = []
for d in dates:
    iso_dates.append(d.date().isoformat())

# Loop through all of the dates in the data set to update cases and deaths.  Calculate these for
# all rows because some cases and death data are back filled over time.
cfr_table_count = 0
#                   0           1                           2
cfr_table_fields = ['Date', 'Total_Positive_Residents', 'UT_Cumulative_Cases', 
                #       3                          4                                5               6
                'UT_Cumulative_Deaths', 'Corrected_Res_Cumulative_Deaths', 'LTCF_DeathRatio', 'UT_DeathRatio']
                

with arcpy.da.UpdateCursor(ltcf_events_by_day, cfr_table_fields) as ucursor:
    print("Looping through rows to make updates ...")
    
    for row in ucursor:
        # Run the loop for every day except today because the statewide cases usually aren't updated for
        # the current day
        # if dt.datetime.now().date() != row[0].date():
        if row[0].date().isoformat() in iso_dates:
            print(row[0])
            # select row of dataframe where date == date in hosted 'ltcf events by day' table
            d = row[0].strftime('%Y-%m-%d')
            temp_df = combined.loc[combined['date_x'] == d].reset_index()
            row[2] = temp_df.iloc[0]['Cumulative_cases']
            row[3] = temp_df.iloc[0]['Cumulative_deaths']
            row[4] = temp_df.iloc[0]['LTCF_Cumulative_Deaths']
            row[5] = temp_df.iloc[0]['LTCF_Cumulative_Deaths'] / row[1] * 100
            row[6] = temp_df.iloc[0]['Cumulative_deaths'] / temp_df.iloc[0]['Cumulative_cases'] * 100
            
            cfr_table_count += 1
            ucursor.updateRow(row)
print(f'Total count of LTCF Events By Day Table updates is: {cfr_table_count}')

print("Script shutting down ...")
# Stop timer and print end time in UTC
readable_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
print("The script end time is {}".format(readable_end))
print("Time elapsed: {:.2f}s".format(time.time() - start_time))