# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 16:01:19 2020

@author: rich
"""

import pandas as pd
import datetime
import os
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sn
import requests
import csv
import ephem

########################### FUNCTIONS

####### DOWNLOAD FILE FROM GOOGLE DRIVE
#taken from this github: https://github.com/nsadawi/Download-Large-File-From-Google-Drive-Using-Python/tree/670dab844d0b539bfcd27eaaf1a6055ddd8ef5ca
#taken from this StackOverflow answer: https://stackoverflow.com/a/39225039
def download_file_from_google_drive(id, destination):
    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = get_confirm_token(response)

    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)

    save_response_content(response, destination)    

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value

    return None

def save_response_content(response, destination):
    CHUNK_SIZE = 32768
    
    #destination = "c:\\tempz\\outputfile.csv"

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)


########################### END FUNCTIONS

# ######### set pandas options to print all columns
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)



start_time = datetime.datetime.now();
print("Script start time:" + str(start_time))
file_path = "C:/tempz/Moon Phase Crime Analysis"
#crime_file = "NYPD_Complaint_Data_Current__Year_To_Date_full.csv"
crime_file = "NYPD_Complaint_Data_Historic.csv"
out_file = 'results_nb.csv'
out_file_final = 'final_results_nb.csv'
source_url = 'https://drive.google.com/file/d/11mrNnQFgLTiu_AEfsTXROdpAtAnac4gk/view?usp=sharing'

 
########## HOUSEKEEPING
# CREATE DIR, REMOVE OLD FILE

print("Performing housekeeping...")

path_exists = os.path.exists(file_path)
if not path_exists:
    os.mkdir(file_path)    

if os.path.exists(file_path + "\\" + out_file):
    os.remove(file_path + "\\" + out_file)
    
if os.path.exists(file_path + "\\" + out_file_final):
    os.remove(file_path + "\\" + out_file_final)    



############### DOWNLOAD SOURCE FILE
print("Downloading source data...")
download_file_from_google_drive('11mrNnQFgLTiu_AEfsTXROdpAtAnac4gk',file_path + "/" + crime_file)  
print("Downloading source data complete. Elapsed time:" + str(datetime.datetime.now() - start_time))
    
    
 ### I need to get rid of records prior to 2010, the dates are all over the map.   


print("Iterating source data by chunk, removing unneeded columns ....")
########## 1. CREATE CSV FILE THAT READS CRIME DATA FILE, KEEPING ONLY NEEDED COLUMNS....AND SAVING NEW, SLIMMER FILE FOR RE USE.
for chunk in pd.read_csv(file_path + "\\" + crime_file,chunksize=1000,nrows=1000,index_col = 0,
                         usecols=['CMPLNT_NUM','CMPLNT_FR_DT']):  
    
    
    for index, row in chunk.iterrows():
        
        rowdt = pd.to_datetime(pd.to_datetime(row['CMPLNT_FR_DT'], errors='coerce'))
        #print(rowdt)
        if rowdt.year < 2013:
              chunk.drop(index,inplace=True)    
         
    
    chunk.to_csv(file_path + "\\" + out_file, mode='a',header=None)   
 
        
print("Iterating source data complete. Working file located in: " +file_path + "\\" + out_file +" Elapsed time:" + str(datetime.datetime.now() - start_time) )    
                                                                                                    
print("Reading working file into memory...")
######### 2. LOAD OUTPUT CSV INTO DF FOR AGGREGATION ANALYSIS
dfComplaints = pd.read_csv(out_file,names=['CMPLNT_NUM','CMPLNT_FR_DT'])
print("Reading working file into memory complete. Elapsed time:" + str(datetime.datetime.now() - start_time))
######### 3. INIT DF TO STORE AGGS
dfCrimeVolumeByDate = dfComplaints[['CMPLNT_FR_DT']].copy()


print("Calculating count of crimes by date...")
for index, row in dfCrimeVolumeByDate.iterrows():
    dfCrimeVolumeByDate.loc[index,'CMPLNT_FR_DT'] = pd.to_datetime(row['CMPLNT_FR_DT'], errors='coerce')
    # print(row['CMPLNT_FR_DT'])    

dfStage = pd.DataFrame(dfCrimeVolumeByDate.groupby(['CMPLNT_FR_DT']).agg({'CMPLNT_FR_DT': ['count']}).reset_index())
dfStage.columns = ['Complaint_Date','Count']

print("Calculating count of crimes by date complete. Elapsed time:" + str(datetime.datetime.now() - start_time))

print("Creating ephem moon/observer object, assigning lat/long to NYC")
moon = ephem.Moon()
nyc = ephem.Observer()
nyc.lat = '40.730610'
nyc.lon = '-73.935242'

print("Calculating moon % for each date....")
for index, row in dfStage.iterrows():      
    
      
      try:
          
          nyc.date = row['Complaint_Date'].strftime("%Y-%m-%d")
          moon.compute(nyc)
          #print(moon.phase)
          #print(moon.earth_distance)
          phase = moon.phase
          #moon = moon_phase(row[0].month,row[0].day,row[0].year)
          #must reference/assign based on index when iterating chunks...
          dfStage.loc[index,'MOON_PHASE']=phase            
          
          
          
      except Exception as e:
           print(e) 

    
print("Calculating moon % for each date complete.Elapsed time:" + str(datetime.datetime.now() - start_time))

print("Calculating correlation coefficient for crime volume by date, moon % on date.")  
cr_cf = dfStage['Count'].corr(dfStage['MOON_PHASE'])
print("###########Calculating correlation coefficient complete. Correllation coefficient is:")
print(cr_cf)


print("Writing final output file showing count of crimes by date, along with moon %.")
dfStage.to_csv(file_path + "\\" + out_file_final)    
print("Writing final output file complete.")
print("Script end time:" + str(datetime.datetime.now() - start_time))




