import os
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import datetime
from datetime import date, timedelta, datetime

def metelco(df, 
            start_date = None, 
            end_date = None,
            start_hour = '00:00:00',
            end_hour = '07:00:00',
            weekdays = ['Monday','Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']):

    """
    Finds a BTS with maximum user presence per specified time-period

    Optimisation required

    'msisdn' = USER ID // must be adapted to the data !!
    'enodeb_id' = BTS ID // must be adapted to the data !!

    Parameters
    ----------
    df : pandas.DataFrame
    start_date : str in format 'yyyy-mm-dd' // NOT IMPLEMENTED YET
    end_date: str in format 'yyyy-mm-dd' // NOT IMPLEMENTED YET
    start_hour: str in format 'hh-mm-ss'
            to filter the hours we are interested in
    end_hour: str in format 'hh-mm-ss'
            to filter the hours we are interested in
    weekdays: list of days in str format. 
            default = ['Monday','Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    Returns
    -------
    result : pandas.DataFrame
    """

    # Default date and time formats 
    d_format = '%Y-%m-%d'
    h_format = '%H:%M:%S'

    # Create a new datetime collumn from separate date and time coluns 
    df['date'] = (
        df[["event_date","event_time"]]
        .apply(lambda x: '-'.join(x.values.astype(str)), axis="columns")
        .astype(np.datetime64)
        )

    # Group data to hourly observation with count operator on BTS collumn
    df1 = pd.DataFrame(df.groupby(['msisdn', 'enodeb_id', pd.Grouper(key='date', freq='H')]).enodeb_id.count())
    df1['H_occur'] = df1['enodeb_id'] #rename enodeb_id for index reset, other enodeb_id collumn exist in MultiIndex
    df1 = df1.drop('enodeb_id', axis=1) #drop the unneccessary enodeb_id collumn
    df1 = df1.reset_index()

    # Create new day and time columns in grouped dataframe
    df1['day'] = df1['date'].dt.strftime(d_format)
    df1['hour'] = df1['date'].dt.strftime(h_format)

    # Function to extract individual days from date-time range
    def _daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield (start_date + timedelta(n)).strftime(d_format)
    
    #if start_date is None:
    start_date = df1['date'].min()
    #if end_date is None:
    end_date = df1['date'].max()
    
    # This sequence of code create a new DataFrame to filter user based specified day and time range, 
    # with implemented conditions as proposed by Maťo

    # First filter DataFrame for desirable day-range, time-range and weekday
    df_filter = pd.DataFrame() # Create an empty DataFrame for concatination
    for single_date in _daterange(start_date, end_date): # For every day in day range
        if datetime.strptime(str(single_date), d_format).strftime("%A") in weekdays: # Find out if this weekday is desired
            # If yes, filter this day from grouped DataFrame
            temp1 = df1.loc[(df1['day'] == single_date)]
            # And filter desirable time range from grouped DataFrame
            temp1 = temp1.loc[(temp1['hour'] >= start_hour)
                            & (temp1['hour'] <= end_hour)]
            df_filter = pd.concat([df_filter, temp1])


    df_slots_1 =  pd.DataFrame(df_filter.groupby(['msisdn', pd.Grouper(key='date', freq='D')]).H_occur.sum())

    # Chyba implementacia:: for each sim, find the bts with the maximum number of observations for each hour !!!!!
    # Condition 1. by Maťo
    df_slots_1['filter'] = (
        df_slots_1['H_occur']
        .apply(lambda x: '1' if x >= 3 else '0')
        .astype(np.int16)
        )
    # Return records with 3 and more observations
    df_slots_1_filter = df_slots_1.loc[(df_slots_1['filter'] > 0)]

    # Condition 2. by Maťo
    df_slots_2= pd.DataFrame(df_slots_1_filter.groupby('msisdn').H_occur.sum())
    df_slots_2 = df_slots_2.loc[(df_slots_2['H_occur']>9)].reset_index()

    # 
    df_filtered = df_filter.merge(df_slots_2, how='right', left_on='msisdn', right_on='msisdn')

    #Find BTS with max observation for every user
    result =  (pd.DataFrame(
        df_filtered.groupby(['msisdn', 'enodeb_id']).H_occur_x.count()
        )
        .groupby('msisdn')
        .idxmax()
        )
        
    #Postprocessing
    result = result.reset_index()
    result['H_occur_x'] = result['H_occur_x'].astype(str)
    result['enodeb_id'] = (
        result['H_occur_x'].str.split(',').str[1]
        .str.replace(")","")
        .astype(np.int16)
        )
    result = result.drop('H_occur_x', axis = 1)

    return result