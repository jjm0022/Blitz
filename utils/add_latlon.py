#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File: add_latlon.py
# Author: JJ Miller
# Date: 2018-09-12
# Last Modified: 2018-09-12

import json
import pandas as pd

with open('NWS_OFFICE_LOCATION.json', 'r') as j:
    info = json.load(j)

def getLat(office):
    return info[office]['Latitude']

def getLon(office):
    return info[office]['Longitude']

new_df = pd.DataFrame()

for year in range(2007,2018):
    df = pd.read_csv('subset_nwsTerms/{0}_subset_extractions.csv'.format(year))
    
    df['longitude'] = df['office'].apply(getLon)
    df['latitude'] = df['office'].apply(getLat)
    new_df = new_df.append(df, ignore_index=True)

new_df.to_csv('presentation_year_extractions.csv', index=False)


