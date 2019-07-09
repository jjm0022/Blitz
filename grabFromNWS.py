import os
import time
import requests
from datetime import datetime

from forecast import Forecast
from processText import Pipeline
from forecastDb import DB

import logging
logger = logging.getLogger(__name__)


OFFICES = ["BOX","GSP","EPZ","FWD","BOU","BOI","PAH","FSD","LIX","OAX",
           "PIH","FGZ","RIW","RAH","TAE","OHX","MQT","LBF","FGF","IWX",
           "MRX","JAX","GLD","APX","BGM","JAN","CHS","PHI","PQR","MAF",
           "GYX","FFC","LKN","CRP","GJT","GUM","TSA","DTX","DMX","AMA",
           "HUN","MFL","MFR","BMX","BUF","LSX","AFG","GRR","SHV","DLH",
           "AFC","EWX","GRB","MHX","DDC","PBZ","RNK","BTV","ABQ","OKX",
           "TBW","LZK","PSR","MOB","MPX","DVN","CTP","ILN","AKQ","ILM",
           "REV","EKA","LUB","HFO","HNX","ILX","SEW","GGW","OUN","JKL",
           "ICT","UNR","CLE","PDT","SJT","SJU","VEF","MEG","PUB","BRO",
           "LMK","RLX","KEY","ALY","TWC","LWX","CAR","TOP","MSO","LCH",
           "LOT","BYZ","LOX","CAE","MKX","CYS","SGX","EAX","BIS","SGF",
           "MLB","HGX","MTR","STO","IND","ABR","AJK","SLC","ARX","OTX",
           "GID","TFX"]


def checkNewForecast(forecast):
  '''
  This function will check NWS link for new/updated forecast
  '''
  r = requests.get(forecast.url)
  if r.status_code != 200:
    logger.info("Status Code: {0} for URL: {1}".format(r.status_code, forecast.url))
    return False 
  
  if forecast.parse(r):
    return forecast.isNew()
  else:
    return False 

def downloadForecast(office, table):
  '''
  Main function for grabbing forcast from NWS
  '''
  forecast = Forecast(office, table)
  new_forecast = checkNewForecast(forecast)
  if not new_forecast:
    logger.info("No new forecast for {0}".format(forecast.office))
  else:
    logger.info("Adding new forecast for {0}".format(forecast.office))
    forecast.addForecast()

def processForecast(row):
  '''
  '''
  pipe = Pipeline(row)
  db = DB()
  phrases = pipe.getPhrases()
  table_keys = dict({'uID': 'TEXT', 'Office': 'TEXT', 'TimeStamp': 'TEXT',
                      'Phrase': 'TEXT', 'StartIndex': 'INT', 'EndIndex': 'INT'})
  table = "Phrase"
  db.createTable(table, table_keys)
  for phrase in phrases:
    #logger.info('Adding: {0} to Table: {1}'.format(phrase['Phrase'], table))
    db.insert('Phrase', phrase)
  processed_dict = dict({'uID': pipe.uid, 'Office': pipe.office, 'TimeStamp': pipe.time_string})
  db.insert('Processed', processed_dict)


def process():
  '''
  '''
  db = DB()
  unprocessed = db.getUnprocessed()
  total_processed = 0
  for row in unprocessed:
    logger.info('Processing forecast from {0} at {1}'.format(row['Office'], row['TimeStamp']))
    processForecast(row)
    total_processed += 1
  logger.info('{0} forecasts processed.'.format(total_processed))


if __name__ == '__main__':
  for office in OFFICES:
    downloadForecast(office, 'Forecast')

