import os
import time
import requests
from datetime import datetime
import json

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
  response = requests.get(forecast.url)
  if not response:
    logger.info(f"{response.status_code} error; Reason: {response.reason}")
    logger.info(f"{response.status_code} error; URL: {forecast.url}")
    return False 
  else: 
    if response.status_code != 200:
      logger.info(f"{response.status_code} error; Reason: {response.reason}")
      return False
    elif forecast.parse(response):
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
  processed_dict = dict({'uID': pipe.uid, 
                         'Office': pipe.office, 
                         'TimeStamp': pipe.time_string,
                         'Dataset': 0})

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


def generateDataset():
  '''
  '''
  import collections
  Annotation = collections.namedtuple('Annotation', ['start', 'end', 'label', 'content', 'id', 'phrase'])
  db = DB()
  json_lines = list()

  tot = 0
  # Grab the rows that have not been added to the dataset
  forecasts = db.getNotInDataset()
  for forecast in forecasts:
    if tot >= 10:
      break
    tot += 1
    #logger.info(f"Processing phrases for {forecast['Office']} on {forecast['TimeStamp']}")
    print(f"Processing phrases for {forecast['Office']} on {forecast['TimeStamp']}")
    pipe = Pipeline(forecast)
    rows = db.getProcessedPhrases(forecast['uID'])
    annotations = list()
    annotations_list = list()
    # for each forecast, store it, and the annotations in a single json with 
    for row in rows:
      annotation = Annotation(start=row['StartIndex'],
                              end=row['EndIndex'],
                              label='PlaceHolder',
                              content=forecast['Forecast'],
                              phrase=row['Phrase'],
                              id=row['uID'])

      def _AddAnnotation(annotation):
        for a in annotations:
          if _HasOverlap(annotation, a):
            #logger.info(f'Overlap with {a.phrase} and {annotation.phrase}')
            print(f'Overlap with {a.phrase} and {annotation.phrase}')
            return False
        annotations.append(annotation)
        return True
        
      if _AddAnnotation(annotation):
        annotations_list.append(_AnnotationToJson(annotation))
      
      db.updateDataset(annotation.id)

    print(f'Adding {len(annotations_list)} annotations')
    json_lines.append(_toJSON(annotations_list, pipe))

    with open('nws_phrase_dataset.jonsl', 'a+') as t:
      t.writelines(json_lines)


def _HasOverlap(a1, a2):
  '''
  Check if the 2 annotations overlap.
  '''
  return (a1.start >= a2.start and a1.start < a2.end or
          a1.end > a2.start and a1.end <= a2.end)
   
def _AnnotationToJson(annotation):
  return {
      'text_extraction': {
          'text_segment': {
              'start_offset': annotation.start,
              'end_offset': annotation.end
          }
      },
      'display_name': annotation.label
  }

def _toJSON(annotations_list, pipe):
  '''
  Convert a pure text example into a jsonl string.
  '''
  json_obj = {
      'annotations': annotations_list,
      'text_snippet': {
          'content': pipe.doc.text,
          'forecast_id': pipe.uid
      },
  }
  return json.dumps(json_obj, ensure_ascii=False) + '\n'


if __name__ == '__main__':
  for office in OFFICES:
    downloadForecast(office, 'Forecast')

