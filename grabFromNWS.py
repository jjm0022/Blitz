import os
import time
from datetime import datetime
import json

from forecast import Forecast
from processText import Pipeline
from database import DB

import logging
logger = logging.getLogger(__name__)



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




if __name__ == '__main__':
  for office in OFFICES:
    downloadForecast(office, 'Forecast')

