import requests
from bs4 import BeautifulSoup
import sqlite3 as lite
from dateutil.parser import parse
from datetime import datetime
import uuid
from forecastDb import DB


class Forecast(object):

  def __init__(self, office, table):
    '''
    '''
    self.office = office
    self.table = table
    self.db = DB('test_2.db')

    # Make sure the table already exists
    if not self.tableExists(self.table):
      self.createTable(self.table)

    self.url = 'https://forecast.weather.gov/product.php?site={0}&issuedby={0}&product=AFD&format=txt&version=1&glossary=0'.format(self.office)
    self.text = None
    self.current_time_stamp = None
    self.most_recent = self.db.getMostRecent(self.office)
    

  def tableExists(self, table_name):
    '''
    Checks if a table by the name of <table_name> already exists in the DB
    '''
    tables = self.db.listTables()
    for table in tables:
      if table[0] == table_name:
        return True
    return False
  
  def createTable(self, table_name):
    '''
    '''
    self.db.createTable(table_name)


  def parse(self, request):
    '''
    parses the forecast text
    '''
    soup = BeautifulSoup(request.text, features='lxml')
    
    for tag in soup.findAll('pre'):
      self.text = tag.text
    
    self.current_time_stamp = self.getForecastTime(self.text)

    if not self.current_time_stamp:
      print('Could not locate a timestamp for {}'.format(self.office))
      #print(self.text) 
      return False 
    else:
      return True
  
  def getForecastTime(self, forecast_text):
    '''
    Searches the forecast text for the forecast issue time
    '''
    sections  = forecast_text.split('\n\n')
    lines = list()
    for sec in sections:
      for line in sec.split('\n'):
        lines.append(line)

    # Clean up a little
    for ind, line in enumerate(lines):
      if line == '':
        lines.pop(ind)

    # Search for the line with the timestamp
    for line in lines:
      try:
        # Remove the timezone since python doesn't like CST/CDT and others
        line = ' '.join(line.split(' ')[:2] + line.split(' ')[3:])
        timestamp = datetime.strptime(line, '%I%M %p %a %b %d %Y')
        return timestamp
      except ValueError:
        continue
    return None

  def isNew(self):
    '''
    Checks to see if the current forecast is newer than the most recent
    '''
    if not self.most_recent:
      self.most_recent = self.db.getMostRecent(self.office)
    return self.current_time_stamp > parse(self.most_recent['TimeStamp'])
  
  def addForecast(self):
    '''
    Adds the current forecast to the DB
    '''
    self.row_dict = self.getForecastInfo()
    self.db.insertRow(self.row_dict)

  def getForecastInfo(self):
    '''
    Generates a dict that contains information to be inserted into the DB
    '''
    row_dict = {'uid': str(uuid.uuid1()),
                'office': self.office,
                'timestamp':self.current_time_stamp.strftime('%Y%m%dT%H:%M:%S'),
                'year': self.current_time_stamp.year,
                'month': self.current_time_stamp.month,
                'day': self.current_time_stamp.day,
                'forecast': self.text}
    return row_dict
    


    
