import requests
from bs4 import BeautifulSoup
import sqlite3 as lite
from dateutil.parser import parse as dateparse
from datetime import datetime
import uuid
from database import DB
import logging

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

class Parser:

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


class Connection(DB):

  def __init__(self, db_path):
    if db_path:
      self.db = DB(db_path=db_path)
    else:
      self.db = DB()
    self.tables = [row['name'] for row in self.listTables()]

  def createForecastTable(self):
    '''
    '''
    table_keys = dict({'uID': 'TEXT', 'Office': 'TEXT', 'TimeStamp': 'TEXT', 'Year': 'INT',
                        'Month': 'INT', 'Day': 'INT', 'Forecast': 'TEXT'})
    self.db.createTable('Forecast', table_keys)

  def getMostRecent(self):
    '''
    Returns the row-dict for the most recent forcast
    '''
    connection = self.db.connection
    with connection as con:
      cursor = con.cursor()
      cursor = cursor.execute("SELECT * FROM Forecast WHERE Office=? ORDER BY TimeStamp DESC LIMIT 1", (self.office,))
      row = cursor.fetchall()
    try:
      return row[0]
    except:
      return {'TimeStamp': datetime(1900,1,1).strftime('%Y%m%dT%H:%M:%S')}

  def listTables(self):
    '''
    '''
    connection = self.db.connection
    with connection as con:
      cursor = con.cursor()
      cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
      return cursor.fetchall()

  

class Office:
  '''
  '''
  def __init__(self, office):
    self.office = office
    self.url = f'https://forecast.weather.gov/product.php?site={self.office}&issuedby={self.office}&product=AFD&format=txt&version=1&glossary=0'
    self.logger = logging.getLogger(__name__)

  def download(self):
    '''
    This function will check NWS link for new/updated forecast
    '''
    response = requests.get(self.url)
    if not response:
      print(f"{response.status_code} error; Reason: {response.reason}")
      print(f"{response.status_code} error; URL: {self.url}")
      return None
    else: 
      if response.status_code != 200:
        print(f"{response.status_code} error; Reason: {response.reason}")
        return None
      elif self.parse(response):
        print(f"Downloaded forcast from {self.office} valid for {self.current_time_stamp.strftime('%c')}")
        return
      else:
        return None


class Forecast(Office,Connection,Parser):

  def __init__(self, office, db_path=None): 
    '''
    '''
    Office.__init__(self, office)
    Connection.__init__(self, db_path)
    self.createForecastTable()

    self.text = None
    self.current_time_stamp = None
    self.previous = self.getMostRecent()
  
  def isNew(self):
    '''
    Checks to see if the current forecast is newer than the most recent
    '''
    if not self.previous:
      self.previous = self.getMostRecent()
    return self.current_time_stamp > dateparse(self.previous['TimeStamp'])
  
  def add(self, table_name='Forecast'):
    '''
    Adds the current forecast to the DB
    '''
    self.row_dict = self.getRow()
    self.db.insert(table_name, self.row_dict)

  def getRow(self):
    '''
    Generates a dict that contains information to be inserted into the DB
    '''
    row_dict = {'uID': str(uuid.uuid1()),
                'Office': self.office,
                'TimeStamp':self.current_time_stamp.strftime('%Y%m%dT%H:%M:%S'),
                'Year': self.current_time_stamp.year,
                'Month': self.current_time_stamp.month,
                'Day': self.current_time_stamp.day,
                'Forecast': self.text}
    return row_dict
    


    
