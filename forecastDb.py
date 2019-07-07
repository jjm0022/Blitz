import sqlite3 as lite
import re
from datetime import datetime

class DB(object):

  def __init__(self, db_path='forecasts.db'):
    '''
    '''
    self.db_path = db_path
    self.connection = lite.connect(self.db_path)
    self.connection.row_factory = lite.Row
    self.processed_dict = dict({'uID': 'TEXT', 'Office': 'TEXT', 'TimeStamp':'Text'})
    self.createTable('Processed', self.processed_dict)

  def execute(self, command, values=None):
    '''
    Execute the provided command
    '''
    if not values:
      with self.connection:
          cur = self.connection.cursor()
          cur.execute(command)
    else:
      with self.connection:
          cur = self.connection.cursor()
          cur.execute(command, values)

  def createTable(self, table_name, columns):
    '''
    '''
    cmd = 'CREATE TABLE IF NOT EXISTS {0} ('.format(table_name)
    for key, value in columns.items():
      cmd = cmd + '{0} {1}, '.format(key, value)
    cmd = re.sub(r'\, $', ')', cmd)
    self.execute(cmd)

  
  def insert(self, table, row_dict):
    '''
    '''
    cmd = 'INSERT INTO {0} VALUES ('.format(table)
    for key in row_dict:
      cmd = cmd + '?, '
    cmd = re.sub(r'\, $', ')', cmd)
    values = tuple(row_dict.values())
    self.execute(cmd, values) 


  def getUnprocessed(self):
    '''
    '''

    with self.connection as c:
      cur = c.cursor()
      query = cur.execute('''SELECT * FROM Forecast f WHERE NOT
                             EXISTS (SELECT 1 FROM Processed WHERE uID = f.uID)
                             ORDER BY Office, TimeStamp;''')
      rows = query.fetchall()
    for row in rows:
      yield row


  def createFORE(self):
    '''
    '''
    self.execute(
      '''CREATE TABLE IF NOT EXISTS Forecast
         (uID TEXT, Office TEXT, TimeStamp TEXT, Year INT, Month INT, Day INT,
          Forecast TEXT)''')


  def createPOS(self):
    '''
    '''
    self.execute(
      '''CREATE TABLE IF NOT EXISTS POS
         (uID TEXT, Office TEXT, TimeStamp TEXT,
          Token TEXT, POS TEXT, Lemma TEXT)''')


  def createENT(self):
    '''
    '''
    self.execute(
      '''CREATE TABLE IF NOT EXISTS Entity
         (uID TEXT, Office TEXT, TimeStamp TEXT,
          Token TEXT, Label TEXT, Lemma TEXT)''')


  def createPHRASES(self):
    '''
    '''
    self.execute(
      '''CREATE TABLE IF NOT EXISTS Phrase
         (uID TEXT, Office TEXT, TimeStamp TEXT,
          Phrase TEXT StartIndex INT EndIndex INT)''')


  def listTables(self):
    '''
    '''
    with self.connection:
      cursor = self.connection.cursor()
      cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
      return cursor.fetchall()


  def getMostRecent(self, office):
    '''
    Returns the row-dict for the most recent forcast
    '''
    self.connection.row_factory = lite.Row
    with self.connection as con:
      cursor = con.cursor()
      cursor = cursor.execute("SELECT * FROM Forecast WHERE Office=? ORDER BY TimeStamp DESC LIMIT 1", (office,))
      row = cursor.fetchall()
    try:
      return row[0]
    except:
      return {'TimeStamp': datetime(1900,1,1).strftime('%Y%m%dT%H:%M:%S')}

