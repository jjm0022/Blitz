import sqlite3 as lite
from datetime import datetime

class DB(object):

  def __init__(self, db_path):
    '''
    '''
    self.db_path = db_path
    self.connection = lite.connect(self.db_path)

  def createFORE(self):
    '''
    '''
    with self.connection:
        cur = self.connection.cursor()
        cur.execute(
          '''CREATE TABLE IF NOT EXISTS Forecast
             (uID TEXT, Office TEXT, TimeStamp TEXT, Year INT, Month INT, Day INT, Forecast TEXT)''')
  
  def createPOS(self):
    '''
    '''
    with self.connection:
      cur = self.connection.cursor()
      cur.execute(
        '''CREATE TABLE IF NOT EXISTS Part_of_Speech
           (uID TEXT, Office TEXT, TimeStamp TEXT, Year INT, Month INT, Day INT,
           Forecast TEXT)''')

  def createENT(self):
    '''
    '''
    with self.connection:
      cur = self.connection.cursor()
      cur.execute(
        '''CREATE TABLE IF NOT EXISTS Entity
           (uID TEXT, Office TEXT, TimeStamp TEXT, Year INT, Month INT, Day INT, Forecast TEXT)''')

  def createCOUNT(self):
    '''
    '''
    with self.connection:
      cur = self.connection.cursor()
      cur.execute(
        '''CREATE TABLE IF NOT EXISTS Count
           (uID TEXT, Office TEXT, TimeStamp TEXT, Year INT, Month INT, Day INT, Forecast TEXT)''')

  def createPHRASE(self):
    '''
    '''
    with self.connection:
      cur = self.connection.cursor()
      cur.execute(
        '''CREATE TABLE IF NOT EXISTS Phrase
           (uID TEXT, Office TEXT, TimeStamp TEXT, Year INT, Month INT, Day INT, Forecast TEXT)''')

  def insertRow(self, row_dict):
      '''
      '''
      with self.connection as con:
          cur = con.cursor()
          cur.execute('INSERT INTO AFD VALUES (?, ?, ?, ?, ?, ?, ?)', (row_dict['uid'],
                                                                        row_dict['office'],
                                                                        row_dict['timestamp'],
                                                                        row_dict['year'],
                                                                        row_dict['month'],
                                                                        row_dict['day'],
                                                                        row_dict['forecast']))
                                                                      
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
      cursor = cursor.execute("SELECT * FROM AFD WHERE Office=? ORDER BY TimeStamp DESC LIMIT 1", (office,))
      row = cursor.fetchall()
    try:
      return row[0]
    except:
      return {'TimeStamp': datetime(1900,1,1).strftime('%Y%m%dT%H:%M:%S')}

