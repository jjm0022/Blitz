import sqlite3 as lite
from datetime import datetime

class DB(object):

  def __init__(self, db_path):
    '''
    '''
    self.db_path = db_path
    self.connection = lite.connect(self.db_path)

  def createTable(self, table_name):
      '''
      '''
      if table_name == 'AFD':
        with self.connection:
            cur = self.connection.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS AFD
                          (uID TEXT, Office TEXT, TimeStamp TEXT, Year INT, Month INT, Day INT, Forecast TEXT)''')
      else:
        # TODO Figure out how to handle table creation
        pass


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

