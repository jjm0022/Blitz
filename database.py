import sqlite3 as lite
import re
from datetime import datetime
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from database_definitions import Forecast, Status, Extraction, Office

class DB:

    def __init__(self,
                 url='mysql+mysqldb://root:b14z3r5@10.0.0.100:3306/testdb',
                 engine=None,
                 echo=False):
        if engine:
            self.session = Session(bind=engine)
        else:
            engine = create_engine(url, echo=False)
            self.session = Session(bind=engine)

class DB(object):

    def __init__(self, db_path=None):
        '''
        '''
        if not db_path:
            project_path = os.path.join(os.environ['GIT_HOME'], 'AFDTools')
            self.db_path = os.path.join(project_path, 'forecasts.db')
        else:
            self.db_path = db_path
        self.connection = lite.connect(self.db_path)
        self.connection.row_factory = lite.Row
        self.processed_dict = dict({'uID': 'TEXT',
                                    'Office': 'TEXT',
                                    'TimeStamp': 'Text'})
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
