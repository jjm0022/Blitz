from bs4 import BeautifulSoup
from dateutil.parser import parse
from datetime import datetime
from datetime import timedelta
import json
from difflib import SequenceMatcher as SM
from tqdm import tqdm
import sqlite3 as lite
import uuid
import sys

'''
Script used to download the AFD data (Most recent version JJM 4/3/2019)
'''
def createTable():
    '''
    '''
    con = lite.connect('afd2.db')
    with con:
        cur = con.cursor()
        cur.execute('''DROP TABLE IF EXISTS AFD''')
        cur.execute('''CREATE TABLE IF NOT EXISTS AFD
                       (uID TEXT, Office TEXT, TimeStamp TEXT, Year INT, Month INT, Day INT, Ratio REAL, Forecast TEXT)''')


def insertRow(row_dict):
    '''
    '''
    con = lite.connect('afd2.db')
    with con:
        cur = con.cursor()
        cur.execute('INSERT INTO AFD VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (row_dict['uid'],
                                                                       row_dict['office'],
                                                                       row_dict['timestamp'],
                                                                       row_dict['year'],
                                                                       row_dict['month'],
                                                                       row_dict['day'],
                                                                       row_dict['ratio'],
                                                                       row_dict['forecast']))


url_day = 'https://mesonet.agron.iastate.edu/wx/afos/list.phtml?source={0}&year={1}&month={2}&day={3}&view=grid&order=asc'
url_forecast = 'https://mesonet.agron.iastate.edu/wx/afos/{0}'

offices = ["BOX"] #,"GSP","EPZ","FWD","BOU","BOI","PAH","FSD","LIX","OAX","PIH","FGZ","RIW","RAH","TAE","OHX","MQT","LBF","FGF","IWX","MRX","JAX","GLD","APX","BGM","JAN","CHS","PHI","PQR","MAF","GYX","FFC", "LKN","CRP","GJT","GUM","TSA","DTX","DMX","AMA","HUN","MFL","MFR","BMX","BUF","LSX","AFG","GRR", "SHV","DLH","AFC","EWX","GRB","MHX","DDC","PBZ","RNK","BTV","ABQ","OKX","TBW","LZK","PSR","MOB", "MPX","DVN","CTP","ILN","AKQ","ILM","REV","EKA","LUB","HFO","HNX","ILX","SEW","GGW","OUN","JKL", "ICT","UNR","CLE","PDT","SJT","SJU","VEF","MEG","PUB","BRO","LMK","RLX","KEY","ALY","TWC","LWX", "CAR","TOP","MSO","LCH","LOT","BYZ","LOX","CAE","MKX","CYS","SGX","EAX","BIS","SGF","MLB","HGX", "MTR","STO","IND","ABR","AJK","SLC","ARX","OTX","GID","TFX"]

start_year = 2017
end_year = 2018

start_date = datetime(year=start_year,
                      month=1,
                      day=1)
end_date = datetime(year=end_year,
                    month=1,
                    day=1)

dt = timedelta(days=1)
forecast_dict = dict()
office_previous = dict()
total_days = end_date - start_date
node = uuid.getnode()
createTable()


for office in offices:
    current_date = start_date
    pbar = tqdm(total=total_days.days,
                ascii=True,
                desc=office)
    while current_date < end_date:
        forecast_dict.setdefault(office, dict())
        forecast_dict[office].setdefault(current_date.year, dict())
        forecast_dict[office][current_date.year].setdefault(current_date.month, dict())

        office_previous.setdefault(office, None)
        url = url_day.format(office, current_date.year, current_date.month, current_date.day)
        try:
            r = requests.get(url)
        except Exception as e:
            pbar.write(e)
            continue
        soup = BeautifulSoup(r.text, 'html.parser')
        for section in soup.findAll('div'):
            if section.attrs.get('id') == 'sectAFD{0}'.format(office):
                for link in section.findAll('a'):
                    extension = link.attrs.get('href')
                    forecast_url = url_forecast.format(extension)
                    try:
                        forecast_request = requests.get(forecast_url)
                    except Exception as e:
                        pbar.write(e)
                        continue
                    forecast_soup = BeautifulSoup(forecast_request.text, 'html.parser')
                    for main in forecast_soup.findAll('main'):
                        for pre in main.findAll('pre'):
                            if office_previous.get(office):
                                ratio = SM(None, office_previous.get(office), pre.text).ratio()
                                if ratio < 0.6:
                                    forecast_time = parse(extension.split('=')[-1])
                                    pbar.write('time: {0}'.format(forecast_time))
                                    node = uuid.getnode()
                                    row_dict = {'uid': str(uuid.uuid1()),
                                                'office': office,
                                                'timestamp':forecast_time.strftime('%Y%m%dT%H:%M:%S'),
                                                'year': forecast_time.year,
                                                'month': forecast_time.month,
                                                'day': forecast_time.day,
                                                'ratio': ratio,
                                                'forecast': pre.text}
                                    insertRow(row_dict)
                            else:
                                forecast_time = parse(extension.split('=')[-1])
                                pbar.write('time: {0}'.format(forecast_time))
                                row_dict = {'uid': str(uuid.uuid1()),
                                            'office': office,
                                            'timestamp':forecast_time.strftime('%Y%m%dT%H:%M:%S'),
                                            'year': forecast_time.year,
                                            'month': forecast_time.month,
                                            'day': forecast_time.day,
                                            'ratio': 0.0,
                                            'forecast': pre.text}
                                insertRow(row_dict)
                            office_previous[office] = pre.text
        current_date = current_date + dt
        pbar.update(1)
