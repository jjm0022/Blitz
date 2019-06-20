import os
import time
from logger import create_logger
from datetime import datetime
from forecast import Forecast
import requests


'''
import airflow
from airflow.modules import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id='update_forecasts',
    default_args=args,
    schedule_interval=None,
)
'''

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
    print("Status Code: {0} for URL: {1}".format(r.status_code, forecast.url))
    return False 
  
  if forecast.parse(r):
    return forecast.isNew()
  else:
    return False 

def main(office, table):
  '''
  Main function for grabbing forcast from NWS
  '''
  forecast = Forecast(office, table)
  new_forecast = checkNewForecast(forecast)
  if not new_forecast:
    print("No new forecast for {0}".format(forecast.office))
    return
  else:
    print("Adding new forecast for {0}".format(forecast.office))
    forecast.addForecast()

if __name__ == '__main__':
  for office in OFFICES:
    main(office, 'AFD')


'''
logger = create_logger()
now = datetime.now()
logger.info('Starting grabFromNWS workflow')
for office in OFFICES:
  logger.info('Checking most recent forecast for {0}'.format(office))
  task = PythonOperator(
    task_id='get_{0}'.format(office),
    provide_context=True,
    python_callable=main,
    op_arrgs=office,
    dag=dag)
'''
  

