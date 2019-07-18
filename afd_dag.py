import sys
sys.path.append('/home/jmiller/git/AFDTools')
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


args = dict({
  'owner': 'airflow',
  'start_date': airflow.utils.dates.days_ago(1),
  'email': ['jj.miller.jm@gmail.com'],
  'email_on_failure': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=10)
})

dag = DAG(
  dag_id='AFD_Workflow',
  schedule_interval='@hourly',
  default_args=args,
)

###############################################################

def getForecast():
  '''
  '''
  import forecast
  from forecast import Forecast
  for office in forecast.OFFICES:
    try:
      f = Forecast(office)
      f.download()
      f.add()
    except:
      pass

grabForecast = PythonOperator(
  task_id='grab_forecast',
  python_callable=getForecast,
  dag=dag
)

###############################################################

def getPhrases():
  '''
  '''
  import grabFromNWS
  grabFromNWS.process()
  
getPhrases = PythonOperator(
  task_id='get_phrases',
  python_callable=getPhrases,
  dag=dag
)

###############################################################

grabForecast >> getPhrases 
