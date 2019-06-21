import spacy
from forecastDb import DB

from dateutil.parser import parse
import re


'''
TODO Different tables for POS, ENTITY, PHRASES, TERM_COUNTS
      - Each table should have it's own function (Pipeline)
      - Each forecast should be processed before
'''

class Pipeline(object):

  def __init__(self, row, preprocess=True):
    '''
    Row is a dict of a row from the AFD table
    '''
    self._nlp = spacy.load('en_core_web_sm')
    self.forecast = row['Forecast']
    self.raw_forecast = row['Forecast']j
    self.time_string = row['TimeStamp']
    self.time_ = parse(row['TomeStamp'])
    self.office = row['Office']
    
    if preprocess:
      self._preProcess()
    

  def _preProcess(self):
    '''
    Processes the text during initialization
    '''
    self.forecast = self.processText(self.forecast)

  def connect(self, db_path):
    '''
    Connect to database
    '''
    self.db = DB(dp_path)

  def processText(self, text):
    '''
    Cleans text
    Input: text <string>
    Returns: <string> with irrelevant characters removed    
    '''
    clean_text = ''
    sections = text.split('\n\n')
      
    for sec in sections:
      if len(sec.strip()) > 25:
        clean_text = clean_text + sec.strip() + ' '
    
    # Remove new-line characters
    clean_text = re.sub(r'\n', ' ', clean_text)
    # Remove everything but letters and punctuation
    clean_text = re.sub(r'[^a-zA-Z0-9\s\.\,\!\?\']', ' ', clean_text)
    # Remove ...
    clean_text = re.sub(r'\.{2,}', ' ', clean_text)
    # Remove any areas with more than one white-space back to back
    clean_text = re.sub(r'\s{2,}', ' ', clean_text)
    clean_text = re.sub(r'\s\.', '.', clean_text)
    clean_text = re.sub(r'\.{2,}', '.', clean_text)
    
    self.forecast = text

  def filterTrash(doc, numbers=True, stopwords=True):
    '''
    Returns true if token is not a word or number
    '''
    if numbers:
      if doc.like_num:
        return False
    if stopwords:
      if doc.is_stop:
        return False

    if (doc.is_punct or doc.is_space or doc.is_bracket or doc.is_quote or 
        doc.like_url or doc.like_email):
       return False
    else:
      return True

  