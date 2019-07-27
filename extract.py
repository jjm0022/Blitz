import spacy

import json
import types
from dotmap import DotMap
from spacy.matcher import PhraseMatcher

from dateutil.parser import parse
import re

from database import DB


class Connection:

  def __init__(self, db_path):
    if db_path:
      self.db = DB(db_path=db_path)
    else:
      self.db = DB()

    table_keys = dict({'uID': 'TEXT', 'Office': 'TEXT', 'TimeStamp': 'TEXT',
                       'Phrase': 'TEXT', 'StartIndex': 'INT', 'EndIndex': 'INT'})
    table = "Phrase"
    self.db.createTable(table, table_keys)
  
  def getUnprocessed(self):
    '''
    '''
    with self.db.connection as c:
      cur = c.cursor()
      query = cur.execute('''SELECT * FROM Forecast f WHERE NOT
                             EXISTS (SELECT 1 FROM Processed WHERE uID = f.uID)
                             ORDER BY Office, TimeStamp;''')
      rows = query.fetchall()
    for row in rows:
      yield row


class Matcher:

  def getPhrases(self, phrases=None):
    '''
    Generator that provides rows 
    '''
    ind = 0
    if not phrases:
      phrases = self.patternGenerator(self.pattern_path)
    if isinstance(phrases, types.GeneratorType):
      phrases = list(phrases)

    # Only return the longer of phrases that overlap
    while ind < len(phrases)-1:
      if self._HasOverlap(phrases[ind], phrases[ind+1]): 
        if (phrases[ind].end - phrases[ind].start) < (phrases[ind+1].end - phrases[ind+1].start):
          ind+=1
          continue
      if self._HasOverlap(phrases[ind], phrases[ind-1]):
        if (phrases[ind].end - phrases[ind].start) < (phrases[ind-1].end - phrases[ind-1].start):
          ind+=1
          continue
      yield dict({'uID': self.uid,
                  'Office': self.office,
                  'TimeStamp': self.time_string,
                  'Phrase': phrases[ind].text,
                  'StartIndex': phrases[ind].start,
                  'EndIndex': phrases[ind].end})
      ind += 1  
    if self._HasOverlap(phrases[-1], phrases[-2]):
      if (phrases[-1].end - phrases[-1].start) > (phrases[-2].end - phrases[-2].start):
        yield dict({'uID': self.uid,
                    'Office': self.office,
                    'TimeStamp': self.time_string,
                    'Phrase': phrases[-1].text,
                    'StartIndex': phrases[-1].start,
                    'EndIndex': phrases[-1].end})
    else:
      yield dict({'uID': self.uid,
                  'Office': self.office,
                  'TimeStamp': self.time_string,
                  'Phrase': phrases[-1].text,
                  'StartIndex': phrases[-1].start,
                  'EndIndex': phrases[-1].end})

  def _HasOverlap(self, a1, a2):
    '''
    Check if the 2 annotations overlap.
    '''
    return (a1.start >= a2.start and a1.start < a2.end or
            a1.end > a2.start and a1.end <= a2.end)

  def patternGenerator(self, pattern_path):
    '''
    Returns the starting and ending character index for a phrase match along with the phrase
    '''
    phrases = self.readPatterns(self._nlp.tokenizer, pattern_path)
    matcher = PhraseMatcher(self._nlp.tokenizer.vocab, max_length=6)
    matcher.add("Phrase", None, *phrases)
    doc = self._nlp.tokenizer(self.forecast.lower())
    for w in doc:
      _ = doc.vocab[w.text]
    matches = matcher(doc)
    for ent_id, start, end in matches:
      yield DotMap(text=doc[start:end].text,
                   start=doc[start:end].start_char,
                   end=doc[start:end].end_char)
    
  def readPatterns(self, tokenizer, loc, n=-1):
    for i, line in enumerate(open(loc)):
      data = json.loads(line.strip())
      phrase = tokenizer(data["text"].lower())
      for w in phrase:
        _ = tokenizer.vocab[w.text]
      if len(phrase) >= 2:
        yield phrase


class Pipeline(Matcher, Connection):

  def __init__(self, row, preprocess=True):
    '''
    Row is a dict of a row from the AFD table
    '''
    Connection.__init__(self, db_path=None)
    self._nlp = spacy.load('en_core_web_sm')
    self.forecast = row['Forecast']
    self.raw_forecast = row['Forecast']
    self.time_string = row['TimeStamp']
    self.time_ = parse(row['TimeStamp'])
    self.office = row['Office']
    self.uid = row['uID']
    self.pattern_path = 'data/patterns.json'
    
    if preprocess:
      self._preProcess()

    self.doc = self._nlp(self.forecast)

  def processForecast(self):
    '''
    '''
    phrases = self.getPhrases()
    for phrase in phrases:
      self.db.insert('Phrase', phrase)

    # Update the processed table to show that the forecast has been processed
    processed_dict = dict({'uID': self.uid, 
                          'Office': self.office, 
                          'TimeStamp': self.time_string,
                          'Dataset': 0})
    self.db.insert('Processed', processed_dict)
    

  def _preProcess(self):
    '''
    Processes the text during initialization
    '''
    self.forecast = self.processText(self.forecast)

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
    # Remove any words that contain letters AND numbers
    clean_text = re.sub(r'(\b[A-Z]{1,}\d{1,}\b)', ' ', clean_text)
    # Remove any areas with more than one white-space back to back
    clean_text = re.sub(r'\s{2,}', ' ', clean_text)
    clean_text = re.sub(r'\s\.', '.', clean_text)
    clean_text = re.sub(r'\.{2,}', '.', clean_text)
    return clean_text

  def filterTrash(self, doc, numbers=False, stopwords=False):
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
        doc.like_url or doc.like_email or doc.is_currency):
       return False
    else:
      return True


class Extract(Pipeline,Connection):

  def __init__(self, db_path=None):
    Connection.__init__(self, db_path=db_path)
  
  def run(self):
    unprocessed = self.getUnprocessed()
    total_processed = 0
    for row in unprocessed:
      print(f"Processing forecast from {row['Office']} at {row['TimeStamp']}")
      pipe = Pipeline(row)
      pipe.processForecast()
      total_processed += 1
    print(f'{total_processed} forecasts processed.')
