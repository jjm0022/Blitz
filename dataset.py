
import collections
import json
import os

from database import DB
from processText import Pipeline


class Connection(DB):

  def __init__(self):
    self.db = DB()

  def notInDataset(self):
    '''
    '''
    connection = self.db.connection
    with connection as c:
      cur = c.cursor()
      query = cur.execute('''SELECT * FROM Forecast f WHERE uID 
                             IN(SELECT uID FROM Processed WHERE Dataset = 0)
                             ORDER BY Office, TimeStamp;''')
      rows = query.fetchall()
    for row in rows:
      yield row

  def updateEntry(self, uID):
    '''
    '''
    query = '''UPDATE Processed
                SET Dataset=1
                WHERE uID=?;'''
    connection = self.db.connection
    with connection as c:
      cur = c.cursor()
      cur.execute(query,(uID,))


class Dataset(Connection):
  '''
  This class deals with generating a dataset for training a entity recognition model.
  The phrases are being labeled with a generic label for right now. 
  '''

  def __init__(self, dataset_path=None):
    Connection.__init__(self)    

    self.Annotation = collections.namedtuple('Annotation', ['start', 'end', 'label', 'content', 'id', 'phrase'])

    if dataset_path:
      self.dataset_path = dataset_path
    else:
      self.dataset_path = os.path.join('./data', 'nws_phrase_dataest.jsonl')

  def add2Dataset(self, total=1e35):
    '''
    '''
    # Grab the rows that have not been added to the dataset
    for iteration, forecast in enumerate(self.notInDataset()):
      if iteration >= total:
        break
      self.json_lines = list()
      print(f"Processing phrases from {forecast['uID']}")
      #print(f"Processing phrases for {forecast['Office']} on {forecast['TimeStamp']}")
      pipe = Pipeline(forecast)
      self.annotations = list()
      annotations_list = list()

      # for each forecast, store it, and the annotations in a single json with 
      for row in self.db.getProcessedPhrases(forecast['uID']):
        annotation = self.Annotation(start=row['StartIndex'],
                                end=row['EndIndex'],
                                label='PlaceHolder',
                                content=forecast['Forecast'],
                                phrase=row['Phrase'],
                                id=row['uID'])
          
        if self._AddAnnotation(annotation):
          annotations_list.append(self._AnnotationToJson(annotation))
        
        self.updateEntry(annotation.id)
      if len(annotations_list) == 0:
        print(f"No annotations for {row['Office']} on {row['TimeStamp']}")
        continue
      print(f'Adding {len(annotations_list)} annotations')
      self.json_lines.append(self._toJSON(annotations_list, pipe))

      with open('nws_phrase_dataset.jonsl', 'a+') as t:
        t.writelines(self.json_lines)

  def _AddAnnotation(self, annotation):
    for a in self.annotations:
      if self._HasOverlap(annotation, a):
        print(f'Overlap with {a.phrase} and {annotation.phrase}')
        return False
    self.annotations.append(annotation)
    return True

  def _HasOverlap(self, a1, a2):
    '''
    Check if the 2 annotations overlap.
    '''
    return (a1.start >= a2.start and a1.start < a2.end or
            a1.end > a2.start and a1.end <= a2.end)
    
  def _AnnotationToJson(self, annotation):
    return {
        'text_extraction': {
            'text_segment': {
                'start_offset': annotation.start,
                'end_offset': annotation.end
            }
        },
        'display_name': annotation.label
    }

  def _toJSON(self, annotations_list, pipe):
    '''
    Convert a pure text example into a jsonl string.
    '''
    json_obj = {
        'annotations': annotations_list,
        'text_snippet': {
            'content': pipe.doc.text,
            'forecast_id': pipe.uid
        },
    }
    return json.dumps(json_obj, ensure_ascii=False) + '\n'