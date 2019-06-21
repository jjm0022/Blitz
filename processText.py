import spacy



'''
TODO Different tables for POS, ENTITY, PHRASES, TERM_COUNTS
      - Each table should have it's own function (Pipeline)
      - Each forecast should be processed before
'''

class Clean():

  def __init__(self):
    '''
    '''
    self.nlp = spacy.load('en_core_web_sm')
    
  def tokenize(self, text):
    '''
    '''
    clean_sections = list()
    sections = text.split('\n\n')
    for sec in sections:
      if len(sec.strip()) > 25:
        clean_sections.append(sec.strip().replace('\n', ' '))
  
  def clean(self, text):
    '''
    '''
    doc = self.tokenizer(text)
    for sent in doc.sents:
      if len(sent.text) > 20:
        
    return sec
  
  def removeTrash(doc):
    '''
    '''
    if doc.is_digit:
      return False
    elif doc.is_digit:
      return False
    elif doc.is_punct:
      return False
    elif doc.is_space:
      return False
    elif doc.is_bracket:
      return False
    elif doc.is_quote:
      return False
    elif doc.like_url:
      return False
    elif doc.like_email:
      return False
    elif doc.like_num:
      return False
    elif doc.is_stop:
      return False
    else:
      return True



  