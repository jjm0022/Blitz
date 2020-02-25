import spacy
import pandas as pd
import numpy as np
import nltk
from nltk.tokenize.toktok import ToktokTokenizer
from nltk.parse.stanford import StanfordParser, StanfordDependencyParser, StanfordNeuralDependencyParser
from nltk.parse.corenlp import CoreNLPDependencyParser
import re
from bs4 import BeautifulSoup
from data.contractions import CONTRACTION_MAP
import unicodedata
from pathlib import Path
from collections import OrderedDict

from IPython.core.debugger import set_trace


nlp = spacy.load('en_core_web_sm', parse=True, tag=True, entity=True)
tokenizer = ToktokTokenizer()
stopword_list = nltk.corpus.stopwords.words('english')
stopword_list.remove('no')
stopword_list.remove('not')

java_path = '/usr/lin/jvm/java-8-openjdk-amd64/bin/java'
os.environ['JAVAHOME'] = java_path
scp = StanfordParser(path_to_jar='/home/jmiller/stanford/stanford-parser-full-2018-10-17/stanford-parser.jar',
                     path_to_models_jar='/home/jmiller/stanford/stanford-parser-full-2018-10-17/stanford-parser-3.9.2-models.jar')
sdp = StanfordDependencyParser(path_to_jar='/home/jmiller/stanford/stanford-parser-full-2018-10-17/stanford-parser.jar',
                     path_to_models_jar='/home/jmiller/stanford/stanford-parser-full-2018-10-17/stanford-parser-3.9.2-models.jar')
sndp = StanfordNeuralDependencyParser(path_to_jar='/home/jmiller/stanford/stanford-parser-full-2018-10-17/stanford-parser.jar',
                     path_to_models_jar='/home/jmiller/stanford/stanford-parser-full-2018-10-17/stanford-parser-3.9.2-models.jar')                   

def remove_accented_chars(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return text


def expand_contractions(text, contraction_mapping=CONTRACTION_MAP):
    
    contractions_pattern = re.compile('({})'.format('|'.join(contraction_mapping.keys())), 
                                      flags=re.IGNORECASE|re.DOTALL)
    def expand_match(contraction):
        match = contraction.group(0)
        first_char = match[0]
        expanded_contraction = contraction_mapping.get(match)\
                                if contraction_mapping.get(match)\
                                else contraction_mapping.get(match.lower())                       
        expanded_contraction = first_char+expanded_contraction[1:]
        return expanded_contraction
        
    expanded_text = contractions_pattern.sub(expand_match, text)
    expanded_text = re.sub("'", "", expanded_text)
    return expanded_text


def remove_special_characters(text, remove_digits=False):
    pattern = r'[^a-zA-z0-9\s]' if not remove_digits else r'[^a-zA-z\s]'
    text = re.sub(pattern, '', text)
    return text


def simple_stemmer(text):
    ps = nltk.porter.PorterStemmer()
    text = ' '.join([ps.stem(word) for word in text.split()])
    return text


def lemmatize_text(text):
    text = nlp(text)
    text = ' '.join([word.lemma_ if word.lemma_ != '-PRON-' else word.text for word in text])
    return text


def remove_stopwords(text, is_lower_case=False):
    tokens = tokenizer.tokenize(text)
    tokens = [token.strip() for token in tokens]
    if is_lower_case:
        filtered_tokens = [token for token in tokens if token not in stopword_list]
    else:
        filtered_tokens = [token for token in tokens if token.lower() not in stopword_list]
    filtered_text = ' '.join(filtered_tokens)    
    return filtered_text


def normalize_corpus(corpus, contraction_expansion=True,
                     accented_char_removal=True, text_lemmatization=True, stopword_removal=True):
    
    normalized_corpus = []
    # normalize each document in the corpus
    for doc in corpus:
        # expand contractions    
        if contraction_expansion:
            doc = expand_contractions(doc)
        # lemmatize text
        if text_lemmatization:
            doc = lemmatize_text(doc)
        # remove stopwords
        if stopword_removal:
            doc = remove_stopwords(doc)
            
        normalized_corpus.append(doc)
    return normalized_corpus

def processText(text):
    """
    Cleans text
    Input: text <string>
    Returns: <string> with irrelevant characters removed
    """
    clean_text = ""
    sections = text.split("\n\n")
    for sec in sections:
        if len(sec.strip()) > 25:
            clean_text = clean_text + sec.strip() + " "
    # Remove new-line characters
    clean_text = re.sub(r"\n", " ", clean_text)
    # Remove everything but letters and punctuation
    clean_text = re.sub(r"[^a-zA-Z\s\.\,\!\?\']", " ", clean_text)
    # Remove ...
    clean_text = re.sub(r"\.{2,}", " ", clean_text)
    # Remove any words that contain letters AND numbers
    clean_text = re.sub(r"(\b[A-Z]{1,}\d{1,}\b)", " ", clean_text)
    # Remove any areas with more than one white-space back to back
    clean_text = re.sub(r"\s{2,}", " ", clean_text)
    clean_text = re.sub(r"\s\.", ".", clean_text)
    clean_text = re.sub(r"\.{2,}", ".", clean_text)
    return clean_text


if __name__ == "__main__":
    import json
    import os
    java_path = '/usr/lin/jvm/java-8-openjdk-amd64/bin/java'
    os.environ['JAVAHOME'] = java_path
    path = Path('./test_data')
    for f in path.glob('*.txt'):
        with open(f, 'r') as t:
            text = t.read()
        d = OrderedDict()
        text = processText(text)
        section_titles = re.findall(r'\b\.[A-Z]+[A-Z\s0-9]+(?=[^a-z])', text)
        sections = re.split(r'\b\.[A-Z]+[A-Z\s0-9]+(?=[^a-z])', text)[1:]

        #set_trace()

        #sentence_nlp = nlp(sections)

        # POS tagging with Spacy 
        #spacy_pos_tagged = [(word, word.tag_, word.pos_) for word in sentence_nlp]
        #pd.DataFrame(spacy_pos_tagged, columns=['Word', 'POS tag', 'Tag type'])

        for title, section in zip(section_titles, sections):
            if 'WARNINGS' in title.upper():
                continue
            if 'POPS' in title.upper():
                continue
            d.setdefault(title.strip(), remove_stopwords(section.strip()))
        
        with open(path.joinpath(f.name.split('.')[0] + '.json'), 'w') as j:
            json.dump(d, j, indent=4)
        
