#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File: extract_nws_terms.py
# Author: JJ Miller
# Date: 2018-08-27
# Last Modified: 2018-08-27

import pandas as pd
import os
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from dateutil import parser
from tqdm import tqdm
from multiprocessing import Pool
import json
import uuid


def phrase_search(text_list, nws_list):
    '''
    '''
    count_dict = dict()
    for phrase in nws_list:
        n = len(phrase.split(' '))
        for text in text_list:
            grams = ngrams(text.upper().split(' '), n)
            gram_strings = list()
            # Concat all of the ngrams to make them strings which can be
            # matched with the provided phrase
            for gram in grams:
                gram_strings.append(' '.join(gram))
            if phrase.upper() in gram_strings:
                count_dict.setdefault(phrase, 0)
                count_dict[phrase] += 1

    return count_dict


def remove_stopwords(text):
    '''
    '''
    stop_words = set(stopwords.words('english'))
    text_word_list = word_tokenize(text)
    cleaned_text = [w for w in text_word_list if not w in stop_words]
    cleaned_text = ' '.join(cleaned_text)
    return cleaned_text



def clean(text):
    '''
    '''
    clean_sections = list()
    text = text.replace('  ', ' ')
    sections = text.split('\n\n')
    clean_sections = list()
    for ind, sec in enumerate(sections):
        clean_sections.append(remove_stopwords(sec.replace('\n', '').strip()))
        
    return clean_sections


def add2df(count_dict, info):
    '''
    '''
    dt = parser.parse(info['timestamp'])
    node = uuid.getnode()
    forecast_id = uuid.uuid1()
    location = info['location']
    office = info['station_id']
    df = pd.DataFrame()
    for term, count in count_dict.items():
        tmp_df = pd.DataFrame({'forecast_time':[dt],
                           'city':[location],
                           'office':[office],
                           'count':[count], 
                           'term':[term],
                           'forecast_id': [forecast_id]})
        df = df.append(tmp_df, ignore_index=True)
    return df


def extract(forecast):
    '''
    '''
    afd = forecast[0]
    nws_list = forecast[1]
    text = afd['text'].upper()
    sections = clean(text)
    count_dict = phrase_search(sections, nws_list)
    df = add2df(count_dict, afd)
    return df


def iterate(path, office):
    '''
    '''
    if os.path.exists('extractions/{0}_subset_extractions.csv'.format(office)):
        print('Office {} already extracted.'.format(office))
        return

    import string

    df = pd.DataFrame() 
    try:
        with open(path, 'r') as j:
            data = json.load(j)
    except Exception as e:
        print(e)
        return

    with open('nws_office_info.json', 'r') as j:
        office_info = json.load(j)


    print('extracting for {0}'.format(office))

    #f = 'fulltext/{0}.json'.format(year)

    #with open('cleaned_nws_terms.json', 'r') as j:
    #    nws_terms = json.load(j)
    
    #nws_list = set()
    #for letter in string.ascii_lowercase:
    #    for term in nws_terms[letter]:
    #        nws_list.add(term)
        
    nws_list = ['tornado','hard freeze', 'freeze', 'bomb', 'atmospheric river', 'gap wind', 'downslope wind', 'flooding', 'hurricane', 'smoke', 'snow', 'supercell', 'fog', 'accumulation', 'vorticity equation', 'bow echo', 'bright band', 'derecho', 'downburst', 'microburst', 'sea breeze']

    # Search the text for the phrases/terms using multiprocessing
    #   to help speed up processing the data. 
    pairs = list()
    for year in data:
        for month in data[year]:
            for forecast_time, text in data[year][month].items():
                forecast = {
                            'text': text['text'],
                            'station_id': office,
                            'timestamp': forecast_time,
                            'location': office_info[office]['City']}
                pairs.append((forecast, nws_list))

    max_ = len(pairs)
    print(len(pairs))
    with Pool(4) as p:
        # the input for imap needs to be a list and we need to pass both
        #   the forecast and the terms in a tuple
        #pairs = [(text, nws_list) for forecast_time, text in data[year][month].items()]
        extractions = list(tqdm(p.imap(extract, pairs), total=max_))

    print("converting extractions to dataframe")
    # The extractions are returned in a dict so we need to convert that to one big 
    #  dataframe so we can save it as a csv
    for extraction in tqdm(extractions):
        df = df.append(extraction, ignore_index=True)
    
    df.to_csv('extractions/{0}_subset_extractions.csv'.format(office), index=False)
        

def main():
    year = 2017
    offices = ["BOX","GSP","EPZ","FWD","BOU","BOI","PAH","FSD","LIX","OAX","PIH","FGZ","RIW","RAH","TAE","OHX","MQT","LBF","FGF","IWX","MRX","JAX","GLD","APX","BGM","JAN","CHS","PHI","PQR","MAF","GYX","FFC", "LKN","CRP","GJT","GUM","TSA","DTX","DMX","AMA","HUN","MFL","MFR","BMX","BUF","LSX","AFG","GRR", "SHV","DLH","AFC","EWX","GRB","MHX","DDC","PBZ","RNK","BTV","ABQ","OKX","TBW","LZK","PSR","MOB", "MPX","DVN","CTP","ILN","AKQ","ILM","REV","EKA","LUB","HFO","HNX","ILX","SEW","GGW","OUN","JKL", "ICT","UNR","CLE","PDT","SJT","SJU","VEF","MEG","PUB","BRO","LMK","RLX","KEY","ALY","TWC","LWX", "CAR","TOP","MSO","LCH","LOT","BYZ","LOX","CAE","MKX","CYS","SGX","EAX","BIS","SGF","MLB","HGX", "MTR","STO","IND","ABR","AJK","SLC","ARX","OTX","GID","TFX"]

    for office in offices:
        path = '/rgroup/dsig/projects/knowledge_graph/data/afd_extractions/data/{}_forecasts.json'.format(office)
        iterate(path, office)


if __name__ == '__main__':
    main()






