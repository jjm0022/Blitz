import spacy
import pandas as pd
import numpy as np
import nltk
from nltk.tokenize.toktok import ToktokTokenizer
from nltk.parse.corenlp import CoreNLPDependencyParser
import re
from bs4 import BeautifulSoup
import unicodedata
from pathlib import Path
from collections import OrderedDict

# nlp = spacy.load("en_core_web_sm", parse=True, tag=True, entity=True)
tokenizer = ToktokTokenizer()


def remove_stopwords(text, is_lower_case=False):
    stopword_list = nltk.corpus.stopwords.words("english")
    stopword_list.remove("no")
    stopword_list.remove("not")
    tokenizer = ToktokTokenizer()

    tokens = tokenizer.tokenize(text)
    tokens = [token.strip() for token in tokens]
    if is_lower_case:
        filtered_tokens = [token for token in tokens if token not in stopword_list]
    else:
        filtered_tokens = [
            token for token in tokens if token.lower() not in stopword_list
        ]
    filtered_text = " ".join(filtered_tokens)
    filtered_text = remove_space_before_punctuation(filtered_text)
    return filtered_text


def split_sections(text):
    clean_text = ""
    sections = text.split("\n\n")
    for sec in sections:
        if len(sec.strip()) > 25:
            clean_text = clean_text + sec.strip() + " "
    return clean_text


def remove_new_line_char(text):
    return re.sub(r"\n", " ", text)


def remove_bad_characters(text):
    return re.sub(r"[^a-zA-Z\s\.\,\!\?\']", " ", text)


def remove_multiple_periods(text):
    return re.sub(r"\.{2,}", " ", text)


def remove_words_letters_numbers(text):
    return re.sub(r"(\b[A-Z]{1,}\d{1,}\b)", " ", text)


def remove_multiple_spaces(text):
    return re.sub(r"\s{2,}", " ", text)


def remove_space_before_punctuation(text):
    clean_text = re.sub(r'\s+([?.!,\'"])', r'\1', text) 
    return re.sub(r"\.{2,}", ".", clean_text)


def processText(text):
    """
    Cleans text
    Input: text <string>
    Returns: <string> with irrelevant characters removed
    """
    clean_text = split_sections(text)
    # Remove new-line characters
    clean_text = remove_new_line_char(clean_text)
    # Remove everything but letters and punctuation
    clean_text = remove_bad_characters(clean_text)
    # Remove ...
    clean_text = remove_multiple_periods(clean_text)
    # Remove any words that contain letters AND numbers
    clean_text = remove_words_letters_numbers(clean_text)
    # Remove any areas with more than one white-space back to back
    clean_text = remove_multiple_periods(clean_text)
    # Remove any spaces that occur before a period
    clean_text = remove_space_before_punctuation(clean_text)
    # Remove and spaces that occur before a comma
    return clean_text


if __name__ == "__main__":

    path = Path("test_data/kbox.txt")
    with open(path, "r") as t:
        text = t.read()

    t = processText(text)
    t = remove_stopwords(t)
    print(t)

