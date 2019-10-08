import spacy
import json
import types
import os
from dotmap import DotMap
from spacy.matcher import PhraseMatcher
from dateutil.parser import parse
import re

from database import DB
from database_definitions import Extraction, Office, Forecast, Status


class Connection:

    def __init__(self):
        self.db = DB()
        self.session = self.db.session

    @property
    def total_unprocessed(self):
        return self.session.query(Status).\
                            filter(Status.extracted == 0).\
                            count()

    @property
    def total_processed(self):
        return self.session.query(Status).\
                            filter(Status.extracted == 1).\
                            count()

    def getUnprocessed(self):
        unprocessed = self.session.query(Status).filter(Status.extracted == 0).all()
        print(f"Found {len(unprocessed)} forecasts that haven't been processed")
        for status in unprocessed:
            yield status

    def insert_many(self, extractions):
        self.session.add_all(
                extractions
            )
        self.session.commit()


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

        if len(phrases) <= 1:
            print(f'Only {len(phrases)} identified. Skipping...')
            return None
        print(f'{len(phrases)} identified')
        # Only return the longer of phrases that overlap
        while ind < len(phrases) - 1:
            if self._HasOverlap(phrases[ind], phrases[ind + 1]):
                if (phrases[ind].end - phrases[ind].start) < (phrases[ind + 1].end - phrases[ind + 1].start):
                    ind += 1
                    continue
            if self._HasOverlap(phrases[ind], phrases[ind - 1]):
                if (phrases[ind].end - phrases[ind].start) < (phrases[ind - 1].end - phrases[ind - 1].start):
                    ind += 1
                    continue
            yield Extraction(phrase=phrases[ind].text,
                             start_index=phrases[ind].start,
                             end_index=phrases[ind].end)
            ind += 1

        if self._HasOverlap(phrases[-1], phrases[-2]):
            if (phrases[-1].end - phrases[-1].start) > (phrases[-2].end - phrases[-2].start):
                yield Extraction(phrase=phrases[-ind].text,
                                 start_index=phrases[-ind].start,
                                 end_index=phrases[-ind].end)
        else:
            yield Extraction(phrase=phrases[-ind].text,
                             start_index=phrases[-ind].start,
                             end_index=phrases[-ind].end)

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
        doc = self._nlp.tokenizer(self.forecast_text.lower())
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

    def __init__(self, forecast, preprocess=True):
        '''
        Row is a dict of a row from the AFD table
        '''
        Connection.__init__(self)
        self._nlp = spacy.load('en_core_web_sm')
        self.forecast_text = forecast.raw_text
        self.time_ = forecast.time_stamp
        self.office = forecast.office.name
        self.uid = forecast.id
        project_path = os.path.join(os.environ['GIT_HOME'], 'AFDTools')
        self.pattern_path = os.path.join(project_path, 'data', 'patterns.json')

        if preprocess:
            self._preProcess()

        self.doc = self._nlp(self.forecast_text)

    def processForecast(self):
        '''
        '''
        extractions = self.getPhrases()

        if not extractions:
            return

        self.insert_many(extractions)

    def _preProcess(self):
        '''
        Processes the text during initialization
        '''
        self.forecast_text = self.processText(self.forecast_text)

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


class Extract(Pipeline, Connection):

    def __init__(self):
        Connection.__init__(self)

    def run(self, status=None):
        '''
        If status is provided, run for a single forecast
        '''
        if status:
            forecast = status.forecast
            office = forecast.office
            print(f"Processing a single forecast valid at {forecast.time_stamp}")
            pipe = Pipeline(forecast)
            pipe.processForecast()
            status.extracted = 1
            forecast.processes_text = pipe.forecast_text
            self.session.commit()
            return
        else:
            print('Fetching unprocessed forecasts')
            unprocessed = self.getUnprocessed()
            total_processed = 0
            for status in unprocessed:
                forecast = status.forecast
                office = forecast.office
                print(f"Processing forecast from {office.name}  valid at {forecast.time_stamp}")
                pipe = Pipeline(forecast)
                pipe.processForecast()
                status.extracted = 1
                forecast.processed_text = pipe.forecast_text
                self.session.commit()
                total_processed += 1
            print(f'{total_processed} forecasts processed.')

if __name__ == "__main__":
    ex = Extract()
    ex.run()
