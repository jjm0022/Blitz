from collections import namedtuple
import json
import types
import os
import spacy
from spacy.matcher import PhraseMatcher
from dateutil.parser import parse
import re

from database import DB
import clean
from database_definitions import Extraction, Office, Forecast, Status


class Connection:
    def __init__(self):
        self.db = DB()
        self.session = self.db.session

    @property
    def total_unprocessed(self):
        return self.session.query(Status).filter(Status.extracted == 0).count()

    @property
    def total_processed(self):
        return self.session.query(Status).filter(Status.extracted == 1).count()

    def getUnprocessed(self):
        unprocessed = self.session.query(Status).filter(Status.extracted == 0).all()
        print(f"Found {len(unprocessed)} forecasts that haven't been processed")
        for status in unprocessed:
            yield status

    def insert_many(self, extractions):
        self.session.add_all(extractions)
        self.session.commit()


class Matcher:

    def getPhrases(self, doc, phrases=None):
        """
        Generator that provides rows
        """
        assert isinstance(doc, spacy.tokens.doc.Doc), f"<doc> must by of type 'space.tokens.doc.Doc' not {type(doc)}"

        ind = 0
        if not phrases:
            phrases = self.patternGenerator(doc, self.pattern_path)
        if isinstance(phrases, types.GeneratorType):
            phrases = list(phrases)

        if len(phrases) <= 1:
            print(f"Only {len(phrases)} identified. Skipping...")
            return None
        print(f"{len(phrases)} identified")

        # Only return the longer of phrases that overlap
        while ind < len(phrases) - 1:
            if self._HasOverlap(phrases[ind], phrases[ind + 1]):
                if (phrases[ind].end - phrases[ind].start) < (
                    phrases[ind + 1].end - phrases[ind + 1].start
                ):
                    ind += 1
                    continue
            if self._HasOverlap(phrases[ind], phrases[ind - 1]):
                if (phrases[ind].end - phrases[ind].start) < (
                    phrases[ind - 1].end - phrases[ind - 1].start
                ):
                    ind += 1
                    continue
            yield Extraction(
                phrase=phrases[ind].text,
                start_index=phrases[ind].start,
                end_index=phrases[ind].end,
            )
            ind += 1

        if self._HasOverlap(phrases[-1], phrases[-2]):
            if (phrases[-1].end - phrases[-1].start) > (
                phrases[-2].end - phrases[-2].start
            ):
                yield Extraction(
                    phrase=phrases[-ind].text,
                    start_index=phrases[-ind].start,
                    end_index=phrases[-ind].end,
                )
        else:
            yield Extraction(
                phrase=phrases[-ind].text,
                start_index=phrases[-ind].start,
                end_index=phrases[-ind].end,
            )

    def _HasOverlap(self, a1, a2):
        """
        Check if the 2 annotations overlap.
        """
        return (
            a1.start >= a2.start
            and a1.start < a2.end
            or a1.end > a2.start
            and a1.end <= a2.end
        )

    def patternGenerator(self, doc, pattern_path):
        """
        Returns the starting and ending character index for a phrase match along with the phrase
        """
        phrase = namedtuple('Phrase', ['text', 'start', 'end'])
        phrases = self.readPatterns(self._nlp.tokenizer, pattern_path)
        matcher = PhraseMatcher(self._nlp.tokenizer.vocab, max_length=6)
        matcher.add("Phrase", None, *phrases)
        for w in doc:
            _ = doc.vocab[w.text]
        matches = matcher(doc)
        for ent_id, start, end in matches:
            yield phrase(
                text=doc[start:end].text,
                start=doc[start:end].start_char,
                end=doc[start:end].end_char,
            )

    def readPatterns(self, tokenizer, loc, n=-1):
        for i, line in enumerate(open(loc)):
            data = json.loads(line.strip())
            phrase = tokenizer(data["text"].lower())
            for w in phrase:
                _ = tokenizer.vocab[w.text]
            if len(phrase) >= 2:
                yield phrase


class Pipeline(Matcher):
    def __init__(self, forecast, preprocess=True):
        """
        Row is a dict of a row from the AFD table
        """
        self._nlp = spacy.load("en_core_web_sm")
        self.forecast_text = forecast.raw_text
        self.time_ = forecast.time_stamp
        self.office = forecast.office.name
        self.uid = forecast.id
        project_path = os.path.join(os.environ["GIT_HOME"], "AFDTools")
        self.pattern_path = os.path.join(project_path, "data", "patterns.json")

    def standardizeText(self, text):
        """
        """
        return clean.processText(text)

    def extractPhrases(self, text):
        """
        """
        doc = self._nlp(text)
        return self.getPhrases(doc)


class Extract(Pipeline, Connection):
    def __init__(self):
        Connection.__init__(self)

    def run(self, status=None):
        """
        If status is provided, run for a single forecast
        """
        if status:
            assert type(status) == Status, "<status> keyword must be an instance of database_definitions.Status"
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
            print("Fetching unprocessed forecasts")
            unprocessed = self.getUnprocessed()
            total_processed = 0
            for status in unprocessed:
                forecast = status.forecast
                office = forecast.office
                print(
                    f"Processing forecast from {office.name}  valid at {forecast.time_stamp}"
                )
                pipe = Pipeline(forecast)
                pipe.processForecast()
                status.extracted = 1
                forecast.processed_text = pipe.forecast_text
                self.session.commit()
                total_processed += 1
            print(f"{total_processed} forecasts processed.")


if __name__ == "__main__":
    from forecast import Downloader

    recent = d.getMostRecent()

    ex = Extract()
    ex.run(recent.status)
