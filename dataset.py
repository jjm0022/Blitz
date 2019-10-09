import collections
import json
import os

from database import DB
from extract import Pipeline
from database_definitions import Extraction, Office, Forecast, Status


class Connection:
    def __init__(self):
        self.db = DB()
        self.session = self.db.session

    @property
    def total_not_in_dataset(self):
        return self.session.query(Status).filter(Status.in_dataset == 0).count()

    @property
    def total_in_dataset(self):
        return self.session.query(Status).filter(Status.in_dataset == 1).count()

    def get_not_in_dataset(self, limit=1000):
        """
        """
        unprocessed = (
            self.session.query(Status).filter(Status.in_dataset == 0).limit(limit)
        )
        print(f"Found {len(unprocessed)} forecasts to be added to the dataset")
        for status in unprocessed:
            yield status

    def get_forecast_extractions(self, forecast):
        """
        """
        extractions = (
            self.session.query(Extraction)
            .filter(Extraction.forecast_id == forecast.id)
            .all()
        )
        print(f"Found {len(extractions)} extractions for {forecast.id}")
        for extraction in extractions:
            yield extraction


class Dataset(Connection):
    """
    This class deals with generating a dataset for training a entity recognition model.
    The phrases are being labeled with a generic label for right now.
    """

    def __init__(self, dataset_path=None):
        Connection.__init__(self)

        self.Annotation = collections.namedtuple(
            "Annotation", ["start", "end", "label", "content", "id", "phrase"]
        )

        if dataset_path:
            self.dataset_path = dataset_path
        else:
            project_path = os.path.join(os.environ["GIT_HOME"], "AFDTools")
            self.dataset_path = os.path.join(
                project_path, "data", "nws_phrase_dataest.jsonl"
            )

    def add2Dataset(self, limit=1000):
        """
        """
        # Grab the rows that have not been added to the dataset
        for status in self.get_not_in_dataset(limit=limit):
            forecast = status.forecast
            office = forecast.office
            self.json_lines = list()
            print(f"Processing phrases from {forecast.id}")
            pipe = Pipeline(forecast)
            self.annotations = list()
            annotations_list = list()

            try:
                # for each forecast, store it, and the annotations in a single json with
                for extraction in self.get_forecast_extractions(forecast):
                    annotation = self.Annotation(
                        start=extraction.start_index,
                        end=extraction.end_index,
                        label="PlaceHolder",
                        phrase=extraction.phrase,
                        id=extraction.forecast_id,
                    )

                    if self._AddAnnotation(annotation):
                        annotations_list.append(self._AnnotationToJson(annotation))

                    status.in_dataset = 1
                    self.session.flush()

                if len(annotations_list) == 0:
                    continue
                print(f"Adding {len(annotations_list)} annotations")
                self.json_lines.append(self._toJSON(annotations_list, pipe))

                with open(self.dataset_path, "a+") as t:
                    t.writelines(self.json_lines)
            except Exception as e:
                print(e)
                self.session.commit()

    def _AddAnnotation(self, annotation):
        for a in self.annotations:
            if self._HasOverlap(annotation, a):
                print(f"Overlap with {a.phrase} and {annotation.phrase}")
                return False
        self.annotations.append(annotation)
        return True

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

    def _AnnotationToJson(self, annotation):
        return {
            "text_extraction": {
                "text_segment": {
                    "start_offset": annotation.start,
                    "end_offset": annotation.end,
                }
            },
            "display_name": annotation.label,
        }

    def _toJSON(self, annotations_list, pipe):
        """
        Convert a pure text example into a jsonl string.
        """
        json_obj = {
            "annotations": annotations_list,
            "text_snippet": {"content": pipe.doc.text, "forecast_id": pipe.uid},
        }
        return json.dumps(json_obj, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    d = Dataset()
    d.add2Dataset(limit=10)
