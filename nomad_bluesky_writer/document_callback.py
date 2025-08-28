import typing
import queue
from threading import Thread
import pprint

from event_model.documents import (
    EventPage,
    Resource,
    RunStart,
    RunStop,
    StreamDatum,
    StreamResource,
    Datum,
    DatumPage,
    Event,
    EventDescriptor,
)


Document = (
    Datum
    | DatumPage
    | Event
    | EventDescriptor
    | EventPage
    | Resource
    | RunStart
    | RunStop
    | StreamDatum
    | StreamResource
)


class NomadDocumentCallback:
    def __init__(self):
        self._document_queue: queue.Queue[tuple[str, Document]] = queue.Queue()

    def __call__(self, name: str, document: Document):
        """Puts the document with it's name to the document queue so it can be picked up by the API thread."""
        self._document_queue.put((name, document))

    def serve(self, daemon=True):
        """Starts `_serve` in a different thread."""

        thread = Thread(target=self._serve, daemon=daemon)
        thread.start()

    def _serve(self):
        while True:
            name, document = self._document_queue.get()
            self.send_document(name, document)

    def send_document(self, name: str, document: Document):
        match name:
            case "run_start":
                self.send_run_start(
                    typing.cast(
                        RunStart, document
                    )  # TODO Use the basemodels and we can add our nomad specific metadata
                )
            case _:
                return
                raise ValueError(
                    f"Receieved a document of unknown type {pprint.pformat(document)}"
                )

    def send_run_start(self, document: RunStart): ...
