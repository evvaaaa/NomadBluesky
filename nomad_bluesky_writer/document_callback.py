import queue
import typing
from threading import Thread

from bluesky.callbacks.zmq import RemoteDispatcher
from event_model.documents import (
    Datum,
    DatumPage,
    Event,
    EventDescriptor,
    EventPage,
    Resource,
    RunStart,
    RunStop,
    StreamDatum,
    StreamResource,
)

from .environment import ZMQ_ADDRESS
from .nomad_api import add_dictionary_to_upload, create_upload

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

        # The upload to put documents in under this plan
        self._current_upload: str | None = None

    def __call__(self, name: str, document: Document):
        """Puts the document with it's name to the document queue so it can be picked up by the API thread."""
        self._document_queue.put((name, document))

    def listen_on_zmq(self, zmq_address: str):
        dispatcher = RemoteDispatcher(zmq_address)
        dispatcher.subscribe(
            lambda name, document: self._document_queue.put((name, document))
        )
        dispatcher.start()

    def serve(self, zmq_address: str | None = ZMQ_ADDRESS):
        """Starts `_serve` in a different thread and if provided will listen on `zmq_address` for new documents to send."""

        thread = Thread(target=self._serve, daemon=True)
        thread.start()

        if zmq_address:  # If no `zmq_address` is provided then documents can be sent from this thread with `__call__`.
            self.listen_on_zmq(zmq_address)

    def _serve(self):
        while True:
            name, document = self._document_queue.get()
            self.send_document(name, document)

    def send_document(self, name: str, document: Document):
        # TODO: convert the document from dictionary to subclasses of event-model basemodels containing
        # our experiment metadata. Then we'd match here by those classes.
        match name:
            case "run_start":
                self.upload_run_start(typing.cast(RunStart, document))
            case "run_start":
                self.upload_run_stop(typing.cast(RunStop, document))
            case _:
                self.upload_document(name, document)

    def upload_document(self, name: str, document: Document):
        assert self._current_upload is not None

        add_dictionary_to_upload(
            name,  # TODO: what naming convention?
            typing.cast(dict, document),
            self._current_upload,
        )

    def upload_run_start(self, document: RunStart):
        upload_name = f"run_{document['time']}"
        self._current_upload = create_upload(upload_name)["upload_id"]
        self.upload_document("run_start", document)

    def upload_run_stop(self, document: RunStop):
        self._current_upload = None
        self.upload_document("run_stop", document)
