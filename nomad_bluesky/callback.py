import queue
import typing
import threading

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

from .logger import logger
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


class NomadCallback:
    def __init__(
        self, nomad_api_url: str, nomad_api_token: str, zmq_url: str | None = None
    ):
        self.NOMAD_API_URL: str = nomad_api_url
        self.NOMAD_API_TOKEN: str = nomad_api_token
        self.ZMQ_URL: str | None = zmq_url

        self._document_queue: queue.Queue[tuple[str, Document] | None] = queue.Queue()

        # The uid of the run to the upload
        self._run_start_to_upload: dict[str, str] = {}

        # The uid of the event descriptor to the uid of the run start.
        # Seperate so that we know to remove from cache after run stop comes in.
        self._descriptor_to_run_start: dict[str, str] = {}

        self._serve_thread: threading.Thread | None = None

    def __call__(self, name: str, document: Document):
        if self._serve_thread is None:
            # If there is no serve thread then put the document manually.
            self.send_document(name, document)
        else:
            self._document_queue.put((name, document))

    def _listen_over_zmq(self, zmq_url: str):
        dispatcher = RemoteDispatcher(zmq_url)
        dispatcher.subscribe(
            lambda name, document: self._document_queue.put((name, document))
        )
        dispatcher.start()

    def _serve(self):
        while True:
            popped = self._document_queue.get()
            if popped is None:  # None is used as the kill signal
                break
            name, document = popped
            self.send_document(name, document)

    def serve(self):
        """Starts `_serve` in a different thread and if provided will listen on `zmq_url` for new documents to send."""

        self._serve_thread = threading.Thread(target=self._serve, daemon=True)
        self._serve_thread.start()

        if self.ZMQ_URL:
            # If `zmq_address` then this is blocking and the thread will be busy waiting for new documents over zmq.
            self._listen_over_zmq(self.ZMQ_URL)

        # If no `zmq_address` is provided then documents can be sent from this thread with `__call__`.

    def join(self):
        """If using directly subscribed to the `RunEngine` instead of as a service, then it may finish while the queue has elements.

        We use a kill signal and a join to keep the thread alive until it's finished processing all documents.
        """
        if self._serve_thread:
            self._document_queue.put(None)
            self._serve_thread.join()

    def send_document(self, name: str, document: Document):
        # TODO: convert the document from dictionary to subclasses of event-model basemodels containing
        # our experiment metadata. Then we'd match here by those classes.

        match name:
            case "start":
                self.upload_run_start(typing.cast(RunStart, document))
            case "stop":
                self.upload_run_stop(typing.cast(RunStop, document))
            case "descriptor":
                self.upload_descriptor(typing.cast(EventDescriptor, document))
            case "event":
                self.upload_event(typing.cast(Event, document))
            case _:
                raise RuntimeError(
                    f"Receieved unsupported document `{name}`. Other documents are in progress."
                )

    def upload_run_start(self, document: RunStart):
        upload_name = f"run_{document['time']}"
        upload_id = create_upload(
            upload_name, self.NOMAD_API_URL, self.NOMAD_API_TOKEN
        )["upload_id"]
        logger.info(f"Created upload with name `{upload_name}` and ID `{upload_id}`")
        self._run_start_to_upload[document["uid"]] = upload_id

        add_dictionary_to_upload(
            f"{document['time']}_start",
            typing.cast(dict, document),
            upload_id,
            self.NOMAD_API_URL,
            self.NOMAD_API_TOKEN,
        )
        logger.info(
            f"Added `start` document `{document['uid']}` to upload `{upload_id}`."
        )

    def upload_run_stop(self, document: RunStop):
        upload_id = self._run_start_to_upload.pop(document["run_start"])
        for descriptor_uid in [
            d for d, s in self._descriptor_to_run_start.items() if s == upload_id
        ]:
            del self._descriptor_to_run_start[descriptor_uid]

        add_dictionary_to_upload(
            f"{document['time']}_stop",
            typing.cast(dict, document),
            upload_id,
            self.NOMAD_API_URL,
            self.NOMAD_API_TOKEN,
        )
        logger.debug(
            f"Added `stop` document `{document['uid']}` to upload `{upload_id}`."
        )

    def upload_descriptor(self, document: EventDescriptor):
        self._descriptor_to_run_start[document["uid"]] = document["run_start"]
        upload_id = self._run_start_to_upload[document["run_start"]]

        add_dictionary_to_upload(
            f"{document['time']}_descriptor",
            typing.cast(dict, document),
            upload_id,
            self.NOMAD_API_URL,
            self.NOMAD_API_TOKEN,
        )
        logger.debug(
            f"Added `event` document `{document['uid']}` to upload `{upload_id}`."
        )

    def upload_event(self, document: Event):
        upload_id = self._run_start_to_upload[
            self._descriptor_to_run_start[document["descriptor"]]
        ]

        add_dictionary_to_upload(
            f"{document['time']}_event",
            typing.cast(dict, document),
            upload_id,
            self.NOMAD_API_URL,
            self.NOMAD_API_TOKEN,
        )
        logger.debug(
            f"Added `event` document `{document['uid']}` to upload `{upload_id}`."
        )
