import threading
import queue
import typing
from tiled.client import from_uri
from tiled.client.container import Container

DEFAULT_POLL_PERIOD = 5.0  # seconds


class NomadTiledListener:
    def __init__(
        self,
        nomad_api_url: str,
        nomad_api_token: str,
        tiled_url: str,
        tiled_api_secret: str,
        poll_period: float = DEFAULT_POLL_PERIOD,
    ) -> None:
        self._tiled_url = tiled_url
        self._tiled_api_secret = tiled_api_secret

        # Tiled client and number of elements at previous poll,
        # `None` in the case of not connected. A thready will poll
        self._tiled_client: Container | None = None
        self._number_of_elements: int | None = None

        self._run_queue: queue.Queue[tuple[str, typing.Any]] = queue.Queue()

    def _serve(self): ...

    def serve(self):
        self._serve_thread = threading.Thread(target=self._serve, daemon=True)
        self._serve_thread.start

    def try_connect(self):
        try:
            self._tiled_client = typing.cast(
                Container, from_uri(self._tiled_url, api_key=self._tiled_api_secret)
            )
        except Exception as exception:
            self._tiled_client = None
