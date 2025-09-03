import os

from nomad_bluesky.nomad_api import add_dictionary_to_upload, create_upload
from nomad_bluesky.logger import logger

logger.setLevel("DEBUG")

NOMAD_API_URL = os.environ["NOMAD_API_URL"]
NOMAD_API_TOKEN = os.environ["NOMAD_API_TOKEN"]

some_dictionary = {"foo": "bar"}

uid = create_upload("some_upload", NOMAD_API_URL, NOMAD_API_TOKEN)["upload_id"]
add_dictionary_to_upload("foobar", some_dictionary, uid, NOMAD_API_URL, NOMAD_API_TOKEN)
