import io
import json
import pprint
import tempfile
import zipfile
from pathlib import Path
from typing import Any

import psutil
import requests

from .logger import logger

DEFAULT_TIMEOUT = 10.0


def create_dataset(
    dataset_name: str,
    nomad_url: str,
    nomad_token: str,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """
    Create a new "dataset".

    The dataset contains upload.
    """
    response = requests.post(
        f"{nomad_url}datasets/",
        headers={
            "Authorization": f"Bearer {nomad_token}",
            "Accept": "application/json",
        },
        json={"dataset_name": dataset_name},
        timeout=timeout,
    )
    response.raise_for_status()

    response_json = response.json()
    logger.debug(f"create_dataset: {pprint.pformat(response_json)}")
    return response_json


def create_upload(
    upload_name: str,
    nomad_url: str,
    nomad_token: str,
    timeout: float = DEFAULT_TIMEOUT,
):
    """
    Create a new "upload".

    The upload created in this class is a directory containing other uploads.
    """

    response = requests.post(
        f"{nomad_url}uploads?upload_name={upload_name}",
        headers={
            "Authorization": f"Bearer {nomad_token}",
            "Accept": "application/json",
        },
        timeout=timeout,
    )
    response.raise_for_status()

    response_json = response.json()
    logger.debug(f"create_upload: {pprint.pformat(response_json)}")
    return response_json


def add_dictionary_to_upload(
    name: str,
    data: dict[Any, Any],
    upload_uid: str,
    nomad_url: str,
    nomad_token: str,
    timeout: float = DEFAULT_TIMEOUT,
):
    """Add the python dictionary `data`, as a .json, to the upload."""

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        # Serialize the dictionary to JSON and write it to the zip in memory
        json_bytes = json.dumps(data).encode("utf-8")
        zip_file.writestr(f"{name}.json", json_bytes)
    zip_buffer.seek(0)
    with open("hahaha.zip", "wb") as f:
        f.write(zip_buffer.read())

    zip_buffer.seek(0)
    try:
        response = requests.put(
            f"{nomad_url}uploads/{upload_uid}/raw/{name}",
            headers={
                "Authorization": f"Bearer {nomad_token}",
                "Accept": "application/json",
            },
            data=zip_buffer,
            timeout=timeout,
        )
        response.raise_for_status()

        response_json = response.json()
        logger.debug(f"add_dictionary_to_upload: {pprint.pformat(response_json)}")
        return response_json

    finally:
        zip_buffer.close()


def add_file_to_upload(
    name: str,
    upload_path: Path,
    upload_uid: str,
    nomad_url: str,
    nomad_token: str,
    timeout: float = DEFAULT_TIMEOUT,
):
    """Upload a single file under the `parent_upload_name` upload.

    If parent_upload_name is `None` then the root directory will be used.
    """

    # Hold zip in memory if the file is small enough, else temporarily store it on disk
    file_size = upload_path.stat().st_size
    memory_left = psutil.virtual_memory().available
    if memory_left >= file_size:
        zip_buffer = io.BytesIO()
    else:
        zip_buffer = tempfile.NamedTemporaryFile(delete_on_close=True)

    try:
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.write(str(upload_path), arcname=upload_path.name)
        zip_buffer.seek(0)

        response = requests.put(
            f"{nomad_url}uploads/{upload_uid}/raw/{name}",
            headers={
                "Authorization": f"Bearer {nomad_token}",
                "Accept": "application/json",
            },
            data=zip_buffer,
            timeout=timeout,
        )
        response.raise_for_status()

        response_json = response.json()
        logger.debug(f"add_file_to_upload: {pprint.pformat(response_json)}")
        return response_json

    finally:
        zip_buffer.close()


def check_upload_status(
    upload_id: str,
    nomad_url: str,
    nomad_token: str,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    response = requests.get(
        f"{nomad_url}uploads/{upload_id}",
        headers={"Authorization": f"Bearer {nomad_token}"},
        timeout=timeout,
    )
    response.raise_for_status()

    response_json = response.json()
    logger.debug(f"check_upload_status: {pprint.pformat(response_json)}")
    return response_json


def add_upload_metadata(
    upload_id: str,
    metadata: dict,
    nomad_url: str,
    nomad_token: str,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    response = requests.post(
        f"{nomad_url}uploads/{upload_id}/edit",
        headers={
            "Authorization": f"Bearer {nomad_token}",
            "Accept": "application/json",
        },
        json={"metadata": metadata} if metadata else None,
        timeout=timeout,
    )
    response.raise_for_status()

    response_json = response.json()
    logger.debug(f"add_upload_metadata: {pprint.pformat(response_json)}")
    return response_json


def query(
    query_fields: list[str],
    nomad_url: str,
    nomad_token: str,
    page_size=1,
    required: list[str] | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    query = {
        "query": {"all": query_fields},
        "pagination": {"page_size": page_size},
    }
    if required:
        query.update({"required": {"include": required}})

    response = requests.post(
        f"{nomad_url}/entries/query",
        json=query,
        headers={
            "Authorization": f"Bearer {nomad_token}",
            "Accept": "application/json",
        },
        timeout=timeout,
    )
    response.raise_for_status()

    response_json = response.json()
    logger.debug(f"query: {pprint.pformat(response_json)}")
    return response_json


# TODO:
# This could be used for automatically publishing data to a central nomad service...
# It could be nice if we have already created a user for the data in question but currently we don't have
# a central nomad service. We can play around with it in future.

# def publish_upload(
#     upload_id: str,
#     nomad_url: str,
#     nomad_token: str,
#     timeout=DEFAULT_TIMEOUT,
# ) -> dict[str, Any]:
#     return requests.post(
#         f"{nomad_url}uploads/{upload_id}/action/publish",
#         headers={
#             "Authorization": f"Bearer {nomad_token}",
#             "Accept": "application/json",
#         },
#         timeout=timeout,
#     ).json()
