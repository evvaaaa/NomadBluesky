from typing import Any
import psutil
import tempfile
import pprint
import logging
import zipfile
import io
from pathlib import Path
import requests
import os


def _get_environment_variable(name: str) -> str:
    if (variable := os.environ.get(name)) is None:
        raise ValueError(f"`{name}` environment variable not found.")
    return variable


DEFAULT_TIMEOUT = 10.0

NOMAD_API_TOKEN = _get_environment_variable("NOMAD_API_TOKEN")
NOMAD_API_URL = _get_environment_variable("NOMAD_API_URL")


def create_dataset(
    dataset_name: str,
    timeout=DEFAULT_TIMEOUT,
    nomad_url: str = NOMAD_API_URL,
    nomad_token: str = NOMAD_API_TOKEN,
) -> dict[str, Any]:
    """
    Create a new "dataset".

    The dataset contains upload.
    """
    return requests.post(
        f"{nomad_url}datasets/",
        headers={
            "Authorization": f"Bearer {nomad_token}",
            "Accept": "application/json",
        },
        json={"dataset_name": dataset_name},
        timeout=timeout,
    ).json()


def create_upload(
    upload_name: str,
    timeout=DEFAULT_TIMEOUT,
    nomad_url: str = NOMAD_API_URL,
    nomad_token: str = NOMAD_API_TOKEN,
):
    """
    Create a new "upload".

    The upload created in this class is a directory containing other uploads.
    """

    return requests.post(
        f"{nomad_url}uploads?upload_name={upload_name}",
        headers={
            "Authorization": f"Bearer {nomad_token}",
            "Accept": "application/json",
        },
        timeout=timeout,
    ).json()


def add_file_to_upload(
    zip_name: str,
    upload_path: Path,
    parent_upload_uid: str | None,
    timeout=DEFAULT_TIMEOUT,
    nomad_url: str = NOMAD_API_URL,
    nomad_token: str = NOMAD_API_TOKEN,
):
    """Upload a single file under the `parent_upload_name` upload.

    If parent_upload_name is `None` then the root directory will be used.
    """
    parent_upload_uid = "" if parent_upload_uid is None else f"{parent_upload_uid}/"

    file_size = upload_path.stat().st_size
    memory_left = psutil.virtual_memory().available

    if memory_left >= file_size:
        zip_buffer = io.BytesIO()
    else:
        zip_buffer = tempfile.NamedTemporaryFile(delete_on_close=True)

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(str(upload_path), arcname=upload_path.name)
    zip_buffer.seek(0)

    try:
        response = requests.put(
            f"{nomad_url}uploads/{parent_upload_uid}raw/{zip_name}",
            headers={
                "Authorization": f"Bearer {nomad_token}",
                "Accept": "application/json",
            },
            data=zip_buffer,
            timeout=timeout,
        ).json()

        logging.debug(pprint.pformat(response))
        return response

    finally:
        zip_buffer.close()


def check_upload_status(
    upload_id: str,
    timeout=DEFAULT_TIMEOUT,
    nomad_url: str = NOMAD_API_URL,
    nomad_token: str = NOMAD_API_TOKEN,
) -> dict[str, Any]:
    return requests.get(
        f"{nomad_url}uploads/{upload_id}",
        headers={"Authorization": f"Bearer {nomad_token}"},
        timeout=timeout,
    ).json()


def add_upload_metadata(
    upload_id: str,
    metadata: dict,
    timeout=DEFAULT_TIMEOUT,
    nomad_url: str = NOMAD_API_URL,
    nomad_token: str = NOMAD_API_TOKEN,
) -> dict[str, Any]:
    return requests.post(
        f"{nomad_url}uploads/{upload_id}/edit",
        headers={
            "Authorization": f"Bearer {nomad_token}",
            "Accept": "application/json",
        },
        json=metadata,
        timeout=timeout,
    ).json()


def query(
    query_fields: list[str],
    page_size=1,
    required: list[str] | None = None,
    timeout=DEFAULT_TIMEOUT,
    nomad_url: str = NOMAD_API_URL,
    nomad_token: str = NOMAD_API_TOKEN,
) -> dict[str, Any]:
    query = {
        "query": {"all": query_fields},
        "pagination": {"page_size": page_size},
    }
    if required:
        query.update({"required": {"include": required}})

    return requests.post(
        f"{nomad_url}/entries/query",
        json=query,
        headers={
            "Authorization": f"Bearer {nomad_token}",
            "Accept": "application/json",
        },
        timeout=timeout,
    ).json()

    # TODO:
    # This could be used for automatically publishing data to a central nomad service...
    # It could be nice if we have already created a user for the data in question but currently we don't have
    # a central nomad service. We can play around with it in future.
    # def publish_upload(
    #     upload_id: str,
    timeout = (DEFAULT_TIMEOUT,)


#     nomad_url: str = NOMAD_API_URL,
#     nomad_token: str = NOMAD_API_TOKEN,
# ) -> dict[str, Any]:
#     return requests.post(
#         f"{nomad_url}uploads/{upload_id}/action/publish",
#         headers={
#             "Authorization": f"Bearer {nomad_token}",
#             "Accept": "application/json",
#         },
#         timeout=timeout,
#     ).json()
