from typing import Any
from pathlib import Path
import requests

import os


def _get_environment_variable(name: str) -> str:
    if (variable := os.environ.get(name)) is None:
        raise ValueError(f"`{name}` environment variable not found.")
    return variable


NOMAD_API_TOKEN = _get_environment_variable("NOMAD_API_TOKEN")
NOMAD_API_URL = _get_environment_variable("NOMAD_API_URL")


def create_dataset(
    dataset_name: str,
    nomad_url: str = NOMAD_API_URL,
    nomad_token: str = NOMAD_API_TOKEN,
) -> dict[str, Any]:
    return requests.post(
        f"{nomad_url}datasets/",
        headers={
            "Authorization": f"Bearer {nomad_token}",
            "Accept": "application/json",
        },
        json={"dataset_name": dataset_name},
        timeout=10,
    ).json()


def upload_file_to_nomad(
    upload_path: Path,
    nomad_url: str = NOMAD_API_URL,
    nomad_token: str = NOMAD_API_TOKEN,
):
    """Upload a single file as a new NOMAD upload. Compressed zip/tar files are
    automatically decompressed.
    """
    with upload_path.open("rb") as f:
        return requests.post(
            f"{nomad_url}uploads?file_name={upload_path.name}",
            headers={
                "Authorization": f"Bearer {nomad_token}",
                "Accept": "application/json",
            },
            data=f,
            timeout=30,
        ).json()


def check_upload_status(
    upload_id: str,
    nomad_url: str = NOMAD_API_URL,
    nomad_token: str = NOMAD_API_TOKEN,
) -> dict[str, Any]:
    return requests.get(
        f"{nomad_url}uploads/{upload_id}",
        headers={"Authorization": f"Bearer {nomad_token}"},
        timeout=30,
    ).json()


def add_upload_metadata(
    upload_id: str,
    metadata: dict,
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
        timeout=30,
    ).json()


def query(
    query_fields: list[str], page_size=1, required: list[str] | None = None
) -> dict[str, Any]:
    if not query_fields:
        return {}

    query = {
        "query": {"all": query_fields},
        "pagination": {"page_size": page_size},
    }
    if required:
        query.update({"required": {"include": required}})

    return requests.post(f"{NOMAD_API_URL}/entries/query", json=query).json()


# TODO:
# This could be used for automatically publishing data to a central nomad service...
# It could be nice if we have already created a user for the data in question but currently we don't have
# a central nomad service. We can play around with it in future.
# def publish_upload(
#     upload_id: str,
#     nomad_url: str = NOMAD_API_URL,
#     nomad_token: str = NOMAD_API_TOKEN,
# ) -> dict[str, Any]:
#     return requests.post(
#         f"{nomad_url}uploads/{upload_id}/action/publish",
#         headers={
#             "Authorization": f"Bearer {nomad_token}",
#             "Accept": "application/json",
#         },
#         timeout=30,
#     ).json()
