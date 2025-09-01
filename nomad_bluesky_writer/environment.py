import os


def _get_environment_variable(name: str) -> str:
    if (variable := os.environ.get(name)) is None:
        raise ValueError(f"`{name}` environment variable not found.")
    return variable


NOMAD_API_TOKEN: str = _get_environment_variable("NOMAD_API_TOKEN")
NOMAD_API_URL: str = _get_environment_variable("NOMAD_API_URL")
ZMQ_ADDRESS: str | None = os.environ.get("ZMQ_ADDRESS")
