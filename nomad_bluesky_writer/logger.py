import logging

logger = logging.getLogger(__name__)
formatter = logging.Formatter("%(asctime)s (%(levelname)s): %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False
