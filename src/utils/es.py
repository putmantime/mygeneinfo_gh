from pyes import ES
import logging
log = logging.getLogger('pyes')
log.setLevel(logging.DEBUG)
if len(log.handlers) == 0:
    log_handler = logging.StreamHandler()
    log.addHandler(log_handler)

from config import ES_SERVER


def get_es():
    conn = ES(ES_SERVER, default_indexes=[],
              timeout=10.0)
    return conn
