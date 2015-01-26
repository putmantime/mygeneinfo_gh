from mongokit import OR
from pharmgkb_base import load_pharmgkb

__metadata__ = {
    '__collection__' : 'pharmgkb',
    'structure': {'pharmgkb': OR(unicode, list)},
}


def load_genedoc(self=None):
    return load_pharmgkb()


def get_mapping(self=None):
    mapping = {
        "pharmgkb":  {"type": "string",
                     "analyzer": "string_lowercase"}
    }
    return mapping
