from ucsc_base import load_ucsc_exons

__metadata__ = {
    '__collection__' : 'ucsc_exons',
    'structure': {'exons': None},
}


def load_genedoc(self=None):
    genedoc_d = load_ucsc_exons()
    return genedoc_d


def get_mapping(self=None):
    mapping = {
        #do not index exons
        "exons":  {"dynamic" : False,
                   "type": "object",
                   "index": "no",
                   "include_in_all": False},
    }
    return mapping
