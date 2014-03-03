from mongokit import OR
from entrez_base import Gene2GeneRifParser

__metadata__ = {
    '__collection__' : 'entrez_generif',
    'structure': {'generif': None},
}

def load_genedoc(self=None):
    gene2generif = Gene2GeneRifParser().load()
    return gene2generif

def get_mapping(self=None):
    mapping = {
    	#do not index generif
        "generif": {"index": "no",
                    "type": "object",
                    "dynamic": False,
                    "include_in_all": False},
    }
    return mapping
