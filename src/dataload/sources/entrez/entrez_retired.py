from mongokit import OR
from entrez_base import Gene2RetiredParser

__metadata__ = {
    '__collection__' : 'entrez_retired',
    'structure': {'retired': OR(int, list)},
}

def load_genedoc(self):
    gene2retired = Gene2RetiredParser().load()
    return gene2retired

def get_mapping(self):
    mapping = {
        "retired": {"type": "long"},
    }
    return mapping
