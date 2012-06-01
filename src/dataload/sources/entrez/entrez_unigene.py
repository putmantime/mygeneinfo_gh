from entrez_base import Gene2UnigeneParser


__metadata__ = {
    '__collection__' : 'entrez_unigene',
    'structure': {'unigene': None}
}

def load_genedoc(self):
    gene2unigene = Gene2UnigeneParser().load()
    return gene2unigene

def get_mapping(self):
    mapping = {
        "unigene":  {"type": "string",
                     "analyzer": "string_lowercase"}
    }
    return mapping
