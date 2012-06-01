from entrez_base import Gene2GOParser


__metadata__ = {
    '__collection__' : 'entrez_go',
    'structure': {'go': None}
}

def load_genedoc(self):
    gene2go = Gene2GOParser().load()
    return gene2go

def get_mapping(self):
    mapping = {
        "go":       {"dynamic": False,
                     "path": "just_name",
                     "properties": {
                        "MF": {
                            "dynamic": False,
                            "path": "just_name",
                            "properties": {
                                "term": {
                                    "type": "string",
                                    "index": "no",
                                    "include_in_all": False,   #do not index GO term string
                                },
                                "id": {
                                    "type": "string",
                                    "analyzer": "string_lowercase",
                                    "index_name": "go",
                                }
                            }
                        },
                        "CC": {
                            "dynamic": False,
                            "path": "just_name",
                            "properties": {
                                "term": {
                                    "type": "string",
                                    "index": "no",
                                    "include_in_all": False,   #do not index GO term string
                                },
                                "id": {
                                    "type": "string",
                                    "analyzer": "string_lowercase",
                                    "index_name": "go",
                                }
                            }
                        },
                        "BP": {
                            "dynamic": False,
                            "path": "just_name",
                            "properties": {
                                "term": {
                                    "type": "string",
                                    "index": "no",
                                    "include_in_all": False,   #do not index GO term string
                                },
                                "id": {
                                    "type": "string",
                                    "analyzer": "string_lowercase",
                                    "index_name": "go",
                                }
                            }
                        },
                     }
        }
    }
    return mapping
