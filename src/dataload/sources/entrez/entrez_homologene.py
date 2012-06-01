from entrez_base import HomologeneParser

__metadata__ = {
    '__collection__' : 'entrez_homologene',
    'structure': {'homologene': None},
}

def load_genedoc(self):
    gene2homologene = HomologeneParser().load()
    return gene2homologene

def get_mapping(self):
    mapping = {
        "homologene": {"dynamic": False,
                       "path": "just_name",
        			   "properties": {
        			   		"genes": {
                                "type": "long",
	                            "index": "no",
                                "include_in_all": False,
        			   		},
        			   		"id": {
        			   			"type": "long",
                                "include_in_all": False,
        			   			"index_name": "homologene",
        			   		}
        			   }
        },
    }
    return mapping
