from uniprot_base import load_uniprot

#UniProt id mapping source from UniProt directly
#ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/idmapping_selected.tab

__metadata__ = {
    '__collection__': 'uniprot',
    'structure': {'uniprot': None},
}


def load_genedoc(self=None):
    genedoc_d = load_uniprot()
    return genedoc_d


def get_mapping(self=None):
    mapping = {
        "uniprot": {
            "dynamic": False,
            "path": "just_name",
            "properties": {
                "Swiss-Prot": {
                    "type": "string",
                    "analyzer": "string_lowercase",
                    "index_name": "uniprot",
                },
                "TrEMBL": {
                    "type": "string",
                    "analyzer": "string_lowercase",
                    "index_name": "uniprot",
                }
            }
        }
    }
    return mapping
