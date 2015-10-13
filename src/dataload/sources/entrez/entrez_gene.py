from mongokit import OR
from entrez_base import GeneInfoParser
from entrez_base import get_geneid_d as _get_geneid_d

structure = {'taxid': int,
             'entrezgene': int,
             'alias': OR(unicode, list)
             }
string_fields = ['name', 'symbol', 'map_location', 'type_of_gene',
                 'HGNC', 'HPRD', 'MIM', 'MGI', 'RATMAP', 'RGD', 'FLYBASE',
                 'WormBase', 'TAIR', 'ZFIN', 'Xenbase']

Extra_fileds = ['APHIDBASE', 'AnimalQTLdb', 'ApiDB_CryptoDB', 'BEEBASE',
                'BEETLEBASE', 'BGD', 'CGNC', 'ECOCYC', 'EcoGene', 'InterPro',
                'MaizeGDB', 'NASONIABASE', 'PBR', 'PFAM', 'Pathema', 'PseudoCap',
                'SGD', 'UniProtKB/Swiss-Prot', 'VBRC', 'VectorBase', 'Vega',
                'dictyBase', 'miRBase']
string_fields += Extra_fileds

for field in string_fields:
    structure[field] = unicode

__metadata__ = {
    '__collection__' : 'entrez_gene',
    'structure': structure,
    'required_fields' : ['taxid', 'entrezgene', 'symbol'],
    'ENTREZ_GENEDOC_ROOT' : True
}

def load_genedoc(self):
    parser = GeneInfoParser()
    parser.set_all_species()
    genedoc_d = parser.load()
    return genedoc_d

def get_mapping(self):
    mapping = {
        "entrezgene": {"type": "long"},
        "taxid":  {"type": "integer",
                   "include_in_all": False},
        "alias":  {"type": "string"},
        "name":   {"type": "string",
                   "boost": 0.8},    #downgrade name field a little bit
        "symbol": {"type": "string",
                   "analyzer": "string_lowercase",
                   "boost": 5.0},

        #do not index map_location and type_of_gene
        "map_location": {"index": "no",
                         "type": "string",
                         "include_in_all": False},
        "type_of_gene": {#"index": "no",
                         "index": "not_analyzed",
                         "type": "string",
                         "include_in_all": False},
        "AnimalQTLdb":  {"index": "no",
                         "type": "string",
                         "include_in_all": False},
        "Vega":         {"index": "no",
                         "type": "string",
                         "include_in_all": False},

        #convert index_name to lower-case, and excluded from "_all"
        "HGNC": {"type": "string",              #1771
                 "index": "not_analyzed",
                 "include_in_all": False,
                 "index_name": 'hgnc'},
        "HPRD": {"type": "string",              #00310
                 "index": "not_analyzed",
                 "include_in_all": False,
                 "index_name": 'hprd'},
        "MIM":  {"type": "string",              #116953
                 "index": "not_analyzed",
                 "include_in_all": False,
                 "index_name": 'mim'},
        "MGI":  {"type": "string",              #MGI:104772
                 "index": "not_analyzed",
                 "include_in_all": False,
                 "index_name": 'mgi'},
        "RATMAP":{"type": "string",
                 "index": "not_analyzed",
                 "include_in_all": False,
                 "index_name": 'ratmap'},
        "RGD":   {"type": "string",             #70486
                 "index": "not_analyzed",
                 "include_in_all": False,
                 "index_name": 'rgd'},
        "FLYBASE":{"type": "string",            #FBgn0004107
                 "analyzer": "string_lowercase",
                 "include_in_all": False,
                 "index_name": 'flybase'},
        "WormBase":  {"type": "string",         #WBGene00000871
                 "analyzer": "string_lowercase",
                 "include_in_all": False,
                 "index_name": 'wormbase'},
        "TAIR":  {"type": "string",             #AT3G48750
                 "analyzer": "string_lowercase",
                 "include_in_all": False,
                 "index_name": 'tair'},
        "ZFIN":  {"type": "string",             #ZDB-GENE-040426-2741
                 "analyzer": "string_lowercase",
                 "include_in_all": False,
                 "index_name": 'zfin'},
        "Xenbase":{"type": "string",
                 "analyzer": "string_lowercase",
                 "include_in_all": False,
                 "index_name": 'xenbase'},

        "miRBase":{"type": "string",
                 "analyzer": "string_lowercase",
                 "include_in_all": True,
                 "index_name": 'mirbase'},


    }
    return mapping


def get_geneid_d(self=None):
    return _get_geneid_d()
