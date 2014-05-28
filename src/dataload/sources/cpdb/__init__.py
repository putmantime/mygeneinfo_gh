from cpdb_base import load_cpdb

__metadata__ = {
    '__collection__' : 'pharmgkb',
    '__url__': 'http://cpdb.molgen.mpg.de/CPDB/getPathwayGenes?idtype=entrez-gene'
}


def load_genedoc(self=None):
    return load_cpdb()


def get_mapping(self=None):
    mapping = {}
    return mapping
