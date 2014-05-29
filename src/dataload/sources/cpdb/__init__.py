from cpdb_base import load_cpdb

__metadata__ = {
    '__collection__' : 'cpdb',
    '__url_human__': 'http://cpdb.molgen.mpg.de/CPDB/getPathwayGenes?idtype=entrez-gene',
    '__url_mouse__': 'http://cpdb.molgen.mpg.de/MCPDB/getPathwayGenes?idtype=entrez-gene',
    '__url_yeast__': 'http://cpdb.molgen.mpg.de/YCPDB/getPathwayGenes?idtype=entrez-gene'
}


def load_genedoc(self=None):
    return load_cpdb()


def get_mapping(self=None):
    mapping = {}
    return mapping
