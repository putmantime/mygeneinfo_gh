from utils.common import (dump, loadobj, get_timestamp)
from utils.dataload import (tab2list,load_start, load_done)
import urllib
import csv


__metadata__ = {
    '__collection__' : 'entrez_genomic_pos',
    'structure': {'genomic_pos': None}
}

TAXIDS_FILE = "ref_microbe_taxids_20151008.pyobj"  #This is static and needs to be dynamic.
DATAFILE = 'gene2refseq.gz' #How do i point to  this on mygene.info?


def load_genedoc():
    """
    loads gene data from NCBI's refseq2gene.gz file.
    Parses it based on genomic position data and refseq status provided by the list of taxids from get_ref_microbe_taxids()
    as lookup table
    :return:
    """
    taxids = loadobj(TAXIDS_FILE)
    taxid_set = set(taxids)
    load_start(DATAFILE)
    _includefn = lambda ld: ld[0] in taxid_set    # include lines with matching taxid from taxid_set
    cols_included = [0, 1, 7, 9, 10, 11]   # 0-based col idx
    gene2genomic_pos_li = tab2list(DATAFILE, cols_included, header=1, includefn=_includefn)

    for gene in gene2genomic_pos_li:
        if gene[5] == '+':
                strand = "1"
        else:
                strand = "-1"

        mgi_dict = {
                    '_id': gene[1],
                    'genomic_pos': {
                        'start': gene[3],
                        'end': gene[4],
                        'chr': gene[2],
                        'strand': strand
                        }
                    }

        yield mgi_dict

    load_done('[%d]' % len(DATAFILE)) #Not sure what needs to be in the 'len section here.  The original you sent me had
                                      #load_done('[%d]' % len(retired2gene)).  That retired2gene was from where?


def get_mapping():
    mapping = {}
    return mapping


def get_ref_microbe_taxids():
    """
    Download the latest bacterial genome assembly summary from the NCBI genome ftp site
    and generate a list of taxids of the bacterial reference genomes.

    :return:
    """
    assembly = urllib.urlopen("ftp://ftp.ncbi.nlm.nih.gov/genomes/refseq/bacteria/assembly_summary.txt")
    datareader = csv.reader(assembly.read().splitlines(), delimiter="\t")
    taxid = []

    for row in datareader:
        if row[4] == 'reference genome':
            taxid.append(row[5])

    ts = get_timestamp()
    dump(taxid, "ref_microbe_taxids_{}.pyobj".format(ts))

    return taxid

