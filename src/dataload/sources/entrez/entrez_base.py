import os.path
from config import species_li, taxid_d, DATA_ARCHIVE_ROOT
from utils.common import file_newer, loadobj, dump
from dataload import get_data_folder
from utils.dataload import (load_start, load_done,
                            tab2dict, tab2list, value_convert, normalized_value,
                            dict_convert, dict_to_list,
                            )

#DATA_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, 'by_resources/entrez/current')
DATA_FOLDER = get_data_folder('entrez')
print('DATA_FOLDER: ' + DATA_FOLDER)

class EntrezParserBase(object):
    def __init__(self):
        #if species_li is None, include all species
        self.set_species_li(species_li)
        self.DATA_FOLDER = DATA_FOLDER
        self.datafile = os.path.join(self.DATA_FOLDER, self.DATAFILE)

    def set_all_species(self):
        '''To load all species.'''
        self.species_li = None
        self.taxid_set = None
        self.species_filter = None

    def set_species_li(self, species_li):
        '''To load only specied species if species_li is not None.'''
        if species_li:
            self.species_li = species_li
            self.taxid_set = set([taxid_d[species] for species in species_li])
            self.species_filter = lambda ld:int(ld[0]) in self.taxid_set
        else:
            self.set_all_species()

    def load(self, aslist=False):
        raise NotImplementedError


class GeneInfoParser(EntrezParserBase):
    '''Parser for NCBI gene_info.gz file.'''
    DATAFILE = 'gene/gene_info.gz'

    def load(self, aslist=False):
        '''
        loading ncbi "gene_info" file
        This must be called first to create basic gene documents
        with all basic fields, e.g., name, symbol, synonyms, etc.

        format of gene_info file:
        #Format: tax_id GeneID Symbol LocusTag Synonyms dbXrefs chromosome map_location description type_of_gene Symbol_from
        _nomenclature_authority Full_name_from_nomenclature_authority Nomenclature_status Other_designations Modification_da
        te (tab is used as a separator, pound sign - start of a comment)

        '''
        load_start(self.datafile)
        gene_d = tab2dict(self.datafile, (0,1,2,4,5,7,8,9), key=1, alwayslist=0, includefn=self.species_filter)
        def _ff(d):
            (taxid, symbol, synonyms,
            dbxrefs,map_location,
            description, type_of_gene) = d
            out = dict(taxid=int(taxid),
                       symbol = symbol,
                       name=description)
            if map_location != '-':
                out['map_location']=map_location
            if type_of_gene != '-':
                out['type_of_gene']=type_of_gene
            if synonyms != '-':
               out['alias']=normalized_value(synonyms.split('|'))

            for x in dbxrefs.split('|'):
                if x=='-': continue
                try:
                    _db, _id = x.split(':')
                except:
                    print x
                    raise
                if _db.lower() in ['ensembl', 'imgt/gene-db']:      # we don't need ensembl xref from here, we will get it from Ensembl directly
                    continue                                        # we don't need 'IMGT/GENE-DB" xref either, because they are mostly the same as gene symbol
                if _db.lower() == 'mgi':            # add "MGI:" prefix for MGI ids.
                    _id = "MGI:"+_id
                out[_db] = _id
            return out

        gene_d = value_convert(gene_d, _ff)

        #add entrezgene field
        for geneid in gene_d:
            d = gene_d[geneid]
            d['entrezgene'] = int(geneid)
            gene_d[geneid] = d

        load_done('[%d]' % len(gene_d))

        if aslist:
            return dict_to_list(gene_d)
        else:
            return gene_d



def get_geneid_d(species_li=None, load_cache=True, save_cache=True):
    '''return a dictionary of current/retired geneid to current geneid mapping.
       This is useful, when other annotations were mapped to geneids may contain
       retired gene ids.

       if species_li is None, genes from all species are loaded.

       Note that all ids are int type.
    '''
    if species_li:
        taxid_set = set([taxid_d[species] for species in species_li])
    else:
        taxid_set = None

    orig_cwd = os.getcwd()
    os.chdir(DATA_FOLDER)

    #check cache file
    _cache_file = 'gene/geneid_d.pyobj'
    if load_cache and os.path.exists(_cache_file) and \
       file_newer(_cache_file, 'gene/gene_info.gz') and \
       file_newer(_cache_file, 'gene/gene_history.gz'):

        print 'Loading "geneid_d" from cache file...',
        _taxid_set, out_d = loadobj(_cache_file)
        assert _taxid_set == taxid_set
        print 'Done.'
        os.chdir(orig_cwd)
        return out_d

    DATAFILE = os.path.join(DATA_FOLDER, 'gene/gene_info.gz')
    load_start(DATAFILE)
    if species_li:
        species_filter = lambda ld:int(ld[0]) in taxid_set
    else:
        species_filter = None
    geneid_li = set(tab2list(DATAFILE, 1, includefn=species_filter))
    load_done('[%d]' % len(geneid_li))

    DATAFILE = os.path.join(DATA_FOLDER, 'gene/gene_history.gz')
    load_start(DATAFILE)

    if species_li:
        _includefn = lambda ld:int(ld[0]) in taxid_set and ld[1] in geneid_li
    else:
        _includefn = lambda ld:ld[1] in geneid_li    #include all species
    retired2gene = tab2dict(DATAFILE, (1,2), 1, alwayslist=0, includefn=_includefn)
    # includefn above makes sure taxid is for species_li and filters out those mapped_to geneid exists in gene_info list

    load_done('[%d]' % len(retired2gene))

    out_d = dict_convert(retired2gene, keyfn=int, valuefn=int)    # convert key/value to int
    for g in geneid_li:
        _g = int(g)
        out_d[_g] = _g

    if save_cache:
        if species_li:
            dump((taxid_set, out_d), _cache_file)
        else:
            dump((None, out_d), _cache_file)

    os.chdir(orig_cwd)
    return out_d


class HomologeneParser(EntrezParserBase):
    '''Parser for NCBI homologenes.data file.'''
    DATAFILE = 'Homologene/homologene.data'

    def _sorted_homologenes(self, homologenes):
        '''sort list of homologenes [(taxid, geneid),...] based on the order
            defined in species_li.
        '''
        d = {}
        for i, species in enumerate(species_li):
            d[taxid_d[species]] = i
        gene_li = [(d.get(taxid, taxid), taxid, geneid) for taxid, geneid in homologenes]
        return [g[1:] for g in sorted(gene_li)]

    def load(self, aslist=False):
        '''
        loading ncbi "homologene.data" file
        adding "homologene" field in gene doc
        '''

        load_start(self.datafile)
        with file(self.datafile) as df:
            homologene_d = {}
            doc_li = []
            print
            geneid_d = get_geneid_d(self.species_li)

            for line in df:
                ld=line.strip().split('\t')
                hm_id, tax_id, geneid = [int(x) for x in ld[:3]]
                if (self.taxid_set is None or tax_id in self.taxid_set) and geneid in geneid_d:
                    #for selected species only
                    #and also ignore those geneid does not match any existing gene doc
                    geneid = geneid_d[geneid]   # in case of orignal geneid is retired, replaced with the new one, if available.
                    genes = homologene_d.get(hm_id, [])
                    genes.append((tax_id, geneid))
                    homologene_d[hm_id] = genes

                    doc_li.append(dict(_id=str(geneid), taxid=tax_id, homologene={'id': hm_id}) )

            for i, gdoc in enumerate(doc_li):
                gdoc['homologene']['genes']=self._sorted_homologenes(set(homologene_d[gdoc['homologene']['id']]))
                doc_li[i] = gdoc

            load_done('[%d]' % len(doc_li))

        if aslist:
            return doc_li
        else:
            gene_d = dict([(d['_id'], d) for d in doc_li])
            return gene_d

class GeneSummaryParser(EntrezParserBase):
    '''Parser for gene2summary_all.txt, adding "summary" field in gene doc'''
    DATAFILE = 'refseq/gene2summary_all.txt'

    def load(self, aslist=False):
        load_start(self.datafile)
        with file(self.datafile) as df:
            geneid_set = set()
            doc_li = []
            for line in df:
                geneid, summary = line.strip().split('\t')
                if geneid not in geneid_set:
                    doc_li.append(dict(_id=geneid, summary=unicode(summary)))
                    geneid_set.add(geneid)
        load_done('[%d]' % len(doc_li))

        if aslist:
            return doc_li
        else:
            gene_d = dict([(d['_id'], d) for d in doc_li])
            return gene_d


class Gene2AccessionParserBase(EntrezParserBase):
    DATAFILE = 'to_be_specified'
    fieldname = 'to_be_specified'

    def load(self, aslist=False):
        load_start(self.datafile)
        gene2acc = tab2dict(self.datafile, (1,3,5,7), 0, alwayslist=1,
                            includefn=self.species_filter)
        def _ff(d):
            out = {'rna':[],
                   'protein':[],
                   'genomic':[]}
            for x1,x2,x3 in d:
                if x1!='-':
                    out['rna'].append(x1.split('.')[0])   #trim version number after dot
                if x2!='-':
                    out['protein'].append(x2.split('.')[0])
                if x3!='-':
                    out['genomic'].append(x3.split('.')[0])
            #remove dup
            for k in out:
                out[k] = normalized_value(out[k])
            #remove empty rna/protein/genomic field
            _out = {}
            for k,v in out.items():
                if v: _out[k] = v
            if _out:
                _out = {self.fieldname:_out}
            return _out

        gene2acc = dict_convert(gene2acc, valuefn=_ff)
        load_done('[%d]' % len(gene2acc))

        if aslist:
            return dict_to_list(gene2acc)
        else:
            return gene2acc

class Gene2AccessionParser(Gene2AccessionParserBase):
    DATAFILE = 'gene/gene2accession.gz'
    fieldname = 'accession'

class Gene2RefseqParser(Gene2AccessionParserBase):
    DATAFILE = 'gene/gene2refseq.gz'
    fieldname = 'refseq'


class Gene2UnigeneParser(EntrezParserBase):
    DATAFILE = 'gene/gene2unigene'
    def load(self, aslist=False):
        load_start(self.datafile)
        print
        geneid_d = get_geneid_d(self.species_li)
        gene2unigene = tab2dict(self.datafile, (0,1), 0, alwayslist=0,
                                includefn=lambda ld:int(ld[0]) in geneid_d)
        gene_d = {}
        for gid, unigene in gene2unigene.items():
            gene_d[gid] = {'unigene': unigene}
        load_done('[%d]' % len(gene_d))

        if aslist:
            return dict_to_list(gene_d)
        else:
            return gene_d

class Gene2GOParser(EntrezParserBase):
    DATAFILE = 'gene/gene2go.gz'

    def load(self, aslist=False):
        load_start(self.datafile)
        gene2go = tab2dict(self.datafile, (1,2,3,4,5,6,7), 0, alwayslist=1,
                           includefn=self.species_filter)
        category_d = {'Function': 'MF',
                      'Process':  'BP',
                      'Component': 'CC'}

        def _ff(d):
            out = {}
            for goid, evidence, qualifier, goterm, pubmed, gocategory in d:
                _gocategory = category_d[gocategory]
                _d = out.get(_gocategory, [])
                _rec = dict(id=goid, term=goterm)
                if evidence != '-':
                    _rec['evidence'] = evidence
                if qualifier != '-':
                    #here I also fixing some inconsistency issues in NCBI data
                    #Colocalizes_with -> colocalizes_with
                    #Contributes_with -> contributes_with
                    #Not -> NOT
                    _rec['qualifier'] = qualifier.replace('Co', 'co').replace('Not', 'NOT')
                if pubmed != '-':
                    if pubmed.find('|') != -1:
                        pubmed = [int(pid) for pid in pubmed.split('|')]
                    else:
                        pubmed = int(pubmed)
                    _rec['pubmed'] = pubmed
                _d.append(_rec)
                out[_gocategory] = _d
            for k in out:
                if len(out[k]) == 1:
                    out[k] = out[k][0]
            return out

        gene2go = dict_convert(gene2go, valuefn=_ff)
        gene_d = {}
        for gid, go in gene2go.items():
            gene_d[gid] = {'go': go}
        load_done('[%d]' % len(gene_d))

        if aslist:
            return dict_to_list(gene_d)
        else:
            return gene_d


class Gene2RetiredParser(EntrezParserBase):
    '''
    loading ncbi gene_history file, adding "retired" field in gene doc
    '''

    DATAFILE = 'gene/gene_history.gz'
    def load(self, aslist=False):
        load_start(self.datafile)
        if self.species_li:
            _includefn = lambda ld:int(ld[0]) in self.taxid_set and ld[1]!='-'
        else:
            _includefn = lambda ld: ld[1]!='-'
        gene2retired = tab2dict(self.datafile, (1,2), 0, alwayslist=1,
                                includefn=_includefn)
        gene2retired = dict_convert(gene2retired, valuefn=lambda x: normalized_value([int(xx) for xx in x]))

        gene_d = {}
        for gid, retired in gene2retired.items():
            gene_d[gid] = {'retired': retired}
        load_done('[%d]' % len(gene_d))

        if aslist:
            return dict_to_list(gene_d)
        else:
            return gene_d


class Gene2ECParser(EntrezParserBase):
    '''
    loading gene2ec data, adding "ec" field in gene doc

    Sample lines for input file:
        24159   2.3.3.8
        24161   3.1.3.2,3.1.3.48
    '''
    DATAFILE = 'refseq/gene2ec_all.txt'

    def load(self, aslist=False):
        load_start(self.datafile)
        with file(self.datafile) as df:
            geneid_set = set()
            doc_li = []
            for line in df:
                geneid, ec = line.strip().split('\t')
                if ec.find(',') != -1:
                    #there are multiple EC numbers
                    ec = [unicode(x) for x in ec.split(',')]
                else:
                    ec = unicode(ec)
                if geneid not in geneid_set:
                    doc_li.append(dict(_id=geneid, ec=ec))
                    geneid_set.add(geneid)
        load_done('[%d]' % len(doc_li))

        if aslist:
            return doc_li
        else:
            gene_d = dict([(d['_id'], d) for d in doc_li])
            return gene_d


class Gene2GeneRifParser(EntrezParserBase):
    '''
    '''
    DATAFILE = 'generif/generifs_basic.gz'

    def load(self):
        load_start(self.datafile)
        gene2generif = tab2dict(self.datafile, (1, 2, 4), 0, alwayslist=1)
        gene2generif = dict_convert(gene2generif, valuefn=lambda v: {'generif': [dict(pubmed=x[0], text=x[1]) for x in v]})
        load_done('[%d]' % len(gene2generif))
        return gene2generif
