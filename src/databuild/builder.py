from utils.mongo import (get_src_db, get_target_db, get_src_master,
                         get_src_build, doc_feeder)
from utils.common import loadobj
from utils.dataload import list2dict, alwayslist
from utils.es import ESIndexer

'''
#Build_Config example

Build_Config = {
    "name":     "test",          #target_collection will be called "genedoc_test"
    "sources" : ['entrez_gene', 'reporter'],
    "gene_root": ['entrez_gene', 'ensembl_gene']     #either entrez_gene or ensembl_gene or both
}

#for genedoc at mygene.info
Build_Config = {
    "name":     "mygene",          #target_collection will be called "genedoc_mygene"
    "sources":  [u'ensembl_acc',
                 u'ensembl_gene',
                 u'ensembl_genomic_pos',
                 u'ensembl_interpro',
                 u'ensembl_prosite',
                 u'entrez_accession',
                 u'entrez_ec',
                 u'entrez_gene',
                 u'entrez_genesummary',
                 u'entrez_go',
                 u'entrez_homologene',
                 u'entrez_refseq',
                 u'entrez_retired',
                 u'entrez_unigene',
                 u'pharmgkb',
                 u'reagent',
                 u'reporter',
                 u'uniprot',
                 u'uniprot_ipi',
                 u'uniprot_pdb',
                 u'uniprot_pir'],
    "gene_root": ['entrez_gene', 'ensembl_gene']
}
'''

class DataBuilder():

    def __init__(self, build_config=None):
        self.src = get_src_db()
        self.target = get_target_db()
        self.step = 10000

        self._build_config = build_config
        self._entrez_geneid_d = None
        self._idmapping_d_cache = {}

        self.get_src_master()

    def make_build_config_for_all(self):
        _cfg = {"sources": self.src_master.keys(),
                "gene_root": ['entrez_gene', 'ensembl_gene']}
        self._build_config = _cfg
        return _cfg

    def load_build_config(self, build):
        '''Load build config from src_build collection.'''
        src_build = get_src_build()
        _cfg = src_build.find_one({'_id': build})
        if _cfg:
            self._build_config = _cfg
        else:
            raise ValueError('Cannot find build config named "%s"' % build)
        return _cfg

    def get_src_master(self):
        src_master = get_src_master(self.src.connection)
        self.src_master = dict([(src['_id'], src) for src in list(src_master.find())])

    def validate_src_collections(self):
        collection_list = set(self.src.collection_names())
        if self._build_config:
            for src in self._build_config['sources']:
                assert src in self.src_master, '"%s" not found in "src_master"' % src
                assert src in collection_list, '"%s" not an existing collection in "%s"' % (src, self.src.name)
        else:
            raise ValueError('"build_config" cannot be empty.')

    def _load_entrez_geneid_d(self):
        self._entrez_geneid_d = loadobj((u"entrez_gene__geneid_d.pyobj", self.src), mode='gridfs')

    def _load_ensembl2entrez_li(self):
        ensembl2entrez_li = loadobj((u"ensembl_gene__2entrezgene_list.pyobj", self.src), mode='gridfs')
        #filter out those deprecated entrez gene ids
        print len(ensembl2entrez_li)
        ensembl2entrez_li = [(ensembl_id, self._entrez_geneid_d[int(entrez_id)]) for (ensembl_id, entrez_id) in ensembl2entrez_li
                                                     if int(entrez_id) in self._entrez_geneid_d]
        print len(ensembl2entrez_li)
        ensembl2entrez = list2dict(ensembl2entrez_li, 0)
        self._idmapping_d_cache['ensembl_gene'] = ensembl2entrez


    def make_genedoc_root(self, target_collection):
        if not self._entrez_geneid_d:
            self._load_entrez_geneid_d()

        if 'ensembl_gene' in self._build_config['gene_root']:
            self._load_ensembl2entrez_li()
            ensembl2entrez = self._idmapping_d_cache['ensembl_gene']

        geneid_set = []
        if "entrez_gene" in self._build_config['gene_root']:
            for doc_li in doc_feeder(self.src['entrez_gene'], inbatch=True,  step=self.step):
                target_collection.insert(doc_li, manipulate=False, check_keys=False)
                geneid_set.extend([doc['_id'] for doc in doc_li])
            cnt_total_entrez_genes = len(geneid_set)
            print '# of entrez Gene IDs in total: %d' % cnt_total_entrez_genes

        if "ensembl_gene" in self._build_config['gene_root']:
            cnt_ensembl_only_genes = 0
            cnt_total_ensembl_genes = 0
            for doc_li in doc_feeder(self.src['ensembl_gene'], inbatch=True, step=self.step):
                _doc_li = []
                for _doc in doc_li:
                    cnt_total_ensembl_genes += 1
                    ensembl_id = _doc['_id']
                    entrez_gene = ensembl2entrez.get(ensembl_id, None)
                    if entrez_gene is None:
                        #this is an Ensembl only gene
                        _doc_li.append(_doc)
                        cnt_ensembl_only_genes += 1
                        geneid_set.append(_doc['_id'])
                if _doc_li:
                    target_collection.insert(_doc_li, manipulate=False, check_keys=False)
            print '# of ensembl Gene IDs in total: %d' % cnt_total_ensembl_genes
            print '# of ensembl Gene IDs match entrez Gene IDs: %d' % len(ensembl2entrez)
            print '# of ensembl Gene IDs DO NOT match entrez Gene IDs: %d' % cnt_ensembl_only_genes

            geneid_set = set(geneid_set)
            print '# of total Root Gene IDs: %d' % len(geneid_set)
            return  geneid_set

    def get_idmapping_d(self, src):
        if src in self._idmapping_d_cache:
            return self._idmapping_d_cache[src]
        else:
            self._load_ensembl2entrez_li()
            return self._idmapping_d_cache[src]
            #raise ValueError('cannot load "idmapping_d" for "%s"' % src)

    def merge(self, step=100000, restart_at=0):
        self.validate_src_collections()
        target_collection = self.target['genedoc'+'_'+self._build_config['name']]
        if restart_at == 0:
            target_collection.drop()
            geneid_set = self.make_genedoc_root(target_collection)
        else:
            if not self._entrez_geneid_d:
                self._load_entrez_geneid_d()
            geneid_set = set([x['_id'] for x in target_collection.find(fields=[], manipulate=False)])
            print '\t', len(geneid_set)

        src_collection_list = self._build_config['sources']
        src_cnt = 0
        for collection in src_collection_list:
            if collection in ['entrez_gene', 'ensembl_gene']:
                continue

            src_cnt += 1

            id_type = self.src_master[collection].get('id_type', None)
            flag_need_id_conversion =  id_type is not None
            if flag_need_id_conversion:
                idmapping_d = self.get_idmapping_d(id_type)
            else:
                idmapping_d = None

            if restart_at <= src_cnt:
                for doc in doc_feeder(self.src[collection], step=step):
                    _id = doc['_id']
                    if flag_need_id_conversion:
                        _id = idmapping_d.get(_id, None) or _id
                    for __id in alwayslist(_id):    #there could be cases that idmapping returns multiple entrez_gene ids.
                        __id = str(__id)
                        if __id in geneid_set:
                            doc.pop('_id', None)
                            target_collection.update({'_id': __id}, {'$set': doc},
                                                      manipulate=False,
                                                      upsert=False) #,safe=True)

                            # _doc = target_collection.get_from_id(__id)
                            # if _doc:
                            #     _doc.update(doc)
                            #     doc = _doc
                            # target_collection.save(doc, manipulate=False, check_keys=False, w=0)

    def merge_0(self):
        target_collection = self.target.genedoc
        for collection in self.src_collection_list:
            for doc in self.src[collection].find():
                _doc = target_collection.get_from_id(doc['_id'])
                if not _doc:
                    _doc = doc
                else:
                    _doc.update(doc)
                target_collection.save(_doc, safe=True)
                print collection, doc['_id']

    def merge1(self, step=10000):
        target_collection = self.target.genedoc
        target_collection.drop()
        for collection in self.src_collection_list:
            for doc in doc_feeder(self.src[collection], step=step):
                _doc = target_collection.get_from_id(doc['_id'])
                if _doc:
                    _doc.update(doc)
                    doc = _doc
                target_collection.save(doc, safe=True)
                print collection, doc['_id']

    def get_mapping(self):
        mapping = {}
        src_master = get_src_master(self.src.connection)
        for collection in self._build_config['sources']:
            meta = src_master.get_from_id(collection)
            if meta.has_key('mapping'):
                mapping.update(meta['mapping'])
            else:
                print 'Warning: "%s" collection has no mapping data.' % collection
        mapping = {"properties": mapping,
                   "dynamic": False}
        return mapping

    def build_index(self):
        es_idxer = ESIndexer(self.target.genedoc2, self.get_mapping())
        es_idxer.step = 1000
        es_idxer.create_index()
        es_idxer.build_index(verbose=False)
