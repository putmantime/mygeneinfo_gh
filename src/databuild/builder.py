import sys
import os.path
import time
import copy
from datetime import datetime
from utils.mongo import (get_src_db, get_target_db, get_src_master,
                         get_src_build, doc_feeder)
from utils.common import loadobj, timesofar, safewfile, LogPrint, dump2gridfs
from utils.dataload import list2dict, alwayslist
from utils.es import ESIndexer
import databuild.backend
from config import LOG_FOLDER

from multiprocessing import Process, Queue, current_process, freeze_support
NUMBER_OF_PROCESSES = 8


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

    def __init__(self, build_config=None, backend='mongodb'):
        self.src = get_src_db()
        self.step = 10000
        self.use_parallel = False
        self.merge_logging = True     #save output into a logging file when merge is called.
        self.max_build_status = 10    #max no. of records kept in "build" field of src_build collection.

        self.using_ipython_cluster = False
        self.shutdown_ipengines_after_done = False
        self.log_folder = LOG_FOLDER

        self._build_config = build_config
        self._entrez_geneid_d = None
        self._idmapping_d_cache = {}

        self.get_src_master()

        if backend == 'mongodb':
            self.target = databuild.backend.GeneDocMongoDBBackend()
        elif backend == 'es':
            self.target = databuild.backend.GeneDocESBackend(ESIndexer())
        elif backend == 'couchdb':
            from config import COUCHDB_URL
            import couchdb
            self.target = databuild.backend.GeneDocCouchDBBackend(couchdb.Server(COUCHDB_URL))
        elif backend == 'memory':
            self.target = databuild.backend.GeneDocMemeoryBackend()
        else:
            raise ValueError('Invalid backend "%s".' % backend)

    def make_build_config_for_all(self):
        _cfg = {"sources": self.src_master.keys(),
                "gene_root": ['entrez_gene', 'ensembl_gene']}
        self._build_config = _cfg
        return _cfg

    def load_build_config(self, build):
        '''Load build config from src_build collection.'''
        src_build = get_src_build()
        self.src_build = src_build
        _cfg = src_build.find_one({'_id': build})
        if _cfg:
            self._build_config = _cfg
        else:
            raise ValueError('Cannot find build config named "%s"' % build)
        return _cfg

    def log_src_build(self, dict):
        '''put logging dictionary into the corresponding doc in src_build collection.
           if build_config is not loaded from src_build, nothing will be logged.
        '''
        src_build = getattr(self, 'src_build', None)
        if src_build:
            _cfg = src_build.find_one({'_id': self._build_config['_id']})
            _cfg['build'][-1].update(dict)
            src_build.update({'_id': self._build_config['_id']}, {"$set": {'build': _cfg['build']}})

    def log_building_start(self):
        if self.merge_logging:
            #setup logging
            logfile = 'databuild_{}_{}.log'.format('genedoc'+'_'+self._build_config['name'],
                                                   time.strftime('%Y%m%d'))
            log_f, logfile = safewfile(os.path.join(self.log_folder, logfile), prompt=False, default='O')
            sys.stdout = LogPrint(log_f, timestamp=True)

        src_build = getattr(self, 'src_build', None)
        if src_build:
            #src_build.update({'_id': self._build_config['_id']}, {"$unset": {"build": ""}})
            d = {'status': 'building',
                 'started_at': datetime.now(),
                 'logfile': logfile}
            src_build.update({'_id': self._build_config['_id']}, {"$push": {'build': d}})
            _cfg = src_build.find_one({'_id': self._build_config['_id']})
            if len(_cfg['build']) > self.max_build_status:
                #remove the first build status record
                src_build.update({'_id': self._build_config['_id']}, {"$pop": {'build': -1}})


    def prepare_target(self):
        '''call self.update_backend() after validating self._build_config.'''
        if self.target.name == 'mongodb':
            _db = get_target_db()
            self.target.target_collection = _db['genedoc'+'_'+self._build_config['name']]
        elif self.target.name == 'es':
            self.target.target_esidxer.ES_INDEX_NAME = 'genedoc'+'_'+self._build_config['name']
            self.target.target_esidxer._mapping = self.get_mapping()
        elif self.target.name == 'couchdb':
            self.target.db_name = 'genedoc'+'_'+self._build_config['name']
        elif self.target.name == 'memory':
            self.target.target_name = 'genedoc'+'_'+self._build_config['name']

    def get_src_master(self):
        src_master = get_src_master(self.src.connection)
        self.src_master = dict([(src['_id'], src) for src in list(src_master.find())])

    def validate_src_collections(self):
        collection_list = set(self.src.collection_names())
        if self._build_config:
            for src in self._build_config['sources']:
                assert src in self.src_master, '"%s" not found in "src_master"' % src
                assert src in collection_list, '"%s" not an existing collection in "%s"' % (src, self.src.name)
            self.prepare_target()
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

    def _save_idmapping_gridfs(self):
        '''saving _idmapping_d_cache into gridfs.'''
        idmapping_gridfs_d = {}
        if self._idmapping_d_cache:
            for id_type in self._idmapping_d_cache:
                filename = 'tmp_idmapping_d_cache_' + id_type
                dump2gridfs(self._idmapping_d_cache[id_type], filename, self.src)
                idmapping_gridfs_d[id_type] = filename
        return idmapping_gridfs_d

    def make_genedoc_root(self):
        if not self._entrez_geneid_d:
            self._load_entrez_geneid_d()

        if 'ensembl_gene' in self._build_config['gene_root']:
            self._load_ensembl2entrez_li()
            ensembl2entrez = self._idmapping_d_cache['ensembl_gene']

        if "species" in self._build_config:
            _query = {'taxid': {'$in': self._build_config['species']}}
        else:
            _query = None

        geneid_set = []
        if "entrez_gene" in self._build_config['gene_root']:
            for doc_li in doc_feeder(self.src['entrez_gene'], inbatch=True,  step=self.step, query=_query):
                #target_collection.insert(doc_li, manipulate=False, check_keys=False)
                self.target.insert(doc_li)
                geneid_set.extend([doc['_id'] for doc in doc_li])
            cnt_total_entrez_genes = len(geneid_set)
            print '# of entrez Gene IDs in total: %d' % cnt_total_entrez_genes

        if "ensembl_gene" in self._build_config['gene_root']:
            cnt_ensembl_only_genes = 0
            cnt_total_ensembl_genes = 0
            for doc_li in doc_feeder(self.src['ensembl_gene'], inbatch=True, step=self.step, query=_query):
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
                    #target_collection.insert(_doc_li, manipulate=False, check_keys=False)
                    self.target.insert(_doc_li)
            cnt_matching_ensembl_genes = cnt_total_ensembl_genes - cnt_ensembl_only_genes
            print '# of ensembl Gene IDs in total: %d' % cnt_total_ensembl_genes
            print '# of ensembl Gene IDs match entrez Gene IDs: %d' % cnt_matching_ensembl_genes
            print '# of ensembl Gene IDs DO NOT match entrez Gene IDs: %d' % cnt_ensembl_only_genes

            geneid_set = set(geneid_set)
            print '# of total Root Gene IDs: %d' % len(geneid_set)
            self.log_src_build({'stats': {'total_entrez_genes': cnt_total_entrez_genes,
                                          'total_ensembl_genes': cnt_total_ensembl_genes,
                                          'total_ensembl_genes_mapped_to_entrez': cnt_matching_ensembl_genes,
                                          'total_ensembl_only_genes': cnt_ensembl_only_genes,
                                          'total_genes': len(geneid_set)}
                               })
            return  geneid_set

    def get_idmapping_d(self, src):
        if src in self._idmapping_d_cache:
            return self._idmapping_d_cache[src]
        else:
            self._load_ensembl2entrez_li()
            return self._idmapping_d_cache[src]
            #raise ValueError('cannot load "idmapping_d" for "%s"' % src)

    def merge(self, step=100000, restart_at=0):
        t0 = time.time()
        self.validate_src_collections()
        self.log_building_start()
        try:
            if self.using_ipython_cluster:
                self._merge_ipython_cluster(step=step)
            else:
                self._merge_local(step=step, restart_at=restart_at)

            t1 = round(time.time() - t0, 0)
            t = timesofar(t0)
            self.log_src_build({'status': 'success',
                                'time': t,
                                'time_in_s': t1,
                                'timestamp': datetime.now()})

        finally:
            if self.merge_logging:
                sys.stdout.close()

    def _merge_ipython_cluster(self, step=100000):
        '''Do the merging on ipython cluster.'''
        from IPython.parallel import Client, require
        from config import CLUSTER_MONGODB_SERVER, CLUSTER_CLIENT_JSON

        t0 = time.time()
        src_collection_list = [collection for collection in self._build_config['sources']
                                    if collection not in ['entrez_gene', 'ensembl_gene']]

        # self._load_entrez_geneid_d()
        # self._load_ensembl2entrez_li()

        #geneid_set = self.target.get_id_list()
        #print len(geneid_set)

        self.target.drop()
        self.target.prepare()
        geneid_set = self.make_genedoc_root()

        idmapping_gridfs_d = self._save_idmapping_gridfs()

        print timesofar(t0)

        rc = Client(CLUSTER_CLIENT_JSON)
        lview = rc.load_balanced_view()
        print "\t# nodes in use: {}".format(len(lview.targets or rc.ids))
        lview.block = False
        kwargs = {}
        target_collection = self.target.target_collection
        kwargs['server'] = target_collection.database.connection.host
        #kwargs['server'] = CLUSTER_MONGODB_SERVER
        kwargs['port'] = target_collection.database.connection.port
        kwargs['src_db'] = self.src.name
        kwargs['target_db'] = target_collection.database.name
        kwargs['target_collection_name'] = target_collection.name
        kwargs['limit'] = step

        @require('mongokit', 'time', 'types')
        def worker(kwargs):
            server = kwargs['server']
            port = kwargs['port']
            src_db = kwargs['src_db']
            target_db = kwargs['target_db']
            target_collection_name = kwargs['target_collection_name']

            src_collection = kwargs['src_collection']
            skip = kwargs['skip']
            limit = kwargs['limit']

            def load_from_gridfs(filename, db):
                import gzip
                import cPickle as pickle
                import gridfs
                fs = gridfs.GridFS(db)
                fobj = fs.get(filename)
                gzfobj = gzip.GzipFile(fileobj=fobj)
                try:
                    buffer = ""
                    while 1:
                        data = gzfobj.read()
                        if data == "":
                            break
                        buffer += data
                    object = pickle.loads(buffer)
                finally:
                    gzfobj.close()
                    fobj.close()
                return object

            def alwayslist(value):
                if value is None:
                    return []
                if type(value) in (types.ListType, types.TupleType):
                    return value
                else:
                    return [value]

            conn = mongokit.Connection(server, port)
            src = conn[src_db]
            target_collection = conn[target_db][target_collection_name]

            idmapping_gridfs_name = kwargs.get('idmapping_gridfs_name', None)
            if idmapping_gridfs_name:
                idmapping_d = load_from_gridfs(idmapping_gridfs_name, src)
            else:
                idmapping_d = None

            cur = src[src_collection].find(skip=skip, limit=limit, timeout=False)
            cur.batch_size(1000)
            try:
                for doc in cur:
                    _id = doc['_id']
                    if idmapping_d:
                        _id = idmapping_d.get(_id, None) or _id
                    for __id in alwayslist(_id):    #there could be cases that idmapping returns multiple entrez_gene ids.
                        __id = str(__id)
                        doc.pop('_id', None)
                        target_collection.update({'_id': __id}, {'$set': doc},
                                                  manipulate=False,
                                                  upsert=False)
            finally:
                cur.close()

        t0 = time.time()
        task_list = []
        for src_collection in src_collection_list:
            _kwargs = copy.copy(kwargs)
            _kwargs['src_collection'] = src_collection
            id_type = self.src_master[src_collection].get('id_type', None)
            if id_type:
                idmapping_gridfs_name = idmapping_gridfs_d[id_type]
                _kwargs['idmapping_gridfs_name'] = idmapping_gridfs_name
            cnt = self.src[src_collection].count()
            for s in range(0, cnt, step):
                __kwargs = copy.copy(_kwargs)
                __kwargs['skip'] = s
                task_list.append(__kwargs)

        print "\t# of tasks: {}".format(len(task_list))
        print "\tsubmitting...",
        job = lview.map_async(worker, task_list)
        print "done."
        job.wait_interactive()
        print "\t# of results returned: {}".format(len(job.result))
        print "\ttotal time: {}".format(timesofar(t0))

        if self.shutdown_ipengines_after_done:
            print "\tshuting down all ipengine nodes...",
            lview.shutdown()
            print 'Done.'

    def _merge_local(self, step=100000, restart_at=0):
        if restart_at == 0:
            self.target.drop()
            self.target.prepare()
            geneid_set = self.make_genedoc_root()
        else:
            if not self._entrez_geneid_d:
                self._load_entrez_geneid_d()
            #geneid_set = set([x['_id'] for x in target_collection.find(fields=[], manipulate=False)])
            geneid_set = set(self.target.get_id_list())
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
                if self.use_parallel:
                    self.doc_queue = []
                    self._merge_parallel_ipython(collection, geneid_set,
                                         step=step, idmapping_d=idmapping_d)
                else:
                    self._merge_sequential(collection, geneid_set,
                                           step=step, idmapping_d=idmapping_d)
        self.target.finalize()

    def _merge_sequential(self, collection, geneid_set, step=100000, idmapping_d=None):
        for doc in doc_feeder(self.src[collection], step=step):
            _id = doc['_id']
            if idmapping_d:
                _id = idmapping_d.get(_id, None) or _id
            for __id in alwayslist(_id):    #there could be cases that idmapping returns multiple entrez_gene ids.
                __id = str(__id)
                if __id in geneid_set:
                    doc.pop('_id', None)
                    # target_collection.update({'_id': __id}, {'$set': doc},
                    #                           manipulate=False,
                    #                           upsert=False) #,safe=True)
                    self.target.update(__id, doc)

    def _merge_parallel(self, collection, geneid_set, step=100000, idmapping_d=None):

        input_queue = Queue()
        input_queue.conn_pool = []

        def worker(q, target):
            while True:
                doc = q.get()
                if doc == 'STOP':
                    break
                __id = doc.pop('_id')
                target.update(__id, doc)
                # target_collection.update({'_id': __id}, {'$set': doc},
                #                           manipulate=False,
                #                           upsert=False) #,safe=True)

        # Start worker processes
        for i in range(NUMBER_OF_PROCESSES):
            Process(target=worker, args=(input_queue, self.target)).start()


        for doc in doc_feeder(self.src[collection], step=step):
            _id = doc['_id']
            if idmapping_d:
                _id = idmapping_d.get(_id, None) or _id
            for __id in alwayslist(_id):    #there could be cases that idmapping returns multiple entrez_gene ids.
                __id = str(__id)
                if __id in geneid_set:
                    doc['_id'] = __id
                    input_queue.put(doc)

        # Tell child processes to stop
        for i in range(NUMBER_OF_PROCESSES):
            input_queue.put('STOP')

    def _merge_parallel_ipython(self, collection, geneid_set, step=100000, idmapping_d=None):
        from IPython.parallel import Client, require

        rc = Client()
        dview = rc[:]
        #dview = rc.load_balanced_view()
        dview.block = False
        target_collection = self.target.target_collection
        dview['server'] = target_collection.database.connection.host
        dview['port'] = target_collection.database.connection.port
        dview['database'] = target_collection.database.name
        dview['collection_name'] = target_collection.name


        def partition(lst, n):
            q, r = divmod(len(lst), n)
            indices = [q*i + min(i, r) for i in xrange(n+1)]
            return [lst[indices[i]:indices[i+1]] for i in xrange(n)]


        @require('mongokit', 'time')
        def worker(doc_li):
            conn = mongokit.Connection(server, port)
            target_collection = conn[database][collection_name]
            print "len(doc_li): {}".format(len(doc_li))
            t0 = time.time()
            for doc in doc_li:
                __id = doc.pop('_id')
                target_collection.update({'_id': __id}, {'$set': doc},
                                          manipulate=False,
                                          upsert=False) #,safe=True)
            print 'Done. [%.1fs]' % (time.time()-t0)

        for doc in doc_feeder(self.src[collection], step=step):
            _id = doc['_id']
            if idmapping_d:
                _id = idmapping_d.get(_id, None) or _id
            for __id in alwayslist(_id):    #there could be cases that idmapping returns multiple entrez_gene ids.
                __id = str(__id)
                if __id in geneid_set:
                    doc['_id'] = __id
                    self.doc_queue.append(doc)

                    if len(self.doc_queue)>=step:
                        #dview.scatter('doc_li', self.doc_queue)
                        #dview.apply_async(worker)
                        dview.map_async(worker, partition(self.doc_queue, len(rc.ids)))
                        self.doc_queue = []
                        print "!",


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
        #allow source compression
        #Note: no need of source compression due to "Store Level Compression"
        #mapping['_source'] = {'compress': True,}
        #                      'compress_threshold': '1kb'}
        return mapping

    def build_index(self, use_parallel=False):
        target_collection = self.target.target_collection
        es_idxer = ESIndexer(self.get_mapping())
        es_idxer.ES_INDEX_NAME = target_collection.name
        es_idxer.step = 10000
        es_idxer.use_parallel = use_parallel
        #es_idxer.s = 609000
        es_idxer.conn.indices.delete_index(es_idxer.ES_INDEX_NAME)
        es_idxer.create_index()
        es_idxer.delete_index_type(es_idxer.ES_INDEX_TYPE, noconfirm=True)
        #es_idxer.conn.indices.delete_index(es_idxer.ES_INDEX_NAME)
        es_idxer.build_index(target_collection, verbose=False)
        es_idxer.optimize()

    def test2(self):
        collection = 'ensembl_acc'
        step=100000
        self.load_build_config('mygene')
        self._load_entrez_geneid_d()
        self._load_ensembl2entrez_li()
        self.prepare_target()
        geneid_set = set(self.target.get_id_list())
        print len(geneid_set)

        backend = 'couchdb'
        if backend == 'couchdb':
            from config import COUCHDB_URL
            import couchdb
            self.target = databuild.backend.GeneDocCouchDBBackend(couchdb.Server(COUCHDB_URL))
        elif backend == 'memory':
            self.target = databuild.backend.GeneDocMemeoryBackend()
        self.prepare_target()
        self.target.prepare()

        id_type = self.src_master[collection].get('id_type', None)
        flag_need_id_conversion =  id_type is not None
        if flag_need_id_conversion:
            idmapping_d = self._idmapping_d_cache.get(id_type, None)
        else:
            idmapping_d = None


        for doc in doc_feeder(self.src[collection], step=step): #, s=200000, e=300000):
            _id = doc['_id']
            if idmapping_d:
                _id = idmapping_d.get(_id, None) or _id
            for __id in alwayslist(_id):    #there could be cases that idmapping returns multiple entrez_gene ids.
                __id = str(__id)
                if __id in geneid_set:
                    doc.pop('_id', None)
                    self.target.update(__id, doc)


def main():
    t0 = time.time()
    # Build_Config = {
    #     #"name":     "test_parallel_2",
    #     "name":     "test_mongodb",
    #     "sources" : ['entrez_gene', 'ensembl_gene', 'ensembl_acc', 'reporter'], # 'uniprot'],
    #     "gene_root": ['entrez_gene', 'ensembl_gene'],     #either entrez_gene or ensembl_gene or both
    #     "species": [9606, ]
    # }
    # freeze_support()
    # bdr = DataBuilder(build_config=Build_Config, backend='mongodb')
    # #bdr.use_parallel = True
    # bdr.merge()
    # bdr.build_index()

    #freeze_support()
    bdr = DataBuilder(backend='mongodb')
    #bdr = DataBuilder(backend='couchdb')
    #bdr = DataBuilder(backend='memory')
    bdr.load_build_config('mygene')
    #bdr._build_config['sources'] = ['entrez_gene', 'ensembl_gene', 'ensembl_acc']
    bdr.load_build_config('mygene_allspecies')
    #bdr.prepare_target()
    #bdr.use_parallel = True
    #bdr.merge()
    #bdr.build_index()

    print "Finished.", timesofar(t0)

def main1():
    t0 = time.time()
    bdr = DataBuilder(backend='mongodb')
    #bdr.load_build_config('mygene')
    bdr.load_build_config('mygene_allspecies')
    bdr.using_ipython_cluster = True
    #bdr.shutdown_ipengines_after_done = True
    bdr.merge()
    bdr.build_index()
    print "Finished.", timesofar(t0)

def main2():
    t0 = time.time()
    bdr = DataBuilder(backend='mongodb')
    bdr.load_build_config('mygene')
    #bdr.load_build_config('mygene_allspecies')
    bdr.using_ipython_cluster = True
    bdr.prepare_target()
    bdr.build_index(use_parallel=False)
    print "Finished.", timesofar(t0)


if __name__ == '__main__':
    main2()


