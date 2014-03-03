from pyes import ES
from pyes.exceptions import (NotFoundException, IndexMissingException,
                             ElasticSearchException, TypeMissingException)
from pyes.query import MatchAllQuery
from pyes.utils import make_path
import logging
log = logging.getLogger('pyes')
log.setLevel(logging.DEBUG)
if len(log.handlers) == 0:
    log_handler = logging.StreamHandler()
    log.addHandler(log_handler)

from config import ES_HOST, ES_INDEX_NAME, ES_INDEX_TYPE
from utils.common import ask, timesofar
import json
from utils.mongo import doc_feeder

import sys
import time
import re


def get_es(es_host=None):
    es_host = es_host or ES_HOST
    conn = ES(es_host, default_indices=[],
              bulk_size=5000,
              timeout=6000.0, max_retries=100)
    return conn


def lastexception():
    exc_type, exc_value, tb = sys.exc_info()
    if exc_type is None:
        print "No exception occurs."
        return
    print exc_type.__name__ + ':',
    try:
        excArgs = exc_value.__dict__["args"]
    except KeyError:
        excArgs = ()
    return str(exc_type)+':'+''.join([str(x) for x in excArgs])


class ESIndexer(object):
    def __init__(self, es_index_name=None, es_index_type=None, mapping=None, es_host=None, step=5000):
        self.conn = get_es(es_host)
        self.ES_INDEX_NAME = es_index_name or ES_INDEX_NAME
        self.ES_INDEX_TYPE = es_index_type or ES_INDEX_TYPE
        if self.ES_INDEX_NAME:
            self.conn.default_indices = [self.ES_INDEX_NAME]
        if self.ES_INDEX_TYPE:
            self.conn.default_types = [self.ES_INDEX_TYPE]
        self.step = step
        self.conn.bulk_size = self.step
        self.number_of_shards = 5      # set number_of_shards when create_index
        self.s = None     # optionally, can specify number of records to skip,
                          # useful to continue indexing after an error.
        self.use_parallel = False
        self._mapping = mapping

    def check(self):
        '''print out ES server info for verification.'''
        # print "Servers:", self.conn.servers
        print "Servers:", self.conn.connection._get_server()
        print "Default indices:", self.conn.default_indices
        print "ES_INDEX_TYPE:", self.ES_INDEX_TYPE

    def create_index(self):
        try:
            print self.conn.indices.open_index(self.ES_INDEX_NAME)
        except IndexMissingException:
            print self.conn.indices.create_index(self.ES_INDEX_NAME, settings={
                "number_of_shards": self.number_of_shards,
                "number_of_replicas": 0,    # set this to 0 to boost indexing
                                            # after indexing, set "auto_expand_replicas": "0-all",
                                            #   to make additional replicas.
            })

    def exists_index(self, index):
        return self.conn.indices.exists_index(index)

    def delete_index_type(self, index_type, noconfirm=False):
        '''Delete all indexes for a given index_type.'''
        index_name = self.ES_INDEX_NAME
        # Check if index_type exists
        try:
            self.conn.get_mapping(index_type, index_name)
        except TypeMissingException:
            print 'Error: index type "%s" does not exist in index "%s".' % (index_type, index_name)
            return
        path = '/%s/%s' % (index_name, index_type)
        if noconfirm or ask('Confirm to delete all data under "%s":' % path) == 'Y':
            return self.conn.delete_mapping(index_name, index_type)

    def verify_mapping(self, update_mapping=False):
        '''Verify if index and mapping exist, update mapping if mapping does not exist,
           or "update_mapping=True" explicitly
        '''
        conn = self.conn
        index_name = self.ES_INDEX_NAME
        index_type = self.ES_INDEX_TYPE

        #Test if index exists
        try:
            print "Opening index...", conn.indices.open_index(index_name)
        except NotFoundException:
            print 'Error: index "%s" does not exist. Create it first.' % index_name
            return -1

        try:
            conn.indices.get_mapping(index_type, index_name)
            empty_mapping = False
        except ElasticSearchException:
            #if no existing mapping available for index_type
            #force update_mapping to True
            empty_mapping = True
            update_mapping = True

#        empty_mapping = not cur_mapping[index_name].get(index_type, {})
#        if empty_mapping:
#            #if no existing mapping available for index_type
#            #force update_mapping to True
#            update_mapping = True

        if update_mapping:
            print "Updating mapping...",
            if not empty_mapping:
                print "\n\tRemoving existing mapping...",
                print conn.delete_mapping(index_name, index_type)
            _mapping = self.get_field_mapping()
            print conn.indices.put_mapping(index_type,
                                           _mapping,
                                           [index_name])

    def update_mapping_meta(self, meta):
        index_name = self.ES_INDEX_NAME
        index_type = self.ES_INDEX_TYPE

        if isinstance(meta, dict) and meta.keys() == ['_meta']:
            print self.conn.indices.put_mapping(index_type, meta, [index_name])
        else:
            raise ValueError('Input "meta" should have and only have "_meta" field.')

    def count(self, query=None, index_type=None):
        conn = self.conn
        index_name = self.ES_INDEX_NAME
        index_type = index_type or self.ES_INDEX_TYPE
        if isinstance(query, dict) and 'query' in query:
            _query = query['query']
        else:
            _query = query
        return conn.count(_query, index_name, index_type)

    def get(self, id, **kwargs):
        '''get a specific doc by its id.'''
        conn = self.conn
        index_name = self.ES_INDEX_NAME
        index_type = self.ES_INDEX_TYPE
        return conn.get(index_name, index_type, id, **kwargs)

    def index(self, doc, id=None, bulk=False):
        '''add a doc to the index. If id is not None, the existing doc will be
           updated.
        '''
        return self.conn.index(doc, self.ES_INDEX_NAME, self.ES_INDEX_TYPE, id=id, bulk=bulk)

    def delete_doc(self, index_type, id, bulk=False):
        '''delete a doc from the index based on passed id.'''
        return self.conn.delete(self.ES_INDEX_NAME, index_type, id, bulk=bulk)

    def update(self, id, extra_doc, index_type=None, bulk=False):
        '''update an existing doc with extra_doc.'''
        conn = self.conn
        index_name = self.ES_INDEX_NAME
        index_type = index_type or self.ES_INDEX_TYPE
        # old way, update locally and then push it back.
        # return self.conn.update(extra_doc, self.ES_INDEX_NAME,
        #                         index_type, id)

        if not bulk:
            #using new update api since 0.20
            path = make_path((index_name, index_type, id, '_update'))
            body = {'doc': extra_doc}
            return conn._send_request('POST', path, body=body)
        else:
            # ES supports bulk update since v0.90.1.
            op_type = 'update'
            cmd = {op_type: {"_index": index_name,
                             "_type": index_type,
                             "_id": id}
                   }

            doc = json.dumps({"doc": extra_doc}, cls=conn.encoder)
            command = "%s\n%s" % (json.dumps(cmd, cls=conn.encoder), doc)
            conn.bulker.add(command)
            return conn.flush_bulk()

    def wait_till_all_shards_ready(self, timeout=None, interval=5):
        if timeout:
            t0 = time.time()
        while 1:
            assert self.conn.collect_info(), "collect_info failed. Server error?"
            shards_status = self.conn.info['status']['_shards']
            print shards_status['total'], shards_status['successful']
            if shards_status['total'] == shards_status['successful']:
                # all shards are ready now.
                return True
            time.sleep(interval)
            if timeout:
                assert time.time()-t0 < timeout, 'Time out. Shards are not ready within "{}"s.\n\t{}'.format(timeout, shards_status)

    def optimize(self):
        '''optimize the default index.'''
        return self.conn.indices.optimize(self.ES_INDEX_NAME,
                                          wait_for_merge=False,   # True,
                                          max_num_segments=5)

    def optimize_all(self):
        """optimize all indices"""
        return self.conn.indices.optimize([], wait_for_merge=False,  # True,
                                          max_num_segments=5)

    def get_field_mapping(self):
#        raise NotImplementedError
        return self._mapping

    def build_index(self, collection, update_mapping=False, verbose=False, query=None):
        conn = self.conn
        index_name = self.ES_INDEX_NAME
        #index_type = self.ES_INDEX_TYPE

        self.verify_mapping(update_mapping=update_mapping)
        #update some settings for bulk indexing
        conn.indices.update_settings(index_name, {
            #"refresh_interval": "-1",              # disable refresh temporarily
            # "index.store.compress.stored": True,    # store-level compression    #no need to set it since ES v0.90
            # "index.store.compress.tv": True,        # store-level compression
            "auto_expand_replicas": "0-all",
            #"number_of_replicas": 0,
            "refresh_interval": "30s",

        })
        try:
            print "Building index..."
            if self.use_parallel:
                cnt = self._build_index_parallel(collection, verbose)
            else:
                cnt = self._build_index_sequential(collection, verbose, query=query)
        finally:
            #restore some settings after bulk indexing is done.
            conn.indices.update_settings(index_name, {
                "refresh_interval": "1s",              # default settings
            })

            #time.sleep(60)    #wait
            #conn = get_es()   #need to reconnect after parallel jobs are done.
            #self.conn = conn

            try:
                print conn.indices.flush()
                print conn.indices.refresh()
            except:
                pass

            time.sleep(10)
            print "Validating...",
            target_cnt = collection.find(query).count()
            es_cnt = self.count()['count']
            if target_cnt == es_cnt:
                print "OK [total count={}]".format(target_cnt)
            else:
                print "\nWarning: total count of gene documents does not match [{}, should be {}]".format(es_cnt, target_cnt)

        if cnt:
            print 'Done! - {} docs indexed.'.format(cnt)
            print "Optimizing...", self.optimize()
            # conn.indices.update_settings(index_name, {
            #     "auto_expand_replicas": "0-all",   # expand replicas to all nodes
            # })

    def _build_index_sequential(self, collection, verbose=False, query=None):
        conn = self.conn
        index_name = self.ES_INDEX_NAME
        index_type = self.ES_INDEX_TYPE
        cnt = 0

        def rate_control(cnt, t):
            delay = 0
            if t > 90:
                delay = 30
            elif t > 60:
                delay = 10
            if delay:
                print "\tPausing for {}s...".format(delay),
                time.sleep(delay)
                print "done."

        for doc in doc_feeder(collection, step=self.step, s=self.s, batch_callback=rate_control, query=query):
            # ref: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html#index-replication
            #querystring_args = {'replication': 'async'}
            querystring_args = None
            conn.index(doc, index_name, index_type, doc['_id'], bulk=True,
                       querystring_args=querystring_args)
            cnt += 1
            if verbose:
                print cnt, ':', doc['_id']
        return cnt

    def _build_index_parallel(self, collection, verbose=False):
        from utils.parallel import (run_jobs_on_ipythoncluster,
                                    collection_partition,
                                    require)
        kwargs_common = {'ES_HOST': ES_HOST,
                         'ES_INDEX_NAME': self.ES_INDEX_NAME,
                         'ES_INDEX_TYPE': self.ES_INDEX_TYPE,
                         }
        task_list = []
        for kwargs in collection_partition(collection, step=self.step):
            kwargs.update(kwargs_common)
            task_list.append(kwargs)

        @require('mongokit', 'pyes')
        def worker(kwargs):
            import mongokit
            import pyes
            server = kwargs['server']
            port = kwargs['port']
            src_db = kwargs['src_db']
            src_collection = kwargs['src_collection']
            skip = kwargs['skip']
            limit = kwargs['limit']

            mongo_conn = mongokit.Connection(server, port)
            src = mongo_conn[src_db]

            ES_HOST = kwargs['ES_HOST']
            ES_INDEX_NAME = kwargs['ES_INDEX_NAME']
            ES_INDEX_TYPE = kwargs['ES_INDEX_TYPE'],

            es_conn = pyes.ES(ES_HOST, default_indices=[ES_INDEX_NAME],
                              timeout=120.0, max_retries=10)

            cur = src[src_collection].find(skip=skip, limit=limit, timeout=False)
            cur.batch_size(1000)
            cnt = 0
            try:
                for doc in cur:
                    es_conn.index(doc, ES_INDEX_NAME, ES_INDEX_TYPE, doc['_id'], bulk=True)
                    cnt += 1
            finally:
                cur.close()
            es_conn.indices.flush()   # this is important to avoid missing docs
            es_conn.indices.refresh()
            return cnt

        job_results = run_jobs_on_ipythoncluster(worker, task_list)
        if job_results:
            cnt = sum(job_results)
            return cnt

    def doc_feeder(self, index_type=None, index_name=None, step=10000, verbose=True, query=None, scroll='10m', **kwargs):
        conn = self.conn
        index_name = index_name or self.ES_INDEX_NAME
        index_type = index_type or self.ES_INDEX_TYPE

        q = query if query else MatchAllQuery()
        n = self.count(query=q)['count']
        cnt = 0
        t0 = time.time()
        if verbose:
            print '\ttotal docs: {}'.format(n)
            print '\t1-{}...'.format(step),
            i = step
            t1 = time.time()

        res = conn.search_raw(q, indices=index_name, doc_types=index_type,
                              size=step, scan=True, scroll=scroll, **kwargs)
        #id_li.extend([doc['_id'] for doc in res.hits.hits])
        for doc in res.hits.hits:
            yield doc
            cnt += 1
        if verbose:
            print 'done.[%.1f%%,%s]' % (i*100./n, timesofar(t1))
        while 1:
            if verbose:
                t1 = time.time()
                if i < n:
                    print '\t{}-{}...'.format(i+1, min(i+step, n)),
            res = conn.search_scroll(res._scroll_id, scroll=scroll)
            if len(res.hits.hits) == 0:
                break
            else:
                #id_li.extend([doc['_id'] for doc in res.hits.hits])
                for doc in res.hits.hits:
                    yield doc
                    cnt += 1
                if verbose:
                    i += step
                    print 'done.[%.1f%%,%s]' % (min(i, n)*100./n, timesofar(t1))

        if verbose:
            print "Finished! [{}]".format(timesofar(t0))

        assert cnt == n, "Error: scroll query terminated early, please retry.\nLast response:\n"+str(res)

    def get_id_list(self, index_type=None, index_name=None, step=100000, verbose=True):
        cur = self.doc_feeder(index_type=index_type, index_name=index_name, step=step, fields=[], verbose=verbose)
        id_li = [doc['_id'] for doc in cur]
        return id_li

    def get_id_list_parallel(self, taxid_li, index_type=None, index_name=None, step=1000, verbose=True):
        '''return a list of all doc ids in an index_type.'''
        from utils.parallel import run_jobs_on_ipythoncluster

        def _get_ids_worker(args):
            from utils.es import ESIndexer
            from pyes import MatchAllQuery
            es_kwargs, start, step = args
            q = MatchAllQuery().search()
            q.sort = [{'entrezgene': 'asc'}, {'ensembl.gene': 'asc'}]
            q.fields = []
            q.start = start
            q.size = step
            esi = ESIndexer(**es_kwargs)
            cnt = esi.count()['count']
            res = esi.conn.search_raw(q)
            assert res['hits']['total'] == cnt
            return [doc['_id'] for doc in res['hits']['hits']]

        def _get_ids_worker_by_taxid(args):
            from utils.es import ESIndexer
            from pyes import TermQuery
            es_kwargs, taxid, step = args
            q = TermQuery()
            q.add('taxid', taxid)
            q.fields = []
            q.size = step
            esi = ESIndexer(**es_kwargs)
            res = esi.conn.search(q)
            xli = [doc['_id'] for doc in res]
            assert len(xli) == res.total
            return xli

        es_kwargs = {'es_index_name': self.ES_INDEX_NAME, 'es_host': 'su02:9200'}
        task_li = [(es_kwargs, taxid, step) for taxid in taxid_li]
        #print task_li
        job_results = run_jobs_on_ipythoncluster(_get_ids_worker_by_taxid, task_li)
        return job_results

    #def add_docs(self, docs, step=1000):
    def add_docs(self, docs):
        # n = len(docs)
        # for i in range(0, n, step):
        #     print "\t{}-{}...".format(i, min(n, i+step)),
        #     t1 = time.time()
        #     for doc in docs[i:i+step]:
        #         self.index(doc, bulk=True)
        #     print 'done. [{}]'.format(timesofar(t1))
        for doc in docs:
            self.index(doc, id=doc['_id'], bulk=True)
        self.conn.indices.flush()
        self.conn.indices.refresh()

    def delete_docs(self, ids):
        _q = {
            "ids": {
                "values": ids
            }
        }

        #check count first
        _cnt = self.count(_q)['count']
        assert _cnt == len(ids), "Error: {}!={}. Double check ids for deletion.".format(_cnt, len(ids))

        print self.conn.delete_by_query(self.ES_INDEX_NAME, self.ES_INDEX_TYPE, _q)

    def clone_index(self, src_index, target_index, target_es_host=None, step=10000, scroll='10m',
                    target_index_settings=None, number_of_shards=None):
        '''clone src_index to target_index on the same es_host, or another one given
           by target_es_host.
        '''
        t0 = time.time()
        target_es = self or ESIndexer(es_host=target_es_host)
        if not self.exists_index(src_index):
            print 'Error! src_index "{}" does not exist.'.format(src_index)
            return
        if target_es.exists_index(target_index):
            print 'Error! target_index "{}" already exists.'.format(target_index)
            return
        _idx_settings = self.conn.indices.get_settings(src_index)[src_index]['settings']
        idx_settings = {}
        for k, v in _idx_settings.items():
            if k.startswith('index.'):
                k = k[6:]
                if k not in ['uuid', 'version.created']:
                    idx_settings[k] = v
        if target_index_settings:
            idx_settings.update(target_index_settings)
        if number_of_shards:
            idx_settings['number_of_shards'] = number_of_shards
        idx_settings["refresh_interval"] = "60s"
        print target_es.conn.indices.create_index(target_index, settings=idx_settings)
        idx_mapping = self.conn.indices.get_mapping(indices=src_index, raw=True)
        type_list = idx_mapping[src_index].keys()
        print "Building indexing..."
        cnt = 0
        for _type in type_list:
            print "\ttype:", _type
            print target_es.conn.indices.put_mapping(doc_type=_type, mapping=idx_mapping[src_index][_type], indices=target_index)
            for doc in self.doc_feeder(_type, src_index, step=step, scroll=scroll):
                target_es.conn.index(doc['_source'], target_index, _type, doc['_id'], bulk=True)
                cnt += 1
            print target_es.conn.indices.flush()
            print target_es.conn.indices.refresh()
        target_es.conn.indices.update_settings(target_index, {
            "refresh_interval": "1s",              # default settings
        })
        print 'Done! - {} docs indexed. [{}]'.format(cnt, timesofar(t0))
        print "Optimizing...", target_es.conn.indices.optimize(target_index,
                                                               wait_for_merge=True,
                                                               max_num_segments=5)
        print "Validating...",
        src_cnt = self.conn.count(indices=src_index)['count']
        target_cnt = target_es.conn.count(indices=target_index)['count']
        if src_cnt == target_cnt:
            print "OK [total count={}]".format(target_cnt)
        else:
            print "\nWarning: total count of gene documents does not match [{}, should be {}]".format(target_cnt, src_cnt)


def es_clean_indices(keep_last=2, es_host=None, verbose=True, noconfirm=False, dryrun=False):
    '''clean up es indices, only keep last <keep_last> number of indices.'''
    conn = get_es(es_host)
    index_li = conn.get_indices().keys()

    for prefix in ('genedoc_mygene', 'genedoc_mygene_allspecies'):
        pat = prefix + '_(\d{8})_\w{8}'
        _li = []
        for index in index_li:
            mat = re.match(pat, index)
            if mat:
                _li.append((mat.group(1), index))
        _li.sort()   # older collection appears first
        index_to_remove = [x[1] for x in _li[:-keep_last]]   # keep last # of newer indices
        if len(index_to_remove) > 0:
            print "{} \"{}*\" indices will be removed.".format(len(index_to_remove), prefix)
            if verbose:
                for index in index_to_remove:
                    print '\t', index
            if noconfirm or ask("Continue?") == 'Y':
                for index in index_to_remove:
                    if dryrun:
                        print "dryrun=True, nothing is actually deleted"
                    else:
                        conn.indices.delete_index(index)
                print "Done.[%s indices removed]" % len(index_to_remove)
            else:
                print "Aborted."
        else:
            print "Nothing needs to be removed."


def get_lastest_indices(es_host=None):
    conn = get_es(es_host)
    index_li = conn.get_indices().keys()

    latest_indices = []
    for prefix in ('genedoc_mygene', 'genedoc_mygene_allspecies'):
        pat = prefix + '_(\d{8})_\w{8}'
        _li = []
        for index in index_li:
            mat = re.match(pat, index)
            if mat:
                _li.append((mat.group(1), index))
        latest_indices.append(sorted(_li)[-1])
    if latest_indices[0][0] != latest_indices[1][0]:
        print "Warning: unmatched timestamp:"
        print '\n'.join([x[1] for x in latest_indices])
    latest_indices = [x[1] for x in latest_indices]
    return latest_indices
