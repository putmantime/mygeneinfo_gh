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
from utils.common import ask
import json
from utils.mongo import doc_feeder

import sys
import time


def get_es():
    conn = ES(ES_HOST, default_indices=[ES_INDEX_NAME],
              bulk_size=10000,
              timeout=600.0, max_retries=10)
    return conn


def lastexception():
    exc_type,exc_value,tb = sys.exc_info()
    if exc_type is None:
        print "No exception occurs."
        return
    print exc_type.__name__ + ':' ,
    try:
        excArgs = exc_value.__dict__["args"]
    except KeyError:
        excArgs = ()
    return str(exc_type)+':'+''.join([str(x) for x in excArgs])

class ESIndexer(object):
    def __init__(self, es_index_name=None, es_index_type=None, mapping=None):
        self.conn = get_es()
        self.ES_INDEX_NAME = es_index_name or ES_INDEX_NAME
        self.ES_INDEX_TYPE = es_index_type or ES_INDEX_TYPE
        self.step = 10000
        self.s = None     #optionally, can specify number of records to skip,
                          #useful to continue indexing after an error.
        self.use_parallel = False
        self._mapping = mapping

    def check(self):
        '''print out ES server info for verification.'''
        #print "Servers:", self.conn.servers
        print "Servers:", self.conn.connection._get_server()
        print "Default indices:", self.conn.default_indices
        print "ES_INDEX_TYPE:", self.ES_INDEX_TYPE

    def create_index(self):
        try:
            print self.conn.open_index(self.ES_INDEX_NAME)
        except IndexMissingException:
            print self.conn.create_index(self.ES_INDEX_NAME)

    def delete_index_type(self, index_type, noconfirm=False):
        '''Delete all indexes for a given index_type.'''
        index_name = self.ES_INDEX_NAME
        #Check if index_type exists
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
            print "Opening index...", conn.open_index(index_name)
        except NotFoundException:
            print 'Error: index "%s" does not exist. Create it first.' % index_name
            return -1

        try:
            conn.get_mapping(index_type, index_name)
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
            print conn.put_mapping(index_type,
                                   _mapping,
                                   [index_name])
    def count(self, query=None, index_type=None):
        conn = self.conn
        index_name = self.ES_INDEX_NAME
        index_type = index_type or self.ES_INDEX_TYPE
        return conn.count(query, index_name, index_type)

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

    def delete_doc(self, index_type, id):
        '''delete a doc from the index based on passed id.'''
        return self.conn.delete(self.ES_INDEX_NAME, index_type, id)

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

    def optimize(self):
        '''optimize the default index.'''
        return self.conn.indices.optimize(self.ES_INDEX_NAME,
                                          wait_for_merge=False, # True,
                                          max_num_segments=5)

    def optimize_all(self):
        """optimize all indices"""
        return self.conn.indices.optimize([], wait_for_merge=False, #True,
                                              max_num_segments=5)

    def get_field_mapping(self):
#        raise NotImplementedError
        return self._mapping

    def build_index(self, collection, update_mapping=False, verbose=False):
        conn = self.conn
        index_name = self.ES_INDEX_NAME
        #index_type = self.ES_INDEX_TYPE

        self.verify_mapping(update_mapping=update_mapping)
        #update some settings for bulk indexing
        conn.indices.update_settings(index_name,
            {
                "refresh_interval" : "-1",              #disable refresh temporarily
                # "index.store.compress.stored": True,    #store-level compression    #no need to set it since ES v0.90
                #"index.store.compress.tv": True,        #store-level compression
                "auto_expand_replicas": "0-all",
            })
        try:
            print "Building index..."
            if self.use_parallel:
                cnt = self._build_index_parallel(collection, verbose)
            else:
                cnt = self._build_index_sequential(collection, verbose)
        finally:
            #restore some settings after bulk indexing is done.
            conn.indices.update_settings(index_name,
                {
                    "refresh_interval" : "1s",              #default settings
                })

            #time.sleep(60)    #wait
            #conn = get_es()   #need to reconnect after parallel jobs are done.
            #self.conn = conn

            try:
                print conn.indices.flush()
                print conn.indices.refresh()
            except:
                pass
        if cnt:
            print 'Done! - {} docs indexed.'.format(cnt)

    def _build_index_sequential(self, collection, verbose=False):
        conn = self.conn
        index_name = self.ES_INDEX_NAME
        index_type = self.ES_INDEX_TYPE
        cnt = 0
        for doc in doc_feeder(collection, step=self.step, s=self.s):
            conn.index(doc, index_name, index_type, doc['_id'], bulk=True)
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
            import mongokit, pyes
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
            es_conn.flush()   #this is important to avoid missing docs
            es_conn.refresh()
            return cnt

        job_results = run_jobs_on_ipythoncluster(worker, task_list)
        if job_results:
            cnt = sum(job_results)
            return cnt

    def get_id_list(self, index_type=None, index_name=None, step=100000, verbose=True):
        '''return a list of all doc ids in an index_type.'''
        conn = self.conn
        index_name = index_name or self.ES_INDEX_NAME
        index_type = index_type or self.ES_INDEX_TYPE

        id_li = []
        q = MatchAllQuery()
        if verbose:
            import time
            from utils.common import timesofar

            n = self.count()['count']
            print '\ttotal docs: {}'.format(n)
            print '\t1-{}...'.format(step),
            i = step
            t1 = time.time()

        res = conn.search_raw(q, indices=index_name, doc_types=index_type,
                              size=step, scan=True, scroll='10m', fields=[])
        id_li.extend([doc['_id'] for doc in res.hits.hits])

        if verbose:
            print 'done.[%.1f%%,%s]' % (i*100./n, timesofar(t1))
        while 1:
            if verbose:
                t1 = time.time()
                if i < n:
                    print '\t{}-{}...'.format(i+1, min(i+step, n)),
            res = conn.search_scroll(res._scroll_id, scroll='10m')
            if len(res.hits.hits) == 0:
                break
            else:
                id_li.extend([doc['_id'] for doc in res.hits.hits])
                if verbose:
                    i += step
                    print 'done.[%.1f%%,%s]' % (min(i, n)*100./n, timesofar(t1))

        assert len(id_li) == n, "Error: scroll query terminated early, please retry.\nLast response:\n"+str(res)
        return id_li

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
        self.conn.flush()
        self.conn.refresh()

    def delete_docs(self, ids):
        _q = {
            "ids" : {
                "values" : ids
            }
        }

        #check count first
        _cnt = self.count(_q)['count']
        assert _cnt==len(ids), "Error: {}!={}. Double check ids for deletion.".format(_cnt, len(ids))

        print self.conn.delete_by_query(self.ES_INDEX_NAME, self.ES_INDEX_TYPE, _q)






