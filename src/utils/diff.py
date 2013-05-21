'''
Utils to compare two list of gene documents
'''
import time
import os.path
from databuild.backend import (GeneDocMongoDBBackend,
                               GeneDocCouchDBBackend)
from utils.common import timesofar

def diff_doc(doc_1, doc_2, exclude_attrs=[]):
    diff_d = {'update': {},
              'delete': [],
              'add': {}}
    for attr in set(doc_1) | set(doc_2):
        if exclude_attrs and attr in exclude_attrs:
            continue
        if attr in doc_1 and attr in doc_2:
            _v1 = doc_1[attr]
            _v2 = doc_2[attr]
            if _v1 != _v2:
                diff_d['update'][attr] = _v2
        elif attr in doc_1 and attr not in doc_2:
            diff_d['delete'].append(attr)
        else:
            diff_d['add'][attr] = doc_2[attr]
    if diff_d['update'] or diff_d['delete'] or diff_d['add']:
        return diff_d

def two_docs_iterator(b1, b2, id_list, step = 10000):
    t0 = time.time()
    n = len(id_list)
    for i in range(0, n, step):
        t1 = time.time()
        print "Processing %d-%d documents..." % (i+1, min(i+step, n)) ,
        _ids = id_list[i:i+step]
        iter1 = b1.mget_from_ids(_ids, asiter=True)
        iter2 = b2.mget_from_ids(_ids, asiter=True)
        for doc1, doc2 in zip(iter1, iter2):
            yield doc1, doc2
        print 'Done.[%.1f%%,%s]' % (i*100./n, timesofar(t1))
    print "="*20
    print 'Finished.[total time: %s]' % timesofar(t0)


def _diff_doc_worker(args):
    b1_target_collection, b2_es_index, ids, _path = args
    import sys
    if _path not in sys.path:
        sys.path.append(_path)
    from databuild import backend
    from utils.mongo import get_target_db
    from utils.es import ESIndexer
    import utils.diff
    reload(utils.diff)
    from utils.diff import _diff_doc_inner_worker

    target = get_target_db()
    b1 = backend.GeneDocMongoDBBackend(target[b1_target_collection])
    es_idxer = ESIndexer()
    es_idxer.ES_INDEX_NAME = b2_es_index
    b2 = backend.GeneDocESBackend(es_idxer)
    _updates = _diff_doc_inner_worker(b1, b2, ids)
    return _updates


def _diff_doc_inner_worker(b1, b2, ids):
    _updates = []
    for doc1, doc2 in two_docs_iterator(b1, b2, ids):
        assert doc1['_id'] == doc2['_id']
        _diff = diff_doc(doc1, doc2)
        if _diff:
            _diff['_id'] = doc1['_id']
            _updates.append(_diff)
    return _updates

def diff_collections(b1, b2, use_parallel=True, step=10000):
    """
    b1, b2 are one of supported backend class in databuild.backend.
    e.g.,
        b1 = GeneDocMongoDBBackend(c1)
        b2 = GeneDocMongoDBBackend(c2)
    """

    id_s1 = set(b1.get_id_list())
    id_s2 = set(b2.get_id_list())
    print "Size of collection 1:\t", len(id_s1)
    print "Size of collection 2:\t", len(id_s2)

    id_in_1 = id_s1 - id_s2
    id_in_2 = id_s2 - id_s1
    id_common = id_s1 & id_s2
    print "# of docs found only in collection 1:\t", len(id_in_1)
    print "# of docs found only in collection 2:\t", len(id_in_2)
    print "# of docs found in both collections:\t", len(id_common)

    print "Comparing matching docs..."
    if len(id_common) > 0:
        if not use_parallel:
            _updates = _diff_doc_inner_worker(b1, b2, list(id_common))
        else:
            from utils.parallel import run_jobs_on_ipythoncluster
            _path = os.path.split(os.path.split(os.path.abspath(__file__))[0])[0]
            id_common = list(id_common)
            b1_target_collection = b1.target_collection.name
            b2_es_index = b2.target_esidxer.ES_INDEX_NAME
            task_li = [(b1_target_collection, b2_es_index, id_common[i:i+step],_path) for i in range(0, len(id_common), step)]
            job_results = run_jobs_on_ipythoncluster(_diff_doc_worker, task_li)
            _updates = []
            if job_results:
                for res in job_results:
                    _updates.extend(res)
            else:
                print "Parallel jobs failed or were interrupted."
                return None

        print "Done. [{} docs changed]".format(len(_updates))

    _deletes = []
    if len(id_in_1) > 0:
        _deletes = sorted(id_in_1)

    _adds = []
    if len(id_in_2) > 0:
        _adds = sorted(id_in_2)

    changes = {'update': _updates,
               'delete': _deletes,
               'add': _adds}
    return changes


def worker(args):
    a, _path = args
    import sys
    if _path not in sys.path:
        sys.path.append(_path)

    from utils.diff import two_docs_iterator
    from databuild import backend
    import utils.diff
    reload(utils.diff)
    from utils.diff import _diff_doc_inner_worker
    return dir(utils.diff)
