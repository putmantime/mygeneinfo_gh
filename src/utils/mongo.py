import time
from mongokit import Connection
from config import (DATA_SRC_SERVER, DATA_SRC_PORT, DATA_SRC_DATABASE,
                    DATA_SRC_MASTER_COLLECTION,DATA_SRC_DUMP_COLLECTION,
                    DATA_SRC_BUILD_COLLECTION,
                    DATA_TARGET_SERVER, DATA_TARGET_PORT, DATA_TARGET_DATABASE,
                    DATA_TARGET_MASTER_COLLECTION)
from utils.common import timesofar

def get_conn(server, port):
    conn = Connection(server, port)
    return conn

def get_src_conn():
    return get_conn(DATA_SRC_SERVER, DATA_SRC_PORT)

def get_src_db(conn=None):
    conn = conn or get_src_conn()
    return conn[DATA_SRC_DATABASE]

def get_src_master(conn=None):
    conn = conn or get_src_conn()
    return conn[DATA_SRC_DATABASE][DATA_SRC_MASTER_COLLECTION]

def get_src_dump(conn=None):
    conn = conn or get_src_conn()
    return conn[DATA_SRC_DATABASE][DATA_SRC_DUMP_COLLECTION]

def get_src_build(conn=None):
    conn = conn or get_src_conn()
    return conn[DATA_SRC_DATABASE][DATA_SRC_BUILD_COLLECTION]

def get_target_conn():
    return get_conn(DATA_TARGET_SERVER, DATA_TARGET_PORT)

def get_target_db(conn=None):
    conn = conn or get_src_conn()
    return conn[DATA_TARGET_DATABASE]

def get_target_master(conn=None):
    conn = conn or get_target_conn()
    return conn[DATA_TARGET_DATABASE][DATA_TARGET_MASTER_COLLECTION]

def doc_feeder0(collection, step=1000, s=None, e=None, inbatch=False):
    '''A iterator for returning docs in a collection, with batch query.'''
    n = collection.count()
    s = s or 1
    e = e or n
    print 'Found %d documents in database "%s".' % (n, collection.name)
    for i in range(s-1, e+1, step):
        print "Processing %d-%d documents..." % (i+1, i+step) ,
        t0=time.time()
        res = collection.find(skip=i, limit=step, timeout=False)
        if inbatch:
            yield res
        else:
            for doc in res:
                yield doc
        print 'Done.[%s]' % timesofar(t0)

def doc_feeder(collection, step=1000, s=None, e=None, inbatch=False, query=None):
    '''A iterator for returning docs in a collection, with batch query.
       additional filter query can be passed via "query", e.g.,
       doc_feeder(collection, query={'taxid': {'$in': [9606, 10090, 10116]}})
    '''
    cur = collection.find(query, timeout=False)
    n = cur.count()
    s = s or 0
    e = e or n
    print 'Retrieving %d documents from database "%s".' % (n, collection.name)
    t0=time.time()
    if inbatch:
        doc_li = []
    cnt = 0
    t1 = time.time()
    try:
        if s:
            cur.skip(s)
            cnt = s
            print "Skipping %d documents." % s
        if e:
            cur.limit(e - (s or 0))
        cur.batch_size(step)
        print "Processing %d-%d documents..." % (cnt+1, min(cnt+step, e)) ,
        for doc in cur:
            if inbatch:
                doc_li.append(doc)
            else:
                yield doc
            cnt += 1
            if cnt % step == 0:
                if inbatch:
                    yield doc_li
                    doc_li = []
                print 'Done.[%.1f%%,%s]' % (cnt*100./n, timesofar(t1))
                if cnt < e:
                    t1 = time.time()
                    print "Processing %d-%d documents..." % (cnt+1, min(cnt+step, e)) ,
        if inbatch and doc_li:
            #Important: need to yield the last batch here
            yield doc_li

        #print 'Done.[%s]' % timesofar(t1)
        print 'Done.[%.1f%%,%s]' % (cnt*100./n, timesofar(t1))
        print "="*20
        print 'Finished.[total time: %s]' % timesofar(t0)
    finally:
        cur.close()

def src_clean_archives(keep_last=1, src=None, verbose=True, noconfirm=False):
    '''clean up archive collections in src db, only keep last <kepp_last>
       number of archive.
    '''
    from utils.dataload import list2dict
    from utils.common import ask

    src = src or get_src_db()

    archive_li = sorted([(coll.split('_archive_')[0], coll) for coll in src.collection_names() \
                                                    if coll.find('archive')!=-1])
    archive_d = list2dict(archive_li, 0, alwayslist=1)
    coll_to_remove = []
    for k,v in archive_d.items():
        print k,
        #check current collection exists
        if src[k].count() > 0:
            cnt = 0
            for coll in sorted(v)[:-keep_last]:
                coll_to_remove.append(coll)
                cnt += 1
            print "\t\t%s archived collections marked to remove." % cnt
        else:
            print 'skipped. Missing current "%s" collection!' % k
    if len(coll_to_remove)>0:
        print "%d archived collections will be removed." % len(coll_to_remove)
        if verbose:
            for coll in coll_to_remove:
                print '\t', coll
        if noconfirm or ask("Continue?") == 'Y':
            for coll in coll_to_remove:
                src[coll].drop()
            print "Done.[%s collections removed]" % len(coll_to_remove)
        else:
            print "Aborted."
    else:
        print "Nothing needs to be removed."
