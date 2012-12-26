import time
from mongokit import Connection
from config import (DATA_SRC_SERVER, DATA_SRC_PORT, DATA_SRC_DATABASE,
                    DATA_SRC_MASTER_COLLECTION,DATA_SRC_DUMP_COLLECTION,
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

def doc_feeder(collection, step=1000, s=None, e=None, inbatch=False):
    '''A iterator for returning docs in a collection, with batch query.'''

    n = collection.count()
    s = s or 0
    e = e or n
    print 'Found %d documents in database "%s".' % (n, collection.name)
    t0=time.time()
    if inbatch:
        doc_li = []
    cnt = 0
    t1 = time.time()
    print "Processing %d-%d documents..." % (cnt+1, cnt+step) ,
    try:
        cur = collection.find(timeout=True)
        if s: cur.skip(s)
        if e: cur.limit(e)
        cur.batch_size(step)
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
                print 'Done.[%s]' % timesofar(t1)
                t1 = time.time()
                print "Processing %d-%d documents..." % (cnt+1, cnt+step) ,
        if inbatch and doc_li:
            #Important: need to yield the last batch here
            yield doc_li

        print 'Done.[%s]' % timesofar(t1)
        print "="*20
        print 'Finished.[total time: %s]' % timesofar(t0)
    finally:
        cur.close()
