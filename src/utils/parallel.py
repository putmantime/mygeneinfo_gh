'''
Utils for running parallel jobs on IPython cluster.
'''
import time
import types
import copy
from IPython.parallel import Client, require

from config import CLUSTER_CLIENT_JSON
from utils.common import timesofar

def run_jobs_on_ipythoncluster(worker, task_list, shutdown_ipengines_after_done=False):

    t0 = time.time()
    rc = Client(CLUSTER_CLIENT_JSON)
    lview = rc.load_balanced_view()
    print "\t# nodes in use: {}".format(len(lview.targets or rc.ids))
    lview.block = False

    print "\t# of tasks: {}".format(len(task_list))
    print "\tsubmitting...",
    job = lview.map_async(worker, task_list)
    print "done."
    job.wait_interactive()
    if len(job.result) != len(task_list):
        print "WARNING:\t# of results returned ({}) != # of tasks ({}).".format(len(job.result), len(task_list))
    print "\ttotal time: {}".format(timesofar(t0))

    if shutdown_ipengines_after_done:
        print "\tshuting down all ipengine nodes...",
        lview.shutdown()
        print 'Done.'
    return job.result

def collection_partition(src_collection_list, step=100000):
    if src_collection_list not in (types.ListType, types.TupleType):
        src_collection_list = [src_collection_list]

    kwargs = {}
    kwargs['limit'] = step
    for src_collection in src_collection_list:
        _kwargs = copy.copy(kwargs)
        _kwargs['src_collection'] = src_collection.name
        _kwargs['src_db'] = src_collection.database.name
        _kwargs['server'] = src_collection.database.connection.host
        _kwargs['port'] = src_collection.database.connection.port

        cnt = src_collection.count()
        for s in range(0, cnt, step):
            __kwargs = copy.copy(_kwargs)
            __kwargs['skip'] = s
            yield __kwargs
