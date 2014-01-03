from __future__ import print_function
import re
from datetime import datetime
import time

from utils.mongo import get_target_db, doc_feeder
from .backend import GeneDocMongoDBBackend
from utils.diff import diff_collections
from utils.common import iter_n, timesofar


class GeneDocSyncer:
    def __init__(self, build_config='genedoc_mygene'):
        self.build_config = build_config
        self._db = get_target_db()
        self._target_col = self._db[self.build_config+'_current']
        self.step = 10000

    def get_source_list(self):
        '''return a list of available source collections.'''
        pat = self.build_config + '_(\d{8})_\w{8}'
        _li = []
        for coll_name in self._db.collection_names():
            mat = re.match(pat, coll_name)
            if mat:
                _li.append(coll_name)
        return sorted(_li)

    def get_latest_source_col(self, n=-1):
        return self._db[self.get_source_list()[n]]

    def get_changes(self, source_col, use_parallel=True):
        target_col = self._target_col
        source_col = self._db[source_col] if isinstance(source_col, basestring) else source_col

        src = GeneDocMongoDBBackend(source_col)
        target = GeneDocMongoDBBackend(target_col)
        changes = diff_collections(target, src, use_parallel=use_parallel, step=self.step)
        if changes:
            changes['source'] = source_col.name
            changes['timestamp'] = _get_timestamp(source_col.name)
        return changes

    def apply_changes(self, changes):
        step = self.step
        target_col = self._target_col
        source_col = self._db[changes['source']]
        src = GeneDocMongoDBBackend(source_col)
        target = GeneDocMongoDBBackend(target_col)
        _timestamp = changes['timestamp']

        t0 = time.time()
        if changes['add']:
            print("Adding {} new docs...".format(len(changes['add'])), end='')
            t00 = time.time()
            for _ids in iter_n(changes['add'], step):
                _doc_li = src.mget_from_ids(_ids)
                for _doc in _doc_li:
                    _doc['_timestamp'] = _timestamp
                target.insert(_doc_li)
            print("done. [{}]".format(timesofar(t00)))
        if changes['delete']:
            print("Deleting {} discontinued docs...".format(len(changes['delete'])), end='')
            t00 = time.time()
            target.remove_from_ids(changes['delete'], step=step)
            print("done. [{}]".format(timesofar(t00)))

        if changes['update']:
            print("Updating {} existing docs...".format(len(changes['update'])))
            t00 = time.time()
            i = 0
            t1 = time.time()
            for _diff in changes['update']:
                target.update_diff(_diff, extra={'_timestamp': _timestamp})
                i += 1
                if i > 1 and i % step == 0:
                    print('\t{}\t{}'.format(i, timesofar(t1)))
                    t1 = time.time()
            print("done. [{}]".format(timesofar(t00)))
        print("\n")
        print("Finished.", timesofar(t0))

    def verify_changes(self, changes):
        _timestamp = changes['timestamp']
        if changes['add']:
            print('Verifying "add"...', end='')
            _cnt = self._target_col.find({'_id': {'$in': changes['add']}}).count()
            if _cnt == len(changes['add']):
                print('...{}=={}...OK'.format(_cnt, len(changes['add'])))
            else:
                print('...{}!={}...ERROR!!!'.format(_cnt, len(changes['add'])))
        if changes['delete']:
            print('Verifying "delete"...', end='')
            _cnt = self._target_col.find({'_id': {'$in': changes['delete']}}).count()
            if _cnt == 0:
                print('...{}==0...OK'.format(_cnt))
            else:
                print('...{}!=0...ERROR!!!'.format(_cnt))

        print("Verifying all docs have timestamp...", end='')
        _cnt = self._target_col.find({'_timestamp': {'$exists': True}}).count()
        _cnt_all = self._target_col.count()
        if _cnt == _cnt_all:
            print('{}=={}...OK'.format(_cnt, _cnt_all))
        else:
            print('ERROR!!!\n\t Should be "{}", but get "{}"'.format(_cnt_all, _cnt))

        print("Verifying all new docs have updated timestamp...", end='')
        cur = self._target_col.find({'_timestamp': {'$gte': _timestamp}}, fields={})
        _li1 = sorted(changes['add'] + [x['_id'] for x in changes['update']])
        _li2 = sorted([x['_id'] for x in cur])
        if _li1 == _li2:
            print("{}=={}...OK".format(len(_li1), len(_li2)))
        else:
            print('ERROR!!!\n\t Should be "{}", but get "{}"'.format(len(_li1), len(_li2)))

    def _get_cleaned_timestamp(self, timestamp):
        if isinstance(timestamp, basestring):
            timestamp = datetime.strptime(timestamp, '%Y%m%d')
        assert isinstance(timestamp, datetime)
        return timestamp

    def get_change_history(self, before=None, after=None):
        _range = {}
        if before:
            before = self._get_cleaned_timestamp(before)
            _range['$lt'] = before
        if after:
            after = self._get_cleaned_timestamp(after)
            _range['$gt'] = after

        if _range:
            return self._target_col.find({'_timestamp': _range})
        else:
            raise ValueError('must provide either "before" for "after" argument.')

    def backup_timestamp(self, outfile=None, compress=True):
        '''backup "_id" and "_timestamp" fields into a output file.'''
        ts = time.strftime('%Y%m%d')
        outfile = outfile or self._target_col.name + '_tsbk_' + ts + '.txt'
        if compress:
            outfile += '.bz'
            import bz2
        print('Backing up timestamps into "{}"...'.format(outfile))
        file_handler = bz2.BZ2File if compress else file
        with file_handler(outfile, 'w') as out_f:
            for doc in doc_feeder(self._target_col, step=100000, fields=['_timestamp']):
                out_f.write('{}\t{}\n'.format(doc['_id'], doc['_timestamp'].strftime('%Y%m%d')))

    def get_timestamp_stats(self, returnresult=False):
        '''Return the count of each timestamps in _target_col.'''
        res = self._target_col.aggregate([{"$group": {"_id": "$_timestamp", "count": {"$sum": 1}}}])
        res = sorted([(x['_id'], x['count']) for x in res['result']], reverse=True)
        for ts, cnt in res:
            print('{}\t{}'.format(ts.strftime('%Y%m%d'), cnt))
        if returnresult:
            return res


def mark_timestamp(timestamp):
    #.update({'_id': {'$in': xli1}}, {'$set': {'_timestamp': ts}}, multi=True)
    target = get_target_db()
    #genedoc_col = target.genedoc_mygene_allspecies_current
    genedoc_col = target.genedoc_mygene_xxxxx
    for doc in doc_feeder(genedoc_col):
        genedoc_col.update({'_id': doc['_id']},
                           {'$set': {'_timestamp': timestamp}},
                           manipulate=False, check_keys=False,
                           upsert=False, w=0)


def _get_timestamp(source_col):
    mat = re.search('_(\d{8})_\w{8}$', source_col)
    if mat:
        _timestamp = mat.group(1)
        _timestamp = datetime.strptime(_timestamp, '%Y%m%d')
        return _timestamp


def get_changes_stats(changes):
    for k in ['source', 'timestamp', 'add', 'delete', 'update']:
        if k in changes:
            v = changes[k]
            if isinstance(v, (list, dict)):
                v = len(v)
            print("{}: {}".format(k, v))
    _update = changes['update']
    if _update:
        attrs = dict(add=set(), delete=set(), update=set())
        for _d in _update:
            for k in attrs.keys():
                if _d[k]:
                    attrs[k] |= set(_d[k])
        #pprint(attrs)
        print('\n'.join(["\t{}: {} {}".format(k, len(attrs[k]), ', '.join(sorted(attrs[k]))) for k in attrs]))


def diff_two(col_1, col_2, use_parallel=True):
    target = get_target_db()
    b1 = GeneDocMongoDBBackend(target[col_1])
    b2 = GeneDocMongoDBBackend(target[col_2])
    return diff_collections(b1, b2, use_parallel=use_parallel)
