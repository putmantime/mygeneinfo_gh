from utils.mongo import (get_src_db, get_target_db, get_src_master,
                         doc_feeder)
from utils.es import ESIndexer


class DataBuilder():

    def __init__(self):
        self.src = get_src_db()
        self.target = get_target_db()
        self.src_collection_list = []

        self.get_src_collection_list()

    def get_src_collection_list(self):
        src_master = get_src_master(self.src.connection)
        self.src_collection_list = [doc['name'] for doc in src_master.find()]
        return self.src_collection_list

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

    def merge(self, step=10000):
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
        for collection in self.src_collection_list:
            meta = src_master.get_from_id(collection)
            if meta.has_key('mapping'):
                mapping.update(meta['mapping'])
            else:
                print 'Warning: "%s" collection has no mapping data.' % collection
        mapping = {"properties": mapping,
                   "dynamic": False}
        return mapping

    def build_index(self):
        es_idxer = ESIndexer(self.target.genedoc, self.get_mapping())
        es_idxer.step = 1000
        es_idxer.create_index()
        es_idxer.build_index(verbose=False)




