'''
Backend for storing merged genedoc after building.
Support MongoDB, ES, CouchDB
'''

from utils.es import ESIndexer, IndexMissingException

class GeneDocBackendBase:
    name = 'Undefined'

    def prepare(self):
        '''if needed, add extra preparation steps here.'''
        pass

    def insert(self, doc_li):
        raise NotImplemented

    def update(self, id, extra_doc):
        '''update only, no upsert.'''
        raise NotImplemented

    def drop(self):
        raise NotImplemented

    def get_id_list(self):
        raise NotImplemented

    def get_from_id(self, id):
        raise NotImplemented


class GeneDocMongoDBBackend(GeneDocBackendBase):
    name = 'mongodb'
    def __init__(self, target_collection=None):
        self.target_collection = target_collection

    def insert(self, doc_li):
        self.target_collection.insert(doc_li, manipulate=False, check_keys=False)

    def update(self, id, extra_doc):
        self.target_collection.update({'_id': id}, {'$set': extra_doc},
                                      manipulate=False,
                                      upsert=False) #,safe=True)

    def drop(self):
        self.target_collection.drop()

    def get_id_list(self):
        return [x['_id'] for x in self.target_collection.find(fields=[], manipulate=False)]

    def get_from_id(self, id):
        return self.target_collection.get_from_id(id)


class GeneDocESBackend(GeneDocBackendBase):
    name = 'es'
    def __init__(self, esidxer=None):
        self.target_esidxer = esidxer

    def prepare(self, update_mapping=True):
        self.target_esidxer.create_index()
        self.target_esidxer.verify_mapping(update_mapping=update_mapping)

    def insert(self, doc_li):
        conn = self.target_esidxer.conn
        index_name = self.target_esidxer.ES_INDEX_NAME
        index_type = self.target_esidxer.ES_INDEX_TYPE
        for doc in doc_li:
            conn.index(doc, index_name, index_type, doc['_id'], bulk=True)
        conn.flush()
        conn.refresh()

    def update(self, id, extra_doc):
        self.target_esidxer.update(id, extra_doc)

    def drop(self):
        conn = self.target_esidxer.conn
        index_name = self.target_esidxer.ES_INDEX_NAME
        index_type = self.target_esidxer.ES_INDEX_TYPE

        #Check if index_type exists
        try:
            conn.get_mapping(index_type, index_name)
        except IndexMissingException:
            return
        return conn.delete_mapping(index_name, index_type)

    def get_id_list(self):
        return self.target_esidxer.get_id_list()

    def get_from_id(self, id):
        conn = self.target_esidxer.conn
        index_name = self.target_esidxer.ES_INDEX_NAME
        index_type = self.target_esidxer.ES_INDEX_TYPE
        return conn.get(index_name, index_type, id)
