'''data_load module is for loading individual genedocs from various data sources.'''

import sys
import copy
import types
import time
import datetime
import importlib
from mongokit import Document, CustomType
from utils.mongo import get_src_conn, get_src_dump
from utils.common import get_timestamp, get_random_string, timesofar, dump2gridfs
from config import DATA_SRC_DATABASE, DATA_SRC_MASTER_COLLECTION


__sources_dict__ = {
    'entrez': [
        'entrez.entrez_gene',
        'entrez.entrez_homologene',
        'entrez.entrez_genesummary',
        'entrez.entrez_accession',
        'entrez.entrez_refseq',
        'entrez.entrez_unigene',
        'entrez.entrez_go',
        'entrez.entrez_ec',
        'entrez.entrez_retired',
        'entrez.entrez_generif'
    ],
    'ensembl': [
        'ensembl.ensembl_gene',
        'ensembl.ensembl_acc',
        'ensembl.ensembl_genomic_pos',
        'ensembl.ensembl_prosite',
        'ensembl.ensembl_interpro'
    ],
    'uniprot': [
        'uniprot',
        'uniprot.uniprot_pdb',
        # 'uniprot.uniprot_ipi',   # IPI is now discontinued, last update is still in the db, but won't be updated.
        'uniprot.uniprot_pir'
    ],
    'pharmgkb': ['pharmgkb'],
    'reporter': ['reporter'],
    'ucsc':     ['ucsc.ucsc_exons']
    }

__sources__ = None   # should be a list defined at runtime

conn = get_src_conn()


def get_data_folder(src_name):
    src_dump = get_src_dump()
    src_doc = src_dump.find_one({'_id': src_name})
    assert src_doc['status'] == 'success', "Source files are not ready yet [status: \"%s\"]." % src_doc['status']
    return src_doc['data_folder']


class CustomField(CustomType):
    pass


@conn.register
class GeneDocSourceMaster(Document):
    '''A class to manage various genedoc data sources.'''
    __collection__ = DATA_SRC_MASTER_COLLECTION
    __database__ = DATA_SRC_DATABASE
    use_dot_notation = True
    use_schemaless = True
    structure = {'name': unicode,
                 'timestamp': datetime.datetime,
                 }


class GeneDocSource(Document):
    '''A base class for all source data.'''
    __collection__ = None      # should be specified individually
    __database__ = DATA_SRC_DATABASE
    use_dot_notation = True
    use_schemaless = True
    DEFAULT_FIELDTYPE = unicode

    temp_collection = None     # temp collection is for dataloading

    def make_temp_collection(self):
        '''Create a temp collection for dataloading, e.g., entrez_geneinfo_INEMO.'''

        new_collection = None
        while 1:
            new_collection = self.__collection__ + '_temp_' + get_random_string()
            if new_collection not in self.db.collection_names():
                break
        self.temp_collection = self.db[new_collection]
        return new_collection

    def doc_iterator(self, genedoc_d, batch=True, step=10000, validate=True):
        if batch:
            doc_li = []
            i = 0
        for _id, doc in genedoc_d.items():
            doc['_id'] = _id
            _doc = copy.copy(self)
            _doc.clear()
            _doc.update(doc)
            if validate:
                _doc.validate()
            if batch:
                doc_li.append(_doc)
                i += 1
                if i % step == 0:
                    yield doc_li
                    doc_li = []
            else:
                yield _doc

        if batch:
            yield doc_li

    def load(self, genedoc_d=None, update_data=True, update_master=True, test=False, step=10000):
        if not self.temp_collection:
            self.make_temp_collection()

        self.temp_collection.drop()       # drop all existing records just in case.

        if update_data:
            genedoc_d = genedoc_d or self.load_genedoc()

            print "Uploading to the DB...",
            t0 = time.time()
            # for doc in self.doc_iterator(genedoc_d, batch=False):
            #     if not test:
            #         doc.save()
            for doc_li in self.doc_iterator(genedoc_d, batch=True, step=step):
                if not test:
                    self.temp_collection.insert(doc_li, manipulate=False, check_keys=False)
            print 'Done[%s]' % timesofar(t0)
            self.switch_collection()

            if getattr(self, 'ENTREZ_GENEDOC_ROOT', False):
                print 'Uploading "geneid_d" to GridFS...',
                t0 = time.time()
                geneid_d = self.get_geneid_d()
                dump2gridfs(geneid_d, self.__collection__+'__geneid_d.pyobj', self.db)
                print 'Done[%s]' % timesofar(t0)
            if getattr(self, 'ENSEMBL_GENEDOC_ROOT', False):
                print 'Uploading "mapping2entrezgene" to GridFS...',
                t0 = time.time()
                x2entrezgene_list = self.get_mapping_to_entrez()
                dump2gridfs(x2entrezgene_list, self.__collection__+'__2entrezgene_list.pyobj', self.db)
                print 'Done[%s]' % timesofar(t0)

        if update_master:
            #update src_master collection
            if not test:
                _doc = {"_id": unicode(self.__collection__),
                        "name": unicode(self.__collection__),
                        "timestamp": datetime.datetime.now()}
                for attr in ['ENTREZ_GENEDOC_ROOT', 'ENSEMBL_GENEDOC_ROOT', 'id_type']:
                    if hasattr(self, attr):
                        _doc[attr] = getattr(self, attr)
                if hasattr(self, 'get_mapping'):
                    _doc['mapping'] = getattr(self, 'get_mapping')()

                conn.GeneDocSourceMaster(_doc).save()

    def switch_collection(self):
        '''after a successful loading, rename temp_collection to regular collection name,
           and renaming existing collection to a temp name for archiving purpose.
        '''
        if self.temp_collection and self.temp_collection.count() > 0:
            if self.collection.count() > 0:
                #renaming existing collections
                new_name = '_'.join([self.__collection__, 'archive', get_timestamp(), get_random_string()])
                self.collection.rename(new_name, dropTarget=True)
            self.temp_collection.rename(self.__collection__)
        else:
            print "Error: load data first."

    def validate_all(self, genedoc_d=None):
        """validate all genedoc_d."""
        genedoc_d = genedoc_d or self.load_genedoc()
        for doc in self.doc_iterator(genedoc_d, batch=False, validate=True):
            pass


def register_sources():
    for src in __sources__:
        src_m = importlib.import_module('dataload.sources.'+src)
        metadata = src_m.__metadata__
        name = src + '_doc'
        metadata['load_genedoc'] = src_m.load_genedoc
        metadata['get_mapping'] = src_m.get_mapping
        if metadata.get('ENTREZ_GENEDOC_ROOT', False):
            metadata['get_geneid_d'] = src_m.get_geneid_d
        if metadata.get('ENSEMBL_GENEDOC_ROOT', False):
            metadata['get_mapping_to_entrez'] = src_m.get_mapping_to_entrez
        src_cls = types.ClassType(name, (GeneDocSource,), metadata)
        conn.register(src_cls)


#register_sources()
def get_src(src):
    _src = conn[src+'_doc']()
    return _src


def load_src(src, **kwargs):
    print "Loading %s..." % src
    _src = conn[src+'_doc']()
    _src.load(**kwargs)


def update_mapping(src):
    _src = conn[src+'_doc']()
    _src.load(update_data=False, update_master=True)


def load_all(**kwargs):
    for src in __sources__:
        load_src(src, **kwargs)


def get_mapping():
    mapping = {}
    properties = {}
    for src in __sources__:
        print "Loading mapping from %s..." % src
        _src = conn[src+'_doc']()
        _field_properties = _src.get_mapping()
        properties.update(_field_properties)
    mapping["properties"] = properties
    #enable _source compression
    mapping["_source"] = {"enabled": True,
                          "compress": True,
                          "compression_threshold": "1kb"}

    return mapping


def main():
    '''
    Example:
        python -m dataload ensembl.ensembl_gene ensembl.ensembl_acc ensembl.ensembl_genomic_pos ensembl.ensembl_prosite ensembl.ensembl_interpro
        python -m dataload/__init__ entrez.entrez_gene entrez.entrez_homologene entrez.entrez_genesummary entrez.entrez_accession entrez.entrez_refseq entrez.entrez_unigene entrez.entrez_go entrez.entrez_ec entrez.entrez_retired

    '''

    global __sources__
    __sources__ = sys.argv[1:]
    register_sources()
    load_all()

if __name__ == '__main__':
    main()
