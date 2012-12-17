'''data_load module is for loading individual genedocs from various data sources.'''

import copy
import types
import time
import datetime
import importlib
from mongokit import Document, CustomType
from utils.mongo import get_src_conn
from utils.common import timesofar, dump2gridfs
from config import DATA_SRC_DATABASE, DATA_SRC_MASTER_COLLECTION


__sources__ = [
              'entrez.entrez_gene',
              'entrez.entrez_homologene',
              'entrez.entrez_genesummary',
              'entrez.entrez_accession',
              'entrez.entrez_refseq',
              'entrez.entrez_unigene',
              'entrez.entrez_go',
              'entrez.entrez_ec',
              'entrez.entrez_retired',

              'ensembl.ensembl_gene',
              'ensembl.ensembl_acc',
              'ensembl.ensembl_genomic_pos',
              'ensembl.ensembl_prosite',
              'ensembl.ensembl_interpro',

              'uniprot',
              'uniprot.uniprot_pdb',
              'uniprot.uniprot_ipi',
              'uniprot.uniprot_pir',

              'pharmgkb',
              'reporter',
               ]

conn = get_src_conn()


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
    __collection__ = None  #should be specified individually
    __database__ = DATA_SRC_DATABASE
    use_dot_notation = True
    use_schemaless = True
    DEFAULT_FIELDTYPE = unicode

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
                if i%step == 0:
                    yield doc_li
                    doc_li = []
            else:
                yield _doc

        if batch:
            yield doc_li

    def load(self, genedoc_d=None, update_data=True, update_master=True, test=False, step=10000):
        if update_data:
            genedoc_d = genedoc_d or self.load_genedoc()

            print "Uploading to the DB...",
            t0 = time.time()
            # for doc in self.doc_iterator(genedoc_d, batch=False):
            #     if not test:
            #         doc.save()
            for doc_li in self.doc_iterator(genedoc_d, batch=True, step=step):
                if not test:
                    self.collection.insert(doc_li, manipulate=False, check_keys=False)
            print 'Done[%s]' % timesofar(t0)
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
    mapping["_source"] = {"enabled" : True,
                          "compress": True,
                          "compression_threshold": "1kb"}

    return mapping


def make_db(source_li):
    pass
