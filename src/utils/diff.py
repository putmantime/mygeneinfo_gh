'''
Utils to compare two list of gene documents
'''
from databuild.backend import (GeneDocMongoDBBackend,
                               GeneDocCouchDBBackend)

def diff_doc(doc_1, doc_2):
    diff_d = {'update': {},
              'delete': [],
              'add': {}}
    for attr in set(doc_1) | set(doc_2):
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


def diff_collections(b1, b2):
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

    print "Comparing matching docs...",
    _updates = []
    if len(id_common) > 0:
        for _id in id_common:
            doc1 = b1.get_from_id(_id)
            doc2 = b2.get_from_id(_id)
            _diff = diff_doc(doc1, doc2)
            if _diff:
                _diff['_id'] = _id
                _updates.append(_diff)
    print "Done. [{} docs changed]".format(len(_updates))

    _deletes = []
    if len(id_in_1) > 0:
        _deletes = sorted(id_in_1)

    _adds = []
    if len(id_in_2) > 0:
        _adds = sorted(id_in_2)

    changes = {'update': _updates,
               'delete': _deletes,
               'add': '_adds'}
    return changes
















