"""dataindex module is for building index for "merged" genedocs (from databuild module) using ElasticSearch."""

#http://www.elasticsearch.org/guide/reference/query-dsl/custom-filters-score-query.html
#http://www.elasticsearch.org/guide/reference/query-dsl/custom-score-query.html
#http://www.elasticsearch.org/guide/reference/query-dsl/custom-boost-factor-query.html
#http://www.elasticsearch.org/guide/reference/query-dsl/boosting-query.html

import json
from utils.common import is_int
from utils.es import get_es
#from pyelasticsearch import ElasticSearch

#es0 = ElasticSearch('http://su02:9200/')
es = get_es()

def is_int(s):
    """return True or False if input string is integer or not."""
    try:
        int(s)
        return True
    except ValueError:
        return False

dummy_model = lambda es, res: res

class ESQuery:
    def __init__(self):
        #self.conn0 = es0
        self.conn = es
        self.conn.model = dummy_model
        self._index = 'genedoc_mygene'
        self._doc_type = 'gene'
        #self._doc_type = 'gene_sample'

    def _search(self, q):
        #return self.conn0.search(q, index=self._index, doc_type=self._doc_type)
        return self.conn.search_raw(q, indices=self._index, doc_types=self._doc_type)

    def get_gene(self, geneid, fields=None, **kwargs):
        if fields:
            kwargs['fields'] = fields
        raw = kwargs.pop('raw', False)
        #res = self.conn0.get(self._index, self._doc_type, geneid, **kwargs)
        res = self.conn.get(self._index, self._doc_type, geneid, **kwargs)
        return res if raw else res['_source']

    def query(self, q, fields=['symbol','name','taxid'], **kwargs):
        mode = int(kwargs.pop('mode', 1))
        qbdr = ESQueryBuilder(fields=fields, **kwargs)
        _q = qbdr.build(q, mode)
        return self._search(_q)

    def query_sample(self, q, **kwargs):
        self._doc_type = 'gene_sample'
        res = self.query(q, **kwargs)
        self._doc_type = 'gene'
        return res

    def query_interval(self, taxid, chr,  gstart, gend, **kwargs):
        kwargs.setdefault('fields', ['symbol','name','taxid'])
        qbdr = ESQueryBuilder(**kwargs)
        _q = qbdr.build_genomic_pos_query(taxid, chr,  gstart, gend)
        return self._search(_q)



def test2(q):
    esq = ESQuery()
    return esq.query(q)


class ESQueryBuilder():
    def __init__(self, **query_options):
        """You can pass these options:
            fields
            from
            size
            explain
        """
        self.options = query_options

    def dis_max_query(self, q):
        _query = {
            "dis_max" : {
                "tie_breaker" : 0,
                "boost" : 1,
                "queries" : [
                    {
                    "custom_boost_factor": {
                        "query" : {
                            "match" : { "symbol" : {
                                            "query": "%(q)s",
                                            "analyzer": "whitespace_lowercase"
                                            }
                                      },
                        },
                        "boost_factor": 5
                    }
                    },
                    {
                    "custom_boost_factor": {
                        "query" : {
                            #This makes phrase match of "cyclin-dependent kinase 2" appears first
                            "match_phrase" : { "name" : "%(q)s"},
                        },
                        "boost_factor": 4
                    }
                    },
                    {
                    "custom_boost_factor": {
                        "query" : {
                            "match" : { "name" : {
                                            "query": "%(q)s",
                                            "analyzer": "whitespace_lowercase"
                                            }
                                      },
                        },
                        "boost_factor" : 3
                    }
                    },
                    {
                    "custom_boost_factor": {
                        "query" : {
                            "match" : { "unigene" : {
                                                    "query": "%(q)s" ,
                                                    "analyzer": "string_lowercase"
                                                 }
                                             }
                        },
                        "boost_factor": 1.1
                    }
                    },
                    {
                    "custom_boost_factor": {
                        "query" : {
                            "match" : { "go" : {
                                                    "query": "%(q)s" ,
                                                    "analyzer": "string_lowercase"
                                                 }
                                             }
                        },
                        "boost_factor": 1.1
                    }
                    },
                    {
                    "custom_boost_factor": {
                        "query" : {
                            "match" : { "_all" : {
                                            "query": "%(q)s",
                                            "analyzer": "whitespace_lowercase"
                                }
                            },
                        },
                        "boost_factor": 1
                    }
                    },

                ]
            }
            }
        _query = json.dumps(_query)
        _query = json.loads(_query % {'q': q})

        if is_int(q):
            _query['dis_max']['queries'] = []
            _query['dis_max']['queries'].insert(0,
                    {
                    "custom_boost_factor": {
                        "query" : {
                            "term" : { "entrezgene" : int(q)},
                        },
                        "boost_factor": 8
                    }
                    }
                    )


        return _query

    def string_query(self, q):
        _query = {
            "query_string": {
                "query": "%(q)s",
                "analyzer": "string_lowercase",
                "default_operator": "AND",
                "auto_generate_phrase_queries": True
            }
        }
        _query = json.dumps(_query)
        q = "symbol:%(q)s OR name:%(q)s OR %(q)s" % {'q': q}
        _query = json.loads(_query % {'q': q})
        return _query

    def raw_string_query(self, q):
        _query = {
            "query_string": {
                "query": "%(q)s",
#                "analyzer": "string_lowercase",
                "default_operator": "AND",
                "auto_generate_phrase_queries": True
            }
        }
        _query = json.dumps(_query)
        _query = json.loads(_query % {'q': q})
        return _query

    def add_species_filter(self, _query):
        _query = {
            'filtered': {
                'query': _query,
                'filter' : {
                    "terms" : {
                        "taxid" : [9606, 10090, 10116, 7227, 6239]
                    }
                }
            }
        }
        return _query

    def add_species_custom_filters_score(self, _query):
        _query = {
            "custom_filters_score": {
            "query": _query,
            "filters" : [
                #downgrade "pseudogene" matches
                {
                    "filter" : { "term" : { "name" : "pseudogene" } },
                    "boost" : "0.5"
                },

                {
                    "filter" : { "term" : { "taxid" : 9606 } },
                    "boost" : "1.5"
                },
                {
                    "filter" : { "term" : { "taxid" : 10090 } },
                    "boost" : "1.3"
                },
                {
                    "filter" : { "term" : { "taxid" : 10116 } },
                    "boost" : "1.1"
                },

            ],
            "score_mode" : "first"
            }
        }
        return _query

    def build(self, q, mode=1):
        if mode == 1:
            _query = self.dis_max_query(q)
            print 'dis_max'
        elif mode == 2:
            _query = self.string_query(q)
            print 'string'
        else:
            _query = self.raw_string_query(q)
            print 'raw_string'

        _query = self.add_species_filter(_query)
        _query = self.add_species_custom_filters_score(_query)
        _q = {'query': _query}
        if self.options:
            _q.update(self.options)
        return _q

    def build_genomic_pos_query(self, taxid, chr, gstart, gend):
        _query = {
                   "nested" : {
                       "path" : "genomic_pos",
                       "query" : {
                            "bool" : {
                                "must" : [
                                    {
                                        "term" : {"genomic_pos.chr" : chr}
                                    },
                                    {
                                        "range" : {"genomic_pos.start" : {"gte" : gstart}}
                                    },
                                    {
                                        "range" : {"genomic_pos.end" : {"lte" : gend}}
                                    }
                                ]
                            }
                        }
                    }
                }
        _query = {
            'filtered': {
                'query': _query,
                'filter' : {
                    "term" : {"taxid" : taxid}
                }
            }
        }
        _q = {'query': _query}
        if self.options:
            _q.update(self.options)
        return _q


def make_test_index():

    def get_sample_gene(gene):
        qbdr = ESQueryBuilder(fields=['_source'], size=1000)
        _query = qbdr.dis_max_query(gene)
        _query = qbdr.add_species_custom_filters_score(_query)
        _q = {'query': _query}
        if qbdr.options:
            _q.update(qbdr.options)

        esq = ESQuery()
        res = esq._search(_q)
        return [h['_source'] for h in res['hits']['hits']]

    gli = get_sample_gene('CDK2') + \
          get_sample_gene('BTK')  + \
          get_sample_gene('insulin')

    from utils.es import ESIndexer
    index_name = 'genedoc_2'
    index_type = 'gene_sample'
    esidxer = ESIndexer(None, None)
    conn = esidxer.conn
    try:
        esidxer.delete_index_type(index_type)
    except:
        pass
    mapping = dict(conn.get_mapping('gene', index_name)['gene'])
    print conn.put_mapping(index_type, mapping, [index_name])

    print "Building index..."
    cnt = 0
    for doc in gli:
        conn.index(doc, index_name, index_type, doc['_id'])
        cnt += 1
        print cnt, ':', doc['_id']
    print conn.flush()
    print conn.refresh()
    print 'Done! - {} docs indexed.'.format(cnt)

