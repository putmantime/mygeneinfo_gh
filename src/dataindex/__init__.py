"""dataindex module is for building index for "merged" genedocs (from databuild module) using ElasticSearch."""

#http://www.elasticsearch.org/guide/reference/query-dsl/custom-filters-score-query.html
#http://www.elasticsearch.org/guide/reference/query-dsl/custom-score-query.html
#http://www.elasticsearch.org/guide/reference/query-dsl/custom-boost-factor-query.html
#http://www.elasticsearch.org/guide/reference/query-dsl/boosting-query.html

import json
#from utils.common import is_int
from pyelasticsearch import ElasticSearch

es = ElasticSearch('http://su02:9200/')

def is_int(s):
    """return True or False if input string is integer or not."""
    try:
        int(s)
        return True
    except ValueError:
        return False


class ESQuery:
    def __init__(self):
        self.conn = es
        self._index = 'genedoc_2'
        self._doc_type = 'gene'
        #self._doc_type = 'gene_sample'

    def _search(self, q):
        return es.search(q, index=self._index, doc_type=self._doc_type)

    def get_gene(self, geneid, fields=None, **kwargs):
        if fields:
            kwargs['fields'] = fields
        return self.conn.get(self._index, self._doc_type, geneid, **kwargs)

    def query(self, q, fields=['symbol','name','taxid'], **kwargs):
        mode = int(kwargs.pop('mode', 1))
        #_q = make_query(q, fields=fields, **kwargs )
        qbdr = ESQueryBuilder(fields=fields, **kwargs)
        _q = qbdr.build(q, mode)
        return self._search(_q)

    def query_sample(self, q, **kwargs):
        self._doc_type = 'gene_sample'
        res = self.query(q, **kwargs)
        self._doc_type = 'gene'
        return res


def test2(q):
    esq = ESQuery()
    return esq.query(q)


def make_query(q, fields, **kwargs):
    '''http://www.elasticsearch.org/guide/reference/query-dsl/dis-max-query.html'''
    _query = {
    "dis_max" : {
        "tie_breaker" : 0,
        "boost" : 1,
        "queries" : [
            {
            "custom_boost_factor": {
                "query" : {
                    "match" : { "symbol" : "%(q)s" },
                },
                "boost_factor": 5
            }
            },
            {
            "custom_boost_factor": {
                "query" : {
                    "match_phrase" : { "name" : "%(q)s" },
                },
                "boost_factor": 4
            }
            },
            {
            "custom_boost_factor": {
                "query" : {
                    "match" : { "name" : "%(q)s" },
                },
                "boost_factor" : 3
            }
            },
            {
            "custom_boost_factor": {
                "query" : {
                    "match" : { "_all" : {
                                                    "query": "%(q)s" ,
                                                    "analyzer": "keyword"
                                                 }
                                     }
                },
                "boost_factor": 2
            }
            },
            {
            "custom_boost_factor": {
                "query" : {
                    "match" : { "_all" : "%(q)s" },
                },
                "boost_factor": 1
            }
            },

        ]
    }
    }

    #adding species filter
    _query = {'filtered': {
                'query': _query,
                'filter' : {
                    "terms" : {
                        "taxid" : [ 9606, 10090, 10116, 7227, 6239]
                    }
                }
              }
              }

    _query = {
        "custom_filters_score": {
        "query": _query,
        "filters" : [
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

    #_q = {'query': _query, "fields": ['symbol', 'name', 'taxid'], "explain": True}
    _q = {'query': _query}
    if fields:
        _q.update({'fields': fields})
    if kwargs:
        _q.update(kwargs)

    _q = json.dumps(_q, indent=2)
    _q = json.loads(_q % {'q': q})

    return _q

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

