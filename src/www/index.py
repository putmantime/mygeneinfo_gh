'''
A thin python layer for accessing GeneDoc MongoDB

Currently available URLs:

    /                  main page, redirected to /doc for now

    /query?q=cdk2      gene query service
    /gene/<geneid>     gene annotation service

'''
import sys
import os.path
import subprocess
import json
import re

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.escape
from tornado.options import define, options

from dataindex import ESQuery

__USE_WSGI__ = False

define("port", default=8000, help="run on the given port", type=int)
define("address", default="127.0.0.1", help="run on localhost")
define("debug", default=False, type=bool, help="run in debug mode")
tornado.options.parse_command_line()
if options.debug:
    import tornado.autoreload
    import logging
    logging.getLogger().setLevel(logging.DEBUG)
    options.address = '0.0.0.0'


def _get_rev():
    '''return current mercurial rev number.
        e.g.
           72:a8ef9f842af7
    '''
    pipe = subprocess.Popen(["hg", "id", "-n", "-i"],
                            stdout=subprocess.PIPE)
    output = pipe.stdout.read().strip()
    return ':'.join(reversed(output.replace('+', '').split(' ')))
__revision__ = _get_rev()


class StatusCheckHandler(tornado.web.RequestHandler):
    '''This reponses to a HEAD request of /status for status check.'''
    def head(self):
        bs = BoCServiceLayer()
        bs.get_genedoc('1017')

    def get(self):
        self.head()


class MetaDataHandler(tornado.web.RequestHandler):
    '''Return db metadata in json string.'''
    def get(self):
        bs = BoCServiceLayer()
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        metadata = bs.get_metadata(raw=True)
        metadata = '{"app_revision": "%s",' % __revision__ + metadata[1:]
        self.write(metadata)


class GeneHandler(tornado.web.RequestHandler):
    def get(self, geneid):
        fields = self.get_argument('fields', None)
        kwargs = {}
        if fields:
            kwargs['fields'] = fields
        esq = ESQuery()
        gene = esq.get_gene(geneid, **kwargs)
        _json_data = json.dumps(gene)
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.write(_json_data)

class QueryHandler(tornado.web.RequestHandler):
    def get(self):
        q = self.get_argument('q', None)
        kwargs = {}
        if q:
            fields = self.get_argument('fields', None)
            if fields:
                fields = fields.split(',')
                kwargs['fields'] = fields
            explain = self.get_argument('explain', None)
            if explain and explain.lower()=='true':
                kwargs['explain'] = True
            for arg in ['from', 'size', 'mode']:
                value = self.get_argument(arg, None)
                if value:
                    kwargs[arg] = int(value)
            sample = self.get_argument('sample', None) == 'true'
            esq = ESQuery()
            if sample:
                res = esq.query_sample(q, **kwargs)
            else:
                res = esq.query(q, **kwargs)
            _json_data = json.dumps(res)
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            self.write(_json_data)


class IntervalQueryHandler(tornado.web.RequestHandler):
    def get(self):
        #/interval?interval=chr12:56350553-56367568&taxid=9606
        interval = self.get_argument('interval', None)
        taxid = self.get_argument('taxid', None)
        kwargs = {}
        if interval and taxid:
            kwargs['taxid'] = int(taxid)
            pattern = r'chr(?P<chr>\w+):(?P<gstart>[0-9,]+)-(?P<gend>[0-9,]+)'
            mat = re.search(pattern, interval)
            if mat:
                kwargs.update(mat.groupdict())
            fields = self.get_argument('fields', None)
            if fields:
                fields = fields.split(',')
                kwargs['fields'] = fields
            explain = self.get_argument('explain', None)
            if explain and explain.lower()=='true':
                kwargs['explain'] = True
            for arg in ['from', 'size', 'mode']:
                value = self.get_argument(arg, None)
                if value:
                    kwargs[arg] = int(value)
            sample = self.get_argument('sample', None) == 'true'
            esq = ESQuery()
            res = esq.query_interval(**kwargs)
            _json_data = json.dumps(res)
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            self.write(_json_data)


class MongoViewer(tornado.web.RequestHandler):
    def get(self, db, collection=None, id=None):
        import random
        from config import DATA_SRC_SERVER, DATA_SRC_PORT
        from mongokit import Connection

        get_random = self.get_argument('random', None) != 'false'
        size = int(self.get_argument('size', 10))

        conn = Connection(DATA_SRC_SERVER, DATA_SRC_PORT)
        if collection:
            if collection == 'fs':
                import gridfs
                fs = gridfs.GridFS(conn[db])
                out = fs.list()
            else:
                collection = conn[db][collection]
                if id:
                    out = collection.find_one({"_id": id})
                elif get_random:
                    cnt = collection.count()
                    num = random.randint(0, max(cnt-size, 0))
                    out = list(collection.find().skip(num).limit(size))
                else:
                    out = list(collection.find().limit(size))
        else:
            #list all collection in this db
            out = conn[db].collection_names()

        def date_handler(obj):
            return obj.isoformat() if hasattr(obj, 'isoformat') else obj
        _json_data = json.dumps(out, default=date_handler)
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.write(_json_data)


APP_LIST = [
#        (r"/status", StatusCheckHandler),
#        (r"/metadata", MetaDataHandler),
#        (r"/release_notes", ReleaseNotesHandler),
        (r"/gene/([\w\-\.]+)/?", GeneHandler),
        (r"/query/?", QueryHandler),
        (r"/interval/?", IntervalQueryHandler),
        (r"/mongo/(\w+)/?(\w*)/?(\w*)/?", MongoViewer),
]

settings = {}
# if options.debug:
#     from boccfg import STATIC_PATH
#     settings.update({
#         "static_path": STATIC_PATH,
# #        "cookie_secret": COOKIE_SECRET,
# #        "login_url": LOGIN_URL,
# #        "xsrf_cookies": True,
#     })


def main():
    application = tornado.web.Application(APP_LIST, **settings)
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.port, address=options.address)
    loop = tornado.ioloop.IOLoop.instance()
    if options.debug:
        tornado.autoreload.start(loop)
        logging.info('Server is running on "%s:%s"...' % (options.address, options.port))

    loop.start()

if __USE_WSGI__:
    import tornado.wsgi
    wsgi_app = tornado.wsgi.WSGIApplication(APP_LIST)


if __name__ == "__main__":
    main()

