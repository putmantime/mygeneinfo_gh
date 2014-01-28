import time
import subprocess
import socket
from optparse import OptionParser

from databuild.builder import DataBuilder, timesofar
#from utils.es import es_clean_indices
from utils.common import ask
from config import ES_HOST_TUNNEL_CFG

es_local_tunnel_port = 9201


def port_open(host, port):
    s = socket.socket()
    try:
        s.connect((host, port))
        return True
    except socket.error:
        return False


class ESTunnelContextManager:
    def __init__(self):
        self.p = None

    def __enter__(self):
        assert not port_open('localhost', es_local_tunnel_port), 'localhost:{} is alreay open'.format(es_local_tunnel_port)
        tunnel_cmd = "ssh -o StrictHostKeyChecking=no -N -L {0}:localhost:9200 -C -i {key} {username}@{host}"
        tunnel_cmd = tunnel_cmd.format(es_local_tunnel_port, **ES_HOST_TUNNEL_CFG)
        self.p = subprocess.Popen(tunnel_cmd.split())
        time.sleep(2)
        if port_open('localhost', es_local_tunnel_port):
            print "ssh tunnel is running at localhost:{} (pid: {})...".format(es_local_tunnel_port, self.p.pid)
        else:
            print 'Error: localhost:{} failed to open'.format(es_local_tunnel_port)
        return self.p

    def __exit__(self, *exc_info):
        if self.p and self.p.poll() is None:
            self.p.kill()
            print "ssh tunnel is closed now."
            time.sleep(1)
            assert not port_open('localhost', es_local_tunnel_port), 'localhost:{} is still open'.format(es_local_tunnel_port)


def open_tunnel():
    return ESTunnelContextManager()


def validate(build_config=None):
    from pprint import pprint
    from utils.diff import diff_collections
    from databuild.backend import GeneDocMongoDBBackend, GeneDocESBackend
    from utils.mongo import get_src_build, get_target_db
    from utils.es import ESIndexer

    src_build = get_src_build()
    _cfg = src_build.find_one({'_id': build_config})
    last_build = _cfg['build'][-1]
    print "Last build record:"
    pprint(last_build)
    target_name = last_build['target']

    mongo_target = get_target_db()
    b1 = GeneDocMongoDBBackend(mongo_target[target_name])
    b2 = GeneDocESBackend(ESIndexer(es_index_name=target_name,
                                    es_host='127.0.0.1:' + str(es_local_tunnel_port)))
    changes = diff_collections(b1, b2, use_parallel=True, step=10000)
    return changes


def test(build_config):
    with open_tunnel():
        validate(build_config)


def main():
    parser = OptionParser()
    parser.add_option("-c", "--conf", dest="config",
                      action="store", default=None,
                      help="ES indexing building config name")
    parser.add_option("-b", "--noconfirm", dest="noconfirm",
                      action="store_true", default=False,
                      help="do not ask for confirmation")
    parser.add_option("-e", "--es-index", dest="es_index_name",
                      action="store", default=None,
                      help="provide an alternative ES index name")
    # parser.add_option("", "--no-cleanup", dest="nocleanup",
    #                   action="store_true", default=False,
    #                   help="do not clean up old ES indices")
    (options, args) = parser.parse_args()

    with open_tunnel():
        # if not options.nocleanup:
        #     es_clean_indices(noconfirm=options.noconfirm)
        t00 = time.time()
        bdr = DataBuilder(backend='es')
        if options.config:
            config_li = [options.config]
        else:
            config_li = ['mygene', 'mygene_allspecies']

        if not options.noconfirm:
            print '\n'.join(["Ready to build these ES indices:"] +
                            ['\t' + conf for conf in config_li])
            if ask('Continue?') != 'Y':
                print "Aborted"
                return

        for _conf in config_li:
            t0 = time.time()
            print '>"{}">>>>>>'.format(_conf)
            bdr.build_index2(_conf,
                             es_index_name=options.es_index_name,
                             es_host='127.0.0.1:' + str(es_local_tunnel_port))
            print '<<<<<<"{}"...done. {}'.format(_conf, timesofar(t0))

        print '='*20
        print "Finished.", timesofar(t00)


if __name__ == '__main__':
    main()
