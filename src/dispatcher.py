import time
from datetime import datetime
import dispatch

from utils.common import safewfile, timesofar
from dataload.dispatch import (check_mongo, get_process_info, src_dump,
                               mark_upload_started, mark_upload_done)
from dataload.dispatch import dispatch as dispatch_src_upload

source_update_available = dispatch.Signal(providing_args=["src_to_update"])
source_upload_success = dispatch.Signal(providing_args=["src_name"])
source_upload_failed = dispatch.Signal(providing_args=["src_name"])
genedoc_merged = dispatch.Signal()
es_indexed = dispatch.Signal()

try:
    from utils.common import hipchat_msg
except:
    hipchat_msg = None

class GeneDocDispatcher:
    running_processes_upload = {}
    idle = True

    def check_src_dump(self):
        src_to_update_li = check_mongo()
        if src_to_update_li:
            print '\nDispatcher:  found pending jobs ', src_to_update_li
            for src_to_update in src_to_update_li:
                source_update_available.send(sender=self, src_to_update=src_to_update)

    @classmethod
    def handle_src_upload(self, src_to_update, **kwargs):
        mark_upload_started(src_to_update)
        p = dispatch_src_upload(src_to_update)
        src_dump.update({'_id': src_to_update}, {"$set": {"upload.pid": p.pid}})
        p.t0 = time.time()
        self.running_processes_upload[src_to_update] = p

    def check_src_upload(self):
        running_processes = self.running_processes_upload
        jobs_finished = []
        if running_processes:
            self.idle = True
            print 'Dispatcher:  {} active job(s)'.format(len(running_processes))
            print get_process_info(running_processes)

        for src in running_processes:
            p = running_processes[src]
            returncode = p.poll()
            if returncode is None:
                p.log_f.flush()
            else:
                t1 = round(time.time()-p.t0, 0)
                d = {
                     'upload.returncode': returncode,
                     'upload.timestamp': datetime.now(),
                     'upload.time_in_s': t1,
                     'upload.time': timesofar(p.t0),
                     'upload.logfile': p.logfile,
                     }
                if returncode == 0:
                    msg = 'Dispatcher:  "{}" uploader finished successfully with code {} (time: {}s)'.format(src, returncode, t1)
                    print msg
                    d['upload.status'] = "success"
                    if hipchat_msg:
                        hipchat_msg(msg)
                    source_upload_success.send(self, src_name = src)
                else:
                    msg = 'Dispatcher:  "{}" uploader failed with code {} (time: {}s)'.format(src, returncode, t1)
                    print msg
                    d['upload.status'] = "failed"
                    if hipchat_msg:
                        hipchat_msg(msg)
                    source_upload_failed.send(self, src_name = src)

                mark_upload_done(src, d)
                jobs_finished.append(src)
                p.log_f.close()
        for src in jobs_finished:
            del running_processes[src]

    @classmethod
    def handle_src_upload_success(self, src_name, **kwargs):
        pass

    @classmethod
    def handle_src_upload_failed(self, src_name, **kwargs):
        pass

    def check_src_build(self):
        pass

    def check_src_index(self):
        pass


    def main(self):
        while 1:
            self.idle = True
            self.check_src_dump()
            self.check_src_upload()
            self.check_src_build()
            self.check_src_index()

            if self.idle:
                print '\b'*50,
                for i in range(100):
                    print '\b'*2+[unichr(8212), '\\', '|', '/'][i%4],
                    time.sleep(0.1)
            else:
                time.sleep(10)



source_update_available.connect(GeneDocDispatcher.handle_src_upload)
source_upload_success.connect(GeneDocDispatcher.handle_src_upload_success)
source_upload_failed.connect(GeneDocDispatcher.handle_src_upload_failed)

if __name__ == '__main__':
    GeneDocDispatcher().main()

