from __future__ import print_function
import sys
import os.path
import time
from datetime import datetime
import base64
import random
import string
import os
import json
from itertools import islice


if sys.version_info.major == 3:
    str_types = str
    import pickle       # noqa
else:
    str_types = (str, unicode)    # noqa
    import cPickle as pickle
    input = raw_input


src_path = os.path.split(os.path.split(os.path.abspath(__file__))[0])[0]


# ===============================================================================
# Misc. Utility functions
# ===============================================================================
def ask(prompt, options='YN'):
    '''Prompt Yes or No,return the upper case 'Y' or 'N'.'''
    options = options.upper()
    while 1:
        s = input(prompt+'[%s]' % '|'.join(list(options))).strip().upper()
        if s in options:
            break
    return s


def timesofar(t0, clock=0, t1=None):
    '''return the string(eg.'3m3.42s') for the passed real time/CPU time so far
       from given t0 (return from t0=time.time() for real time/
       t0=time.clock() for CPU time).'''
    t1 = t1 or time.clock() if clock else time.time()
    t = t1 - t0
    h = int(t / 3600)
    m = int((t % 3600) / 60)
    s = round((t % 3600) % 60, 2)
    t_str = ''
    if h != 0:
        t_str += '%sh' % h
    if m != 0:
        t_str += '%sm' % m
    t_str += '%ss' % s
    return t_str


def get_timestamp():
    return time.strftime('%Y%m%d')


def get_random_string():
    return base64.b64encode(os.urandom(6), random.sample(string.letters, 2))


class LogPrint:
    def __init__(self, log_f, log=1, timestamp=0):
        '''If this class is set to sys.stdout, it will output both log_f and __stdout__.
           log_f is a file handler.
        '''
        self.log_f = log_f
        self.log = log
        self.timestamp = timestamp
        if self.timestamp:
            self.log_f.write('*'*10 + 'Log starts at ' + time.ctime() + '*'*10 + '\n')

    def write(self, text):
        sys.__stdout__.write(text)
        if self.log:
            self.log_f.write(text)
            self.flush()

    def flush(self):
        self.log_f.flush()

    def start(self):
        sys.stdout = self

    def pause(self):
        sys.stdout = sys.__stdout__

    def resume(self):
        sys.stdout = self

    def close(self):
        if self.timestamp:
            self.log_f.write('*'*10 + 'Log ends at ' + time.ctime() + '*'*10 + '\n')
        sys.stdout = sys.__stdout__
        self.log_f.close()

    def fileno(self):
        return self.log_f.fileno()


def addsuffix(filename, suffix, noext=False):
    '''Add suffix in front of ".extension", so keeping the same extension.
       if noext is True, remove extension from the filename.'''
    if noext:
        return os.path.splitext(filename)[0] + suffix
    else:
        return suffix.join(os.path.splitext(filename))


def safewfile(filename, prompt=True, default='C', mode='w'):
    '''return a file handle in 'w' mode,use alternative name if same name exist.
       if prompt == 1, ask for overwriting,appending or changing name,
       else, changing to available name automatically.'''
    suffix = 1
    while 1:
        if not os.path.exists(filename):
            break
        print('Warning:"%s" exists.' % filename, end='')
        if prompt:
            option = ask('Overwrite,Append or Change name?', 'OAC')
        else:
            option = default
        if option == 'O':
            if not prompt or ask('You sure?') == 'Y':
                print("Overwritten.")
                break
        elif option == 'A':
            print("Append to original file.")
            f = open(filename, 'a')
            f.write('\n' + "=" * 20 + 'Appending on ' + time.ctime() + "=" * 20 + '\n')
            return f, filename
        print('Use "%s" instead.' % addsuffix(filename, '_' + str(suffix)))
        filename = addsuffix(filename, '_' + str(suffix))
        suffix += 1
    return open(filename, mode), filename


def SubStr(input_string, start_string='', end_string='', include=0):
    '''Return the substring between start_string and end_string.
        If start_string is '', cut string from the beginning of input_string.
        If end_string is '', cut string to the end of input_string.
        If either start_string or end_string can not be found from input_string, return ''.
        The end_pos is the first position of end_string after start_string.
        If multi-occurence,cut at the first position.
        include=0(default), does not include start/end_string;
        include=1:          include start/end_string.'''

    start_pos = input_string.find(start_string)
    if start_pos == -1:
        return ''
    start_pos += len(start_string)
    if end_string == '':
        end_pos = len(input_string)
    else:
        end_pos = input_string[start_pos:].find(end_string)   # get the end_pos relative with the start_pos
        if end_pos == -1:
            return ''
        else:
            end_pos += start_pos  # get actual end_pos
    if include == 1:
        return input_string[start_pos - len(start_string): end_pos + len(end_string)]
    else:
        return input_string[start_pos:end_pos]


def safe_unicode(s, mask='#'):
    '''replace non-decodable char into "#".'''
    try:
        _s = unicode(s)
    except UnicodeDecodeError as e:
        pos = e.args[2]
        _s = s.replace(s[pos], mask)
        print('Warning: invalid character "%s" is masked as "%s".' % (s[pos], mask))
        return safe_unicode(_s, mask)

    return _s


def file_newer(source, target):
    '''return True if source file is newer than target file.'''
    return os.stat(source)[-2] > os.stat(target)[-2]


def dump(object, filename, bin=1):
    '''Saves a compressed object to disk
    '''
    import gzip
    print('Dumping into "%s"...' % filename, end='')
    file = gzip.GzipFile(filename, 'wb')
    pickle.dump(object, file, protocol=bin)
    file.close()
    print('Done. [%s]' % os.stat(filename).st_size)


def dump2gridfs(object, filename, db, bin=1):
    '''Save a compressed object to MongoDB gridfs.'''
    import gzip
    import gridfs
    print('Dumping into "MongoDB:%s/%s"...' % (db.name, filename), end='')
    fs = gridfs.GridFS(db)
    if fs.exists(_id=filename):
        fs.delete(filename)
    fobj = fs.new_file(filename=filename, _id=filename)
    try:
        gzfobj = gzip.GzipFile(filename=filename, mode='wb', fileobj=fobj)
        pickle.dump(object, gzfobj, protocol=bin)
    finally:
        gzfobj.close()
        fobj.close()
    print('Done. [%s]' % fs.get(filename).length)


def loadobj(filename, mode='file'):
    '''Loads a compressed object from disk file (or file-like handler) or
        MongoDB gridfs file (mode='gridfs')
           obj = loadobj('data.pyobj')

           obj = loadobj(('data.pyobj', mongo_db), mode='gridfs')
    '''
    import gzip

    if mode == 'gridfs':
        import gridfs
        filename, db = filename   # input is a tuple of (filename, mongo_db)
        fs = gridfs.GridFS(db)
        fobj = fs.get(filename)
    else:
        if isinstance(filename, str_types):
            fobj = open(filename, 'rb')
        else:
            fobj = filename   # input is a file-like handler
    gzfobj = gzip.GzipFile(fileobj=fobj)
    try:
        buffer = ""
        while 1:
            data = gzfobj.read()
            if data == "":
                break
            buffer += data
        object = pickle.loads(buffer)
    finally:
        gzfobj.close()
        fobj.close()
    return object


def is_int(s):
    """return True or False if input string is integer or not."""
    try:
        int(s)
        return True
    except ValueError:
        return False


def is_str(s):
    """return True or False if input is a string or not.
        python3 compatible.
    """
    return isinstance(s, str_types)


def is_seq(li):
    """return True if input is either a list or a tuple.
    """
    return isinstance(li, (list, tuple))


def newer(t0, t1, format='%Y%m%d'):
    '''t0 and t1 are string of timestamps matching "format" pattern.
       Return True if t1 is newer than t0.
    '''
    return datetime.strptime(t0, format) < datetime.strptime(t1, format)


def hipchat_msg(msg, color='yellow', message_format='text'):
    import httplib2
    from config import HIPCHAT_CONFIG

    h = httplib2.Http()
    url = 'https://api.hipchat.com/v1/rooms/message?format=json&auth_token=' + HIPCHAT_CONFIG['token']
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    color = 'yellow'
    _msg = msg.lower()
    for keyword in ['fail', 'error']:
        if _msg.find(keyword) != -1:
            color = 'red'
            break
    params = 'room_id={}&from={}&message={}&color={}&message_format={}'
    params = params.format(HIPCHAT_CONFIG['roomid'],
                           HIPCHAT_CONFIG['from'],
                           msg,
                           color,
                           message_format)
    res, con = h.request(url, 'POST', params, headers=headers)
    assert res.status == 200, (str(res), con)


class dotdict(dict):
    def __getattr__(self, attr):
        value = self.get(attr, None)
        if isinstance(value, dict):
            return dotdict(value)
        else:
            return value
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def iter_n(iterable, n, with_cnt=False):
    '''
    Iterate an iterator by chunks (of n)
    if with_cnt is True, return (chunk, cnt) each time
    ref http://stackoverflow.com/questions/8991506/iterate-an-iterator-by-chunks-of-n-in-python
    '''
    it = iter(iterable)
    if with_cnt:
        cnt = 0
    while True:
        chunk = tuple(islice(it, n))
        if not chunk:
            return
        if with_cnt:
            cnt += len(chunk)
            yield (chunk, cnt)
        else:
            yield chunk


def send_s3_file(localfile, s3key, overwrite=False):
    '''save a localfile to s3 bucket with the given key.
       bucket is set via S3_BUCKET
       it also save localfile's lastmodified time in s3 file's metadata
    '''
    from config import AWS_KEY, AWS_SECRET, S3_BUCKET
    from boto import connect_s3

    assert os.path.exists(localfile), 'localfile "{}" does not exist.'.format(localfile)
    s3 = connect_s3(AWS_KEY, AWS_SECRET)
    bucket = s3.get_bucket(S3_BUCKET)
    k = bucket.new_key(s3key)
    if not overwrite:
        assert not k.exists(), 's3key "{}" already exists.'.format(s3key)
    lastmodified = os.stat(localfile)[-2]
    k.set_metadata('lastmodified', lastmodified)
    k.set_contents_from_filename(localfile)


class DateTimeJSONEncoder(json.JSONEncoder):
    '''A class to dump Python Datetime object.
        json.dumps(data, cls=DateTimeJSONEncoder, indent=indent)
    '''
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return super(DateTimeJSONEncoder, self).default(obj)
