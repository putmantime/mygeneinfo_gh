import sys
import os.path
import time
from datetime import datetime
import base64
import os
import types


#===============================================================================
# Misc. Utility functions
#===============================================================================

def ask(prompt,options='YN'):
    '''Prompt Yes or No,return the upper case 'Y' or 'N'.'''
    options=options.upper()
    while 1:
        s=raw_input(prompt+'[%s]' % '|'.join(list(options))).strip().upper()
        if s in options: break
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
    return base64.urlsafe_b64encode(os.urandom(6))


class LogPrint:
    def __init__(self,log_f,log=1,timestamp=0):
        '''If this class is set to sys.stdout, it will output both log_f and __stdout__.
           log_f is a file handler.
        '''
        self.log_f=log_f
        self.log=log
        self.timestamp=timestamp
        if self.timestamp:
           self.log_f.write('*'*10 + 'Log starts at ' + time.ctime() + '*'*10 +'\n')
    def write(self,text):
        sys.__stdout__.write(text)
        if self.log:
            self.log_f.write(text)
            self.flush()

    def flush(self):
        self.log_f.flush()
    def start(self):
        sys.stdout=self
    def pause(self):
        sys.stdout=sys.__stdout__
    def resume(self):
        sys.stdout=self
    def close(self):
        if self.timestamp:
           self.log_f.write('*'*10 + 'Log ends at ' + time.ctime() + '*'*10 +'\n')
        sys.stdout=sys.__stdout__
        self.log_f.close()
    def fileno(self):
        return self.log_f.fileno()

def addsuffix(filename,suffix,noext=False):
    '''Add suffix in front of ".extension", so keeping the same extension.
       if noext is True, remove extension from the filename.'''
    if noext:
        return os.path.splitext(filename)[0] + suffix
    else:
        return suffix.join(os.path.splitext(filename))


def safewfile(filename,prompt=True,default='C',mode='w'):
    '''return a file handle in 'w' mode,use alternative name if same name exist.
       if prompt == 1, ask for overwriting,appending or changing name,
       else, changing to available name automatically.'''
    suffix = 1
    while 1:
        if not os.path.exists(filename):
            break
        print 'Warning:"%s" exists.' % filename,
        if prompt:
            option = ask('Overwrite,Append or Change name?','OAC')
        else:
            option = default
        if option == 'O':
            if not prompt or ask('You sure?') == 'Y':
                print "Overwritten."
                break
        elif option == 'A':
            print "Append to original file."
            f=file(filename,'a')
            f.write('\n'+"="*20+'Appending on '+time.ctime()+"="*20+'\n')
            return f,filename
        print 'Use "%s" instead.' % addsuffix(filename,'_'+str(suffix))
        filename=addsuffix(filename,'_'+str(suffix))
        suffix+=1
    return file(filename,mode),filename


def SubStr(input_string,start_string='',end_string='',include=0):
    '''Return the substring between start_string and end_string.
        If start_string is '', cut string from the beginning of input_string.
        If end_string is '', cut string to the end of input_string.
        If either start_string or end_string can not be found from input_string, return ''.
        The end_pos is the first position of end_string after start_string.
        If multi-occurence,cut at the first position.
        include=0(default), does not include start/end_string;
        include=1:          include start/end_string.'''

    start_pos=input_string.find(start_string)
    if start_pos == -1:
        return ''
    start_pos += len(start_string)
    if end_string == '':
        end_pos=len(input_string)
    else:
        end_pos=input_string[start_pos:].find(end_string) # get the end_pos relative with the start_pos
        if end_pos == -1 :
            return ''
        else:
            end_pos += start_pos  # get actual end_pos
#    print start_pos
#    print end_pos
    if include==1:
        return input_string[start_pos-len(start_string):end_pos+len(end_string)]
    else:
        return input_string[start_pos:end_pos]


def safe_unicode(s, mask='#'):
    '''replace non-decodable char into "#".'''
    try:
        _s = unicode(s)
    except UnicodeDecodeError, e:
        pos = e.args[2]
        _s = s.replace(s[pos],mask)
        print 'Warning: invalid character "%s" is masked as "%s".' % (s[pos], mask)
        return safe_unicode(_s, mask)

    return _s


def file_newer(source,target):
    '''return True if source file is newer than target file.'''
    return  os.stat(source)[-2] > os.stat(target)[-2]


def dump(object, filename, bin = 1):
    '''Saves a compressed object to disk
    '''
    import gzip
    import cPickle as pickle
    print 'Dumping into "%s"...' % filename ,
    file = gzip.GzipFile(filename, 'wb')
    pickle.dump(object, file, protocol=bin)
    file.close()
    print 'Done. [%s]' % os.stat(filename).st_size

def dump2gridfs(object, filename, db, bin=1):
    '''Save a compressed object to MongoDB gridfs.'''
    import gzip, gridfs
    import cPickle as pickle
    print 'Dumping into "MongoDB:%s/%s"...' % (db.name, filename) ,
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
    print 'Done. [%s]' % fs.get(filename).length


def loadobj(filename, mode='file'):
    '''Loads a compressed object from disk file (or file-like handler) or
        MongoDB gridfs file (mode='gridfs')
           obj = loadobj('data.pyobj')

           obj = loadobj(('data.pyobj', mongo_db), mode='gridfs')
    '''
    import gzip
    import cPickle as pickle

    if mode == 'gridfs':
        import gridfs
        filename, db = filename   #input is a tuple of (filename, mongo_db)
        fs = gridfs.GridFS(db)
        fobj = fs.get(filename)
    else:
        if type(filename) in types.StringTypes:
            fobj = file(filename, 'rb')
        else:
            fobj = filename   #input is a file-like handler
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


def newer(t0, t1, format='%Y%m%d'):
    '''t0 and t1 are string of timestamps matching "format" pattern.
       Return True if t1 is newer than t0.
    '''
    return datetime.strptime(t0, format) < datetime.strptime(t1, format)

