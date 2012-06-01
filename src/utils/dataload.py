# Copyright [2010-2012] [Chunlei Wu]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import sys
import os.path
import time
import types
import csv
csv.field_size_limit(10000000)  #default is 131072, too small for some big files

import anyjson as json
from utils.common import ask

#===============================================================================
# Misc. Utility functions
#===============================================================================

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


def file_newer(source,target):
    '''return True if source file is newer than target file.'''
    return  os.stat(source)[-2] > os.stat(target)[-2]

def dump(object, filename, bin = 1):
    '''Saves a compressed object to disk
    '''
    import gzip, pickle
    print 'Dumping into "%s"...' % filename ,
    file = gzip.GzipFile(filename, 'wb')
    file.write(pickle.dumps(object, bin))
    file.close()
    print 'Done. [%s]' % os.stat(filename).st_size


def loadobj(filename):
    '''Loads a compressed object from disk
    '''
    import gzip, pickle
    file = gzip.GzipFile(filename, 'rb')
    buffer = ""
    while 1:
        data = file.read()
        if data == "":
            break
        buffer += data
    object = pickle.loads(buffer)
    file.close()
    return object

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

def load_start(datafile):
    print 'Loading "%s"...' % os.path.split(datafile)[1] ,

def load_done(msg=''):
    print "Done."+msg


#===============================================================================
# List Utility functions
#===============================================================================
def llist(list,sep='\t'):
    '''Nicely output the list with each item a line.'''
    for x in list:
        if type(x) == type(()) or type(x) == type([]):
            xx=sep.join([str(i) for i in x])
        else:
            xx=str(x)
        print xx

def listitems(list,*idx):
    '''Return multiple items from list by given indexes.'''
    if type(list) is type(()):
        return tuple([list[i] for i in idx])
    else:
        return [list[i] for i in idx]

def list2dict(list,keyitem,alwayslist=False):
    '''Return a dictionary with specified keyitem as key, others as values.
       keyitem can be an index or a sequence of indexes.
       For example: li=[['A','a',1],
                        ['B','a',2],
                        ['A','b',3]]
                    list2dict(li,0)---> {'A':[('a',1),('b',3)],
                                         'B':('a',2)}
       if alwayslist is True, values are always a list even there is only one item in it.
                    list2dict(li,0,True)---> {'A':[('a',1),('b',3)],
                                              'B':[('a',2),]}
    '''
    dict={}
    for x in list:
        if type(keyitem)==type(0):      #single item as key
            key=x[keyitem]
            value=tuple(x[:keyitem]+x[keyitem+1:])
        else:                           #
            key=tuple([x[i] for i in keyitem])
            value=tuple([x[i] for i in range(len(list)) if i not in keyitem])
        if len(value) == 1:      #single value
            value=value[0]
        if not dict.has_key(key):
            if alwayslist:
                dict[key] = [value,]
            else:
                dict[key]=value
        else:
            current_value=dict[key]
            if type(current_value) != type([]):
                current_value=[current_value,]
            current_value.append(value)
            dict[key]=current_value
    return dict

def list_nondup(list):
    x={}
    for item in list:
        x[item]=None
    return x.keys()

def listsort(list, by, reverse=False,cmp=None, key=None):
    '''Given list is a list of sub(list/tuple.)
       Return a new list sorted by the ith(given from "by" item)
       item of each sublist.'''
    new_li = [(x[by],x) for x in list]
    new_li.sort(cmp=cmp, key=key, reverse=reverse)
    return [x[1] for x in new_li]

def list_itemcnt(list):
    '''Return number of occurrence for each type of item in the list.'''
    x={}
    for item in list:
        if x.has_key(item):
            x[item]+=1
        else:
            x[item]=1
    return [(i,x[i]) for i in x]

def alwayslist(value):
    """If input value if not a list/tuple type, return it as a single value list."""
    if value is None:
        return []
    if type(value) in (types.ListType, types.TupleType):
        return value
    else:
        return [value]

#===============================================================================
# File Utility functions
#===============================================================================
def anyfile(infile, mode='r'):
    '''
    return a file handler with the support for gzip/zip comppressed files
    if infile is a two value tuple, then first one is the compressed file;
      the second one is the actual filename in the compressed file.
      e.g., ('a.zip', 'aa.txt')

    '''
    if type(infile) is types.TupleType:
        infile, rawfile = infile[:2]
    else:
        rawfile = os.path.splitext(infile)[0]
    filetype = os.path.splitext(infile)[1].lower()
    if filetype == '.gz':
        import gzip
        in_f = gzip.GzipFile(infile, 'r')
    elif filetype == '.zip':
        import zipfile
        in_f = zipfile.ZipFile(infile, 'r').open(rawfile, 'r')
    else:
        in_f = file(infile, mode)
    return in_f

def tabfile_tester(datafile, header=1, sep='\t'):
    reader = csv.reader(anyfile(datafile),delimiter=sep)
    lineno = 0
    try:
        for i in range(header):
            reader.next()
            lineno+=1

        for ld in reader:
            lineno+=1
    except:
        print "Error at line number:", lineno
        raise

def tabfile_feeder(datafile, header=1, sep='\t', includefn=None, coerce_unicode=True):
    '''a generator for each row in the file.'''

    reader = csv.reader(anyfile(datafile),delimiter=sep)
    lineno = 0
    try:
        for i in range(header):
            reader.next()
            lineno += 1

        for ld in reader:
            if not includefn or includefn(ld):
                lineno += 1
                if coerce_unicode:
                    yield [unicode(x) for x in ld]
                else:
                    yield ld
    except:
        print "Error at line number:", lineno
        raise

def tab2list(datafile, cols, **kwargs):
    if os.path.exists(datafile):
        if type(cols) is type(1):
            return [ld[cols] for ld in tabfile_feeder(datafile, **kwargs)]
        else:
            return [listitems(ld, *cols) for ld in tabfile_feeder(datafile, **kwargs)]
    else:
        print 'Error: missing "%s". Skipped!' % os.path.split(datafile)[1]
        return {}

def tab2dict(datafile, cols, key, alwayslist=False, **kwargs):
    if type(datafile) is types.TupleType:
        _datafile = datafile[0]
    else:
        _datafile = datafile
    if os.path.exists(_datafile):
        return list2dict([listitems(ld, *cols) for ld in tabfile_feeder(datafile, **kwargs)], key, alwayslist=alwayslist)
    else:
        print 'Error: missing "%s". Skipped!' % os.path.split(_datafile)[1]
        return {}

def file_merge(infiles, outfile=None, header=1,verbose=1):
    '''merge a list of input files with the same format.
       if header will be removed from the 2nd files in the list.
    '''
    outfile = outfile or '_merged'.join(os.path.splitext(infiles[0]))
    out_f, outfile = safewfile(outfile)
    if verbose:
        print "Merging..."
    cnt = 0
    for i, fn in enumerate(infiles):
        print os.path.split(fn)[1],'...',
        line_no = 0
        in_f = anyfile(fn)
        if i > 0:
            for k in range(header):
                in_f.readline()
        for line in in_f:
            out_f.write(line)
            line_no += 1
        in_f.close()
        cnt += line_no
        print line_no
    out_f.close()
    print "="*20
    print "Done![total %d lines output]" % cnt


#===============================================================================
# Dictionary Utility functions
#===============================================================================
def value_convert(_dict, fn, traverse_list=True):
    '''For each value in _dict, apply fn and then update
       _dict with return the value.
       if traverse_list is True and a value is a list,
       apply fn to each item of the list.
    '''
    for k in _dict:
        if traverse_list and type(_dict[k]) is types.ListType:
            _dict[k] = [fn(x) for x in _dict[k]]
        else:
            _dict[k] = fn(_dict[k])
    return _dict

def dict_convert(_dict, keyfn=None, valuefn=None):
    '''Return a new dict with each key converted by keyfn (if not None),
       and each value converted by valuefn (if not None).
    '''
    if keyfn is None and valuefn is not None:
        for k in _dict:
            _dict[k] = valuefn(_dict[k])
        return _dict

    elif keyfn is not None:
        out_dict = {}
        for k in _dict:
            out_dict[keyfn(k)] = valuefn(_dict[k]) if valuefn else _dict[k]
        return out_dict
    else:
        return _dict

def updated_dict(_dict, attrs):
    '''Same as dict.update, but return the updated dictionary.'''
    out = _dict.copy()
    out.update(attrs)
    return out

def merge_dict(dict_li, attr_li, missingvalue=None):
    '''
    Merging multiple dictionaries into a new one.
    Example:
    In [136]: d1 = {'id1': 100, 'id2': 200}
    In [137]: d2 = {'id1': 'aaa', 'id2': 'bbb', 'id3': 'ccc'}
    In [138]: merge_dict([d1,d2], ['number', 'string'])
    Out[138]:
    {'id1': {'number': 100, 'string': 'aaa'},
     'id2': {'number': 200, 'string': 'bbb'},
     'id3': {'string': 'ccc'}}
    In [139]: merge_dict([d1,d2], ['number', 'string'], missingvalue='NA')
    Out[139]:
    {'id1': {'number': 100, 'string': 'aaa'},
     'id2': {'number': 200, 'string': 'bbb'},
     'id3': {'number': 'NA', 'string': 'ccc'}}
    '''
    dd = dict(zip(attr_li, dict_li))
    key_set = set()
    for attr in dd:
        key_set = key_set | set(dd[attr])

    out_dict = {}
    for k in key_set:
        value = {}
        for attr in dd:
            if k in dd[attr]:
                value[attr] = dd[attr][k]
            elif missingvalue is not None:
                value[attr] = missingvalue
        out_dict[k] = value
    return out_dict

def normalized_value(value, sort=True):
    '''Return a "normalized" value:
           1. if a list, remove duplicate and sort it
           2. if a list with one item, convert to that single item only
           3. if a list, remove empty values
           4. otherwise, return value as it is.
    '''
    if type(value) is types.ListType:
        value = [x for x in value if x]   #remove empty values
        try:
            _v = set(value)
        except TypeError:
            #use alternative way
            _v = [json.decode(x) for x in set([json.encode(x) for x in value])]
        if sort:
            _v = sorted(_v)
        else:
            _v = list(_v)
        if len(_v) == 1:
            _v = _v[0]
    else:
        _v = value

    return _v

def dict_nodup(_dict, sort=True):
    for k in _dict:
        _dict[k] = normalized_value(_dict[k], sort=sort)
    return _dict

def dict_attrmerge(dict_li, removedup=True, sort=True, special_fns={}):
    '''
        dict_attrmerge([{'a': 1, 'b':[2,3]},
                        {'a': [1,2], 'b':[3,5], 'c'=4}])
        sould return
             {'a': [1,2], 'b':[2,3,5], 'c'=4}

        special_fns is a dictionary of {attr:  merge_fn}
         used for some special attr, which need special merge_fn
         e.g.,   {'uniprot': _merge_uniprot}
    '''
    out_dict = {}
    keys = []
    for d in dict_li:
        keys.extend(d.keys())
    keys = set(keys)
    for k in keys:
        _value = []
        for d in dict_li:
            if d.get(k, None):
                if type(d[k]) is types.ListType:
                    _value.extend(d[k])
                else:
                    _value.append(d[k])
        if len(_value) == 1:
            out_dict[k] = _value[0]
        else:
            out_dict[k] = _value

        if k in special_fns:
            out_dict[k] = special_fns[k](out_dict[k])

    if removedup:
        out_dict = dict_nodup(out_dict, sort=sort)
    return out_dict

def dict_apply(dict, key, value, sort=True):
    '''

    '''
    if key in dict:
        _value = dict[key]
        if type(_value) is not types.ListType:
            _value = [_value]
        if type(value) is types.ListType:
            _value.extend(value)
        else:
            _value.append(value)
    else:
        _value = value

    dict[key] = normalized_value(_value, sort=sort)

def dict_to_list(gene_d):
    '''return a list of genedoc from genedoc dictionary and
       make sure the "_id" field exists.s
    '''
    doc_li = [updated_dict(gene_d[k], {'_id': str(k)}) for k in sorted(gene_d.keys())]
    return doc_li

