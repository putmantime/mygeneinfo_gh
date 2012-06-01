import time
import base64
import os

def ask(prompt,options='YN'):
    '''Prompt Yes or No,return the upper case 'Y' or 'N'.'''
    options=options.upper()
    while 1:
        s=raw_input(prompt+'[%s]' % '|'.join(list(options))).strip().upper()
        if s in options: break
    return s


def timesofar(t0, clock=0):
    '''return the string(eg.'3m3.42s') for the passed real time/CPU time so far
       from given t0 (return from t0=time.time() for real time/
       t0=time.clock() for CPU time).'''
    if clock:
        t = time.clock() - t0
    else:
        t = time.time() - t0
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

