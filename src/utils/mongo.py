from mongokit import Connection
from config import (DATA_SRC_SERVER, DATA_SRC_PORT, DATA_SRC_DATABASE,
                    DATA_SRC_MASTER_COLLECTION,
                    DATA_SERVER, DATA_PORT, DATA_DATABASE,
                    DATA_DB_MASTER_COLLECTION)

def get_conn(server, port):
    conn = Connection(server, port)
    return conn

def get_src_conn():
    return get_conn(DATA_SRC_SERVER, DATA_SRC_PORT)

def get_src(name, conn=None):
    conn = conn or get_src_conn()
    return conn[DATA_SRC_DATABASE][name]

def get_src_master(conn=None):
    conn = conn or get_src_conn()
    return conn[DATA_SRC_DATABASE][DATA_SRC_MASTER_COLLECTION]

def get_db_conn():
    return get_conn(DATA_SERVER, DATA_PORT)

def get_db():
    return get_db_conn()[DATA_DATABASE]

def get_db_master(conn=None):
    conn = conn or get_db_conn()
    return conn[DATA_DATABASE][DATA_DB_MASTER_COLLECTION]
