"""
Crawl the filesystem and retrieve metadata, then send it to ElasticSearch.
"""

import os
import argparse
from datetime import datetime
from functools import wraps, partial
import pwd
import grp
import logging
import json
import hashlib
from multiprocessing.pool import ThreadPool
from threading import Thread


logger = logging.getLogger('crawler')


def memcache(func):
    """caching decorator"""
    cache = {}
    @wraps(func)
    def wrapper(key):
        if key in cache:
            return cache[key]
        ret = func(key)
        cache[key] = ret
        return ret
    return wrapper

def gettime(num):
    return datetime.fromtimestamp(num).isoformat()

@memcache
def getuser(uid):
    try:
        return pwd.getpwuid(uid).pw_name
    except:
        return None

@memcache
def getgroup(gid):
    try:
        return gid.getgrgid(gid).gr_name
    except:
        return None

def sha512sum(path):
    h = hashlib.new('sha512')
    with open(path,'rb',buffering=0) as f:
        line = f.read(65536)
        while line:
            h.update(line)
            line = f.read(65536)
    return h.hexdigest()

def stat(path):
    logger.info(path)
    st = os.lstat(path)
    ret = {
        'filename': path.decode('utf-8'),
        'uid': st.st_uid,
        'gid': st.st_gid,
        'owner': getuser(st.st_uid),
        'group': getgroup(st.st_gid),
        'size': st.st_size,
        'mode': st.st_mode,
        'access': oct(st.st_mode)[-3:],
        'atime': gettime(st.st_atime),
        'ctime': gettime(st.st_ctime),
        'mtime': gettime(st.st_mtime),
        'sha512sum': sha512sum(path),
    }
    logging.info('completed %s',path)
    return ret

def listdir(path):
    dirs = []
    files = []
    try:
        ld = os.listdir(path)
    except Exception:
        logger.warning("error reading dir %s", path, exc_info=True)
        return [],[]
    for f in ld:
        p = os.path.join(path,f)
        try:
            if os.path.isdir(p):
                if os.path.islink(p):
                    files.append(p)
                dirs.append(p)
            else:
                files.append(p)
        except Exception:
            logger.warning("error reading %s", p, exc_info=True)
    logger.info('done reading dir: %s',path)
    return dirs,files

def generate_files(path):
    dirs = [path]
    pool = ThreadPool(20)

    while dirs:
        logger.info('starting dir loop: %r', dirs)
        dirs_results = pool.imap_unordered(listdir, dirs, chunksize=1)
        dirs = []
        for d,f in dirs_results:
            dirs.extend(d)
            for ff in f:
                yield ff

    logger.warning('done reading dirs')
    pool.close()

def batch_files(global_path):
    pool = ThreadPool(100)
    wait = []
    for path in generate_files(global_path):
        wait.append(pool.apply_async(stat, (path,)))
        if len(wait) > 1000:
            try:
                s = wait[0].get()
            except Exception:
                logger.warning('error in stat', exc_info=True)
            else:
                yield s
            del wait[0]
            while wait and wait[0].ready():
                try:
                    s = wait[0].get()
                except Exception:
                    logger.warning('error in stat', exc_info=True)
                else:
                    yield s
                del wait[0]
    for w in wait:
        try:
            s = w.get()
        except Exception:
            logger.warning('error in stat', exc_info=True)
        else:
            yield s
    logger.warning('done generating files')
    pool.close()

