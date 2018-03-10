#!venv/bin/python

import os
import sqlite3
import logging
import io
import hashlib
import concurrent.futures

import boto3
from botocore.exceptions import ClientError

import crawler
import util

logger = logging.getLogger('archive')

class Archive:
    db_filename = os.path.join(os.path.dirname(os.path.abspath(__file__)),'metadata.sqlite')
    chunk_size = 268435456 # 256 MB

    def __init__(self):
        self.settings = util.Settings()
        self.encrypt = util.Encrypt(self.settings['encryption-token'])

        self.s3 = boto3.client('s3', region_name='us-east',
                               endpoint_url=self.settings['s3-url'],
                               aws_access_key_id=self.settings['s3-access-key'],
                               aws_secret_access_key=self.settings['s3-secret-key'])
                               
        if not os.path.exists(self.db_filename):
            try:
                b = io.BytesIO()
                self.s3.download_fileobj(Bucket=self.settings['s3-bucket'],
                                         Key='metadata.sqlite',
                                         Fileobj=b)
                with open(db_filename, 'wb') as f:
                    f.write(self.encrypt.decode(b.getvalue()))
            except ClientError:
                logger.warning('metadata.sqlite does not exist. making a new one')
        with sqlite3.connect(self.db_filename) as db:
            db.execute('CREATE TABLE IF NOT EXISTS files (path, size, type, date_modified, link_path, sha256sum, chunk_checksums)')
            db.execute('CREATE UNIQUE INDEX IF NOT EXISTS path_index on files (path)')
        db.close()

    def close(self):
        with open(self.db_filename, 'rb') as f:
            data = f.read()
        data = io.BytesIO(self.encrypt.encode(data))
        self.s3.upload_fileobj(Fileobj=data,
                               Bucket=self.settings['s3-bucket'],
                               Key='metadata.sqlite')

    def upload_one(self, filename):
        """Upload a single file"""
        if not os.path.isfile(filename):
            logger.error('cannot backup %s: not a file or link', filename)
        db = sqlite3.connect(self.db_filename)
        try:
            cur = db.cursor()
            cur.execute('SELECT count(*) FROM files WHERE path = ?', (filename,))
            ret = cur.fetchall()
            if ret[0][0] > 0:
                logger.info('already uploaded: %s', filename)
                return
            type = 'link' if os.path.islink(filename) else 'file'
            date_modified = util.get_date_modified(filename)
            if type == 'link':
                real_path = os.readlink(filename)
                if not real_path.startswith('/'):
                    real_path = os.path.join(os.path.dirname(filename), real_path)
                with db:
                    cur.execute('INSERT INTO files (path, size, type, date_modified, link_path, sha256sum, chunk_checksums) values (?,0,"link",?,?,"","")',
                                (filename, date_modified, real_path))
                logger.info('link: %s', filename)
            else: # this is a real file
                size = os.path.getsize(filename)
                total_cksm = util.sha512sum(filename)
                chunk_cksms = []
                if size > self.chunk_size:
                    # make chunks
                    with open(filename,'rb') as f:
                        chunk = f.read(self.chunk_size)
                        while chunk:
                            cksm = hashlib.sha512(chunk).hexdigest()
                            try:
                                # check if already exists, which can happen if we stopped
                                # in the middle of a large file
                                self.s3.head_object(Bucket=self.settings['s3-bucket'],
                                                    Key=cksm)
                            except ClientError:
                                # need to upload
                                data = io.BytesIO(self.encrypt.encode(chunk))
                                self.s3.upload_fileobj(Fileobj=data,
                                                       Bucket=self.settings['s3-bucket'],
                                                       Key=cksm)
                            chunk_cksms.append(cksm)
                            chunk = f.read(self.chunk_size)
                else:
                    with open(filename,'rb') as f:
                        data = f.read()
                    data = io.BytesIO(self.encrypt.encode(data))
                    self.s3.upload_fileobj(Fileobj=data,
                                           Bucket=self.settings['s3-bucket'],
                                           Key=total_cksm)
                with db:
                    cur.execute('INSERT INTO files (path, size, type, date_modified, link_path, sha256sum, chunk_checksums) values (?,?,"file",?,"",?,?)',
                                (filename, size, date_modified, total_cksm, ','.join(chunk_cksms)))
                logger.info('uploaded: %s', filename)
        finally:
            db.close()

    def upload_many(self, path):
        """Upload a path (file or directory)"""
        if not os.path.isdir(path):
            self.upload_one(path)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                for r in executor.map(self.upload_one, crawler.generate_files(path)):
                    pass

    def restore_one(self, filename, output):
        """Restore a single file"""
        db = sqlite3.connect(self.db_filename)
        try:
            cur = db.cursor()
            cur.execute('SELECT * FROM files WHERE path = ?', (filename,))
            ret = cur.fetchall()
            if not ret:
                raise Exception('file not found: %s' % filename)
            for path, size, type, date_modified, link_path, sha256sum, chunk_checksums, in ret:
                pass # loop variables exist after the loop

            try:
                if not os.path.isdir(output):
                    os.makedirs(os.path.basename(output))
            except OSError:
                pass
            if type == 'link':
                os.symlink(output, link_path)
            else:
                if chunk_checksums:
                    with open(output, 'wb') as f:
                        for cksm in chunk_checksums.split(','):
                            try:
                                b = io.BytesIO()
                                self.s3.download_fileobj(Bucket=self.settings['s3-bucket'],
                                                         Key=cksm,
                                                         Fileobj=b)
                                f.write(self.encrypt.decode(b.getvalue()))
                            except ClientError:
                                raise Exception('chunk for file not found: %s' % filename)
                else:
                    try:
                        b = io.BytesIO()
                        self.s3.download_fileobj(Bucket=self.settings['s3-bucket'],
                                                 Key=sha256sum,
                                                 Fileobj=b)
                        with open(output, 'wb') as f:
                            f.write(self.encrypt.decode(b.getvalue()))
                    except ClientError:
                        raise Exception('chunk for file not found: %s' % filename)
            util.set_date_modified(output, date_modified)
        finally:
            db.close()

    def restore_many(self, path, output):
        """Restore a path (file or directory)"""
        db = sqlite3.connect(self.db_filename)
        try:
            cur = db.cursor()
            cur.execute('SELECT * FROM files WHERE path like ?', (filename+'%',))
            ret = cur.fetchall()
            if not ret:
                raise Exception('file/directory not found: %s' % filename)
            for f in ret:
                common = os.path.commonprefix([f,path])
                extra = f[len(common)-1:]
                self.restore_one(f, output+extra)
        finally:
            db.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='s3 archiver')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--upload', action='store_true', default=False, help='Upload mode')
    group.add_argument('--restore', default=None, help='Restore dir')
    parser.add_argument('paths', default=[], action='append', help='paths to upload/restore')
    args = parser.parse_args()
    ar = Archive()
    try:
        if args.upload:
            for p in args.paths:
                ar.upload_many(p)
        else:
            for p in args.paths:
                output = os.path.join(args.restore, os.path.basename(p))
                ar.restore_many(p, output)
    finally:
        ar.close()
    