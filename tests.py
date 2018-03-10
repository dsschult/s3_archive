import unittest
import os
import sys
import tempfile
import shutil
import json
import random
import string
import logging
import subprocess
from unittest.mock import patch, MagicMock

from cryptography.fernet import Fernet
import boto3
from botocore.exceptions import ClientError

import util
import archive

class TestUtil(unittest.TestCase):
    def setUp(self):
        curdir = os.getcwd()
        tmpdir = tempfile.mkdtemp(dir=curdir)
        os.chdir(tmpdir)
        def clean():
            os.chdir(curdir)
            shutil.rmtree(tmpdir)
        self.addCleanup(clean)

    def test_encrypt(self):
        data = os.urandom(10000)
        e = util.Encrypt(Fernet.generate_key())
        tmp = e.encode(data)
        self.assertIsInstance(tmp, bytes)
        data_out = e.decode(tmp)
        self.assertEqual(data, data_out)
    
    def test_encrpt_getkey(self):
        k = util.Encrypt.get_key()
        self.assertIsInstance(k, str)

    def test_settings(self):
        util.Settings.filename = 'settings-test.json'
        with self.assertRaises(Exception):
            s = util.Settings()
        with self.assertRaises(Exception):
            s = util.Settings()
        data = {}
        with open(util.Settings.filename) as f:
            data = json.load(f)
        data['s3-access-key'] = 'foo'
        with open(util.Settings.filename, 'w') as f:
            json.dump(data, f)
        s = util.Settings()
        self.assertEqual(s['s3-access-key'], 'foo')

    def test_sha512sum(self):
        filename = 'testfile'
        with open(filename, 'wb') as f:
            for _ in range(1024):
                f.write(os.urandom(1024))
        cksm = subprocess.check_output(['sha512sum',filename]).split()[0].decode('utf-8')
        cksm2 = util.sha512sum(filename)
        self.assertEqual(cksm, cksm2)


class TestArchive(unittest.TestCase):
    def setUp(self):
        curdir = os.getcwd()
        tmpdir = tempfile.mkdtemp(dir=curdir)
        os.chdir(tmpdir)
        def clean():
            os.chdir(curdir)
            shutil.rmtree(tmpdir)
        self.addCleanup(clean)

        self.srcdir = os.path.join(tmpdir, 'src')
        self.destdir = os.path.join(tmpdir, 'dest')
        os.mkdir(self.srcdir)
        os.mkdir(self.destdir)
        
        util.Settings.filename = 'settings-test.json'
        self.encryption_token = util.Encrypt.get_key()
        with open('settings-test.json', 'w') as f:
            json.dump({
                "s3-access-key": "keykeykey",
                "s3-secret-key": "secretsecret",
                "s3-url": "foobar",
                "s3-bucket": "backup",
                "encryption-token": self.encryption_token,
                "backup-directories": [
                    self.srcdir
                ]
            }, f)
        archive.Archive.db_filename = os.path.join(tmpdir, 'metadata.sqlite')
        archive.Archive.chunk_size = 10000

    def make_dirs(self, base, N=100, M=100000, lambd=10000):
        """Make test dirs and files"""
        if N < 1:
            return
        for n in range(N):
            name = os.path.join(base,''.join(random.choices(string.ascii_uppercase + string.digits, k=10)))
            if N%4 == 0: # 1/4 dirs
                os.mkdir(name)
                self.make_dirs(os.path.join(base, name), N-10, M, lambd)
            else: # 3/4 files
                with open(name, 'wb') as f:
                    f.write(os.urandom(int(random.expovariate(lambd)*lambd/10*M)))

    @patch('boto3.client', autospec=True)
    def test_init(self, s3_client):
        e = util.Encrypt(self.encryption_token)
        s3_client.return_value.download_fileobj.side_effect = ClientError({},"download_fileobj")
        ar = archive.Archive()
        ar.close()

    @patch('boto3.client', autospec=True)
    def test_upload_one(self, s3_client):
        e = util.Encrypt(self.encryption_token)
        s3_client.return_value.download_fileobj.side_effect = ClientError({},"download_fileobj")
        ar = archive.Archive()
        try:
            filename = 'test'
            data = os.urandom(1000)
            with open(filename, 'wb') as f:
                f.write(data)
            ar.upload_one(filename)
            self.assertEqual(s3_client.return_value.upload_fileobj.call_count, 1)
            data_enc = s3_client.return_value.upload_fileobj.call_args[1]['Fileobj'].getvalue()
            self.assertEqual(data, e.decode(data_enc))
        finally:
            ar.close()

    @patch('boto3.client', autospec=True)
    def test_upload_one_large(self, s3_client):
        e = util.Encrypt(self.encryption_token)
        s3_client.return_value.download_fileobj.side_effect = ClientError({},"download_fileobj")
        s3_client.return_value.head_object.side_effect = ClientError({},"head_object")
        ar = archive.Archive()
        try:
            filename = 'test'
            data = os.urandom(25000)
            with open(filename, 'wb') as f:
                f.write(data)
            ar.upload_one(filename)
            self.assertEqual(s3_client.return_value.upload_fileobj.call_count, 3)
            data_enc = [x[1]['Fileobj'].getvalue() for x in s3_client.return_value.upload_fileobj.call_args_list]
            self.assertEqual(data, b''.join(e.decode(d) for d in data_enc))
        finally:
            ar.close()

    @patch('boto3.client', autospec=True)
    def test_upload_two(self, s3_client):
        e = util.Encrypt(self.encryption_token)
        s3_client.return_value.download_fileobj.side_effect = ClientError({},"download_fileobj")
        ar = archive.Archive()
        try:
            self.make_dirs(self.srcdir, N=2, M=100)
            ar.upload_many(self.srcdir)
            self.assertEqual(s3_client.return_value.upload_fileobj.call_count, 2)
        finally:
            ar.close()
       

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()

