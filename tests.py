import unittest
import os
import tempfile
import shutil
import json

from cryptography.fernet import Fernet

import util

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

if __name__ == '__main__':
    unittest.main()
