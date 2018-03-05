import os
import base64
import json
import logging

from cryptography.fernet import Fernet
import zstd

logger = logging.getLogger('util')

class Encrypt:
    def __init__(self, key=None, level=22):
        if not key:
            raise Exception('need an encryption key')
        if isinstance(key, str):
            key = key.encode('utf-8')
        self.f = Fernet(key)
        self.level = level

    @staticmethod
    def get_key():
        return Fernet.generate_key().decode('utf-8')

    def encode(self, data):
        d = self.f.encrypt(zstd.compress(data, self.level))
        return base64.urlsafe_b64decode(d)

    def decode(self, data):
        d = base64.urlsafe_b64encode(data)
        return zstd.decompress(self.f.decrypt(d))


class Settings(dict):
    filename = os.path.join(os.path.dirname(os.path.abspath(__file__)),'settings.json')
    json_options = {
        'indent': 4,
        'separators': (',',': '),
        #'ensure_ascii': False,
    }
    def __init__(self, *args, **kwargs):
        super(Settings, self).__init__(*args, **kwargs)
        if os.path.exists(self.filename):
            with open(self.filename) as f:
                data = json.load(f)
                if data['s3-access-key'] == "the S3 access key":
                    raise Exception('please fill out settings.json')
                self.update(data)
        else:
            data = {
                "s3-access-key": "the S3 access key",
                "s3-secret-key": "the S3 secret key",
                "s3-url": "the url to the S3 storage",
                "s3-bucket": "the name of the S3 bucket to use",
                "encryption-token": Encrypt.get_key(),
                "backup-directories": [
                    "a list of directories to back up"
                ]
            }
            with open(self.filename,'w') as f:
                json.dump(data, f, **self.json_options)
            raise Exception('please fill out settings.json')

    def set(self):
        """Set the current contents of the json file"""
        with open(self.filename,'w') as f:
            json.dump(dict(self), f, **self.json_options)

