#!/bin/sh
python3 -m virtualenv -p python3 venv
. venv/bin/activate
pip install boto3 zstd cryptography
