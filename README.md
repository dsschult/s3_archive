# s3_archive
Upload files to S3 in compressed, encrypted archives or bundles.

## Setup
Run `./setup.sh` to set up the python virtualenv.

Run `./upload.sh` to run the uploader under the virtualenv.

Run `./restore.sh` to run the restorer under the virtualenv.

## How it works
Each file is split into 256MB chunks, compressed, encrypted, and
uploaded to the S3 target under the sha512 checksum of the original
chunk before compression. If a file lives in only one chunk, the
name of the file in S3 is the checksum of the entire file.

Metadata is stored an SQLite database. For each file, the following
is stored:

* file path including name
* size
* date modified
* sha512 checksum
* chunk checksums as a comma-separated list, in order

Metadata is stored in the S3 bucket under the name "metadata.sqlite".

## Backup settings
Settings are stored in a json config file in the current directory
under the name "settings.json".

Some common settings are:

```json
{
    "s3-access-key": "the S3 access key",
    "s3-secret-key": "the S3 secret key",
    "s3-url": "the url to the S3 storage",
    "s3-bucket": "the name of the S3 bucket to use",
    "encryption-token": "the secret key to use for encryption",
    "backup-directories": [
        "a list of directories to back up"
    ]
}
``` 

## Restoring
To restore a file or directory, run `./restore.sh` while
giving the paths on the command line, one to restore from,
and one to restore to.

As an example,

```bash
./restore.sh /data/foo/bar /data2/baz/
```

will restore the directory `bar` and all its contents inside
the directory `baz`.
