#encoding=utf-8
from datetime import datetime
import tempfile
import mimetypes

from fileflow.storage_drivers import S3StorageDriver

import boto
from moto import mock_s3



@mock_s3
def test_temporaryfile(file):


    bucket_name = 's3storagesdrivertest'
    conn = boto.connect_s3()
    conn.create_bucket(bucket_name)
    driver = S3StorageDriver('','',bucket_name)
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    bucket = conn.get_bucket(bucket_name)

    key = bucket.new_key('test1')
    file.write(u'abč'.encode('utf-8'))
    file.seek(0)

    driver.write_from_stream('abc','123',datetime(2017,10,1), file, content_type=None)
    
    print('Key content type is {}'.format(key.content_type))

file1 = tempfile.TemporaryFile()
print('--------------------- testing tempfile')
test_temporaryfile(file1)

print('--------------------- testing named tempfile')
file2 = tempfile.NamedTemporaryFile()
test_temporaryfile(file2)