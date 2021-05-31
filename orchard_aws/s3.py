import pandas as pd
import logging
import boto3

# TODO: Look up what these are
import gzip
from gzip import compress

log = logging.getLogger('S3 Conn')


class s3(object):
    """
        This class reads and writes to S3.
    """
    def __init__(self, access_key, secret_key, bucket=None):
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.client = boto3.client('s3',
                                   aws_access_key_id=self.access_key,
                                   aws_secret_access_key=self.secret_key)

    def df_to_s3(self,
                 df,
                 obj_name,
                 bucket=None,
                 subdirectory=None,
                 gzip=False,
                 encoding='utf-8'):
        data = df.to_csv(index=False, encoding=encosding)
        try:
            key = self.csv_to_s3(data, obj_name, bucket, subdirectory, gzip)
            return key
        except Exception:
            log.error('error sending {0} to {1}/{2}'.format(
                obj_name, bucket, subdirectory))
            return None

    def csv_to_s3(self,
                  data,
                  obj_name,
                  bucket=None,
                  subdirectory=None,
                  gzip=False):
        if bucket is None:
            bucket = self.bucket
        if gzip:
            data = compress(data.encode('utf-8'))
        if subdirectory is None:
            subdirectory = ''
        else:
            subdirectory = subdirectory + '/'
        key = subdirectory + obj_name
        self.client.put_object(Body=data, Bucket=bucket, Key=key)
        log.info('saved {0} to {1}/{2}'.format(obj_name, bucket, subdirectory))
        return key

    def s3_to_df(self, key, bucket=None):
        """
            Parameters
            ----------
            bucket : str
                name of s3 bucket. When None, defualt to self.bucket

            key : str
                path to the file. Usually 'subdirectory/file_name'
        """
        data = self.s3_to_csv(key=key, bucket=bucket)
        try:
            df = pd.read_csv(data['Body'])
            return df
        except ValueError as e:
            log.error("Error getting {0} from {1}: {}".format(key, bucket, e))
            return None

    def s3_to_csv(self, key, bucket=None):
        """
            Parameters
            ----------
            bucket : str
                name of s3 bucket. When None, defualt to self.bucket

            key : str
                path to the file. Usually 'subdirectory/file_name'
        """
        if bucket is None:
            bucket = self.bucket
        data = self.client.get_object(Bucket=bucket, Key=key)
        return data

    def from_s3(self, key, bucket=None):
        if bucket is None:
            bucket = self.bucket
        data = self.client.get_object(Bucket=bucket, Key=key)
        return data['Body']

    def to_s3(self, data, key, bucket=None):
        if bucket is None:
            bucket = self.bucket
        self.client.put_object(Body=data, Bucket=bucket, Key=key)
        return key

    def delete_file(self, key, bucket=None):
        if bucket is None:
            bucket = self.bucket
        self.client.delete_object(Bucket=bucket, Key=key)
        return "Deleted {} from s3 bucket {}".format(key, bucket)
