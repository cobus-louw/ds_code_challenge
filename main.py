import boto3
import json
import yaml
import os
import logging
logger = logging.getLogger(__name__)

from utils import timeit, load_env_variables


class CPTDataLoader(object):
    def __init__(self, bucket):
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        self.s3 = boto3.client('s3',
                               aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)
        self.bucket = bucket

    @timeit
    def get_geojson_records(self, key, resolution=8):
        '''
        A function to extract geojson records from s3 with a given resolution

        Parameters
        ----------
        resolution : int

        Returns
        -------
        list[dict]: A list of geojson records
        '''
        query = f"SELECT * FROM S3Object[*].features[*] s where s.properties.resolution = {resolution}"

        input_serialization = {'JSON': {'Type': 'DOCUMENT'}}
        output_serialization = {'JSON': {}}
        resp = self.s3.select_object_content(
            Bucket=self.bucket,
            Key=key,
            ExpressionType='SQL',
            Expression=query,
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization,
        )

        records = ''
        for event in resp['Payload']:
            if 'Records' in event:
                records += event['Records']['Payload'].decode('utf-8')
            elif 'Stats' in event:
                statsDetails = event['Stats']['Details']
                logger.debug("Stats details bytesScanned: ")
                logger.debug(statsDetails['BytesScanned'])
                logger.debug("Stats details bytesProcessed: ")
                logger.debug(statsDetails['BytesProcessed'])
                logger.debug("Stats details bytesReturned: ")
                logger.debug(statsDetails['BytesReturned'])

        return [json.loads(record) for record in records.splitlines()]


def main():
    load_env_variables()
    with open('config.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    data_loader = CPTDataLoader(config['bucket'])
    data_loader.get_geojson_records(config['q1data'])


if __name__ == '__main__':
    main()
