import boto3
import json
import yaml
import os
import logging
import pandas as pd
import geopandas as gpd
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
    def get_geojson(self, key, resolution=8):
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

        return records

    @timeit
    def get_geojson_df(self, key, resolution=8):
        '''
        A function to extract geojson records from s3 with a given resolution

        Parameters
        ----------
        resolution : int

        Returns
        -------
        list[dict]: A list of geojson records
        '''
        return pd.read_json(self.get_geojson(key, resolution), lines=True, precise_float=True)

    @timeit
    def get_geojson_gdf(self, key, resolution=8):
        '''
        A function to extract geojson records from s3 with a given resolution

        Parameters
        ----------
        resolution : int

        Returns
        -------
        list[dict]: A list of geojson records
        '''
        return gpd.read_file(self.get_geojson(key, resolution))


def main():
    load_env_variables()
    with open('config.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    data_loader = CPTDataLoader(config['bucket'])
    data_loader.get_geojson_records(config['q1data'])


if __name__ == '__main__':
    main()
