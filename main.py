import boto3
import yaml
import os
import logging
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
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

    @timeit
    def get_service_requests_df(self, key='sr.csv.gz'):
        '''
        A function to extract service requests from s3

        Parameters
        ----------
        key : str

        Returns
        -------
        pd.DataFrame: A dataframe of service requests
        '''
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return pd.read_csv(obj['Body'], compression='gzip', index_col=0)

    @timeit
    def assign_sr_to_gdf(self, gdf, sr_df):
        '''
        A function to assign service requests to a given geojson dataframe

        Parameters
        ----------
        gdf : geopandas.GeoDataFrame
        sr_df : pd.DataFrame

        Returns
        -------
        geopandas.GeoDataFrame: A geodataframe with service requests assigned
        '''

        # create a Point geometry column in the pandas DataFrame
        geometry = [Point(xy) for xy in zip(sr_df.longitude, sr_df.latitude)]
        df1 = gpd.GeoDataFrame(sr_df, crs=gdf.crs, geometry=geometry)

        # join the two GeoDataFrames based on whether the points fall within the polygon boundaries
        joined = gpd.sjoin(df1, gdf, how='left', predicate='within')

        joined.drop(['centroid_lat',
        'index_right',
        'centroid_lon',
        'geometry'], axis=1, inplace=True)

        joined.rename(columns={'index': 'h3_level8_index'}, inplace=True)
        joined['h3_level8_index'].fillna('0', inplace=True) # fill na with 0
        num_records_failed = joined['h3_level8_index'].value_counts()['0']
        percent_failed = float(num_records_failed / len(joined) * 100)
        if percent_failed > 50:
            raise Exception(f'Failed to assign {percent_failed:.2f}% of service requests to a hexagon')
        logger.info(f'Failed to assign {num_records_failed} records ({percent_failed:.2f}%) of service requests to a hexagon') 
        
        return joined.astype({'reference_number': float, 'latitude': float, 'longitude': float})





def main():
    load_env_variables()
    with open('config.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    data_loader = CPTDataLoader(config['bucket'])
    data_loader.get_geojson_records(config['q1data'])


if __name__ == '__main__':
    main()
