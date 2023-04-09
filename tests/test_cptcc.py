import pytest
import pathlib
import logging
import yaml
import geopandas as gpd
import pandas as pd
from geopandas.testing import assert_geodataframe_equal
from pandas.testing import assert_frame_equal
from dotenv import load_dotenv

from cptcc.cptcc import CPTDataLoader

logger = logging.getLogger(__name__)

load_dotenv()
path_to_file = pathlib.Path(__file__).parent.absolute()


@pytest.fixture
def data_loader():
    with open('config.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return CPTDataLoader(config['bucket'])


@pytest.fixture
def q1_validation_data():
    return gpd.read_file('./data/city-hex-polygons-8.geojson')


def test_get_geojson_records(data_loader, q1_validation_data):
    s3_extraction = data_loader.get_geojson_gdf(
        'city-hex-polygons-8-10.geojson', resolution=8)
    s3_extraction.drop(columns=['resolution'], inplace=True)
    assert_geodataframe_equal(s3_extraction, q1_validation_data)


def test_assign_sr_to_gdf(data_loader):
    gdf = gpd.read_file('./data/city-hex-polygons-8.geojson')
    sr_df = pd.read_csv('./data/sr.csv.gz', compression='gzip', index_col=0)
    val_df = pd.read_csv('./data/sr_hex.csv.gz', compression='gzip')
    sr_gdf = data_loader.assign_sr_to_gdf(gdf, sr_df)
    assert_frame_equal(sr_gdf.drop('h3_level8_index', axis=1),
                       val_df.drop('h3_level8_index', axis=1))

    val_hex = val_df['h3_level8_index']
    sr_df_hex = sr_gdf['h3_level8_index']

    val_hex_hash = val_hex.apply(hash)
    sr_df_hex_hash = sr_df_hex.apply(hash)

    diff = val_hex_hash - sr_df_hex_hash
    diff = diff.apply(lambda x: 1 if x != 0 else 0)
    num_diff = diff.sum()
    percentage_diff = (num_diff / len(diff)) * 100

    logger.info(f'Percentage of hexes that are different: {percentage_diff}')

    assert percentage_diff < 0.01
