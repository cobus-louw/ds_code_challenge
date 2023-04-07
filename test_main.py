import pytest
import pathlib
import logging
import yaml
import geopandas as gpd
from geopandas.testing import assert_geodataframe_equal

from main import CPTDataLoader
from utils import load_env_variables
logger = logging.getLogger(__name__)


path_to_file = pathlib.Path(__file__).parent.absolute()


@pytest.fixture
def data_loader():
    load_env_variables()
    with open('config.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return CPTDataLoader(config['bucket'])


@pytest.fixture
def q1_validation_data():
    return gpd.read_file('./data/city-hex-polygons-8.geojson')


def test_get_geojson_records(data_loader, q1_validation_data):
    s3_extraction = data_loader.get_geojson_records_gdf(
        'city-hex-polygons-8-10.geojson', resolution=8)
    s3_extraction.drop(columns=['resolution'], inplace=True)
    assert_geodataframe_equal(s3_extraction, q1_validation_data)
