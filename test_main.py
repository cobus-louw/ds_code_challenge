import json
import pytest
import pathlib
import logging
from main import CPTDataLoader
import yaml
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
    with open(f"{path_to_file}/data/city-hex-polygons-8.geojson") as f:
        return json.load(f)


def test_get_geojson_records(data_loader, q1_validation_data):
    s3_extraction = data_loader.get_geojson_records(
        'city-hex-polygons-8-10.geojson', resolution=8)
    # remove resolution key from properties
    [r['properties'].pop('resolution') for r in s3_extraction]
    assert s3_extraction == q1_validation_data['features']
