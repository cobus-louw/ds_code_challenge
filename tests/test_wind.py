import pytest
import logging
from cptcc.wind import WindData

logger = logging.getLogger(__name__)

URL = 'https://www.capetown.gov.za/_layouts/OpenDataPortalHandler/DownloadHandler.ashx?DocumentName=Wind_direction_and_speed_2020.ods&DatasetDocument=https%3A%2F%2Fcityapps.capetown.gov.za%2Fsites%2Fopendatacatalog%2FDocuments%2FWind%2FWind_direction_and_speed_2020.ods'


def test_get_df():
    WindData.get_df(URL)


@pytest.fixture
def df():
    return WindData.get_df(URL)


def test_clean_wind_data(df):
    df_clean = WindData.clean_wind_data(df)
    assert df_clean.shape == (8784, 15)
    assert df_clean.index.dtype == 'int64'


def test_clean_column_names():
    col_names = ['Date & Time', 'Wind Speed', 'Wind Direction', 'Site']
    col_names_clean = WindData._clean_column_names(col_names)
    assert col_names_clean == ['date_time',
                               'wind_speed', 'wind_direction', 'site']


def test_remove_duplicates():
    my_list = ['a', 'b', 'b', 'c', 'c', 'c']
    my_list_clean = WindData._remove_duplicates(my_list)
    assert my_list_clean == ['a', 'b', 'c']
