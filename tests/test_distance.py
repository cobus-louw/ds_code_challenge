import logging

from cptcc.distance import filter_lon_lat

logger = logging.getLogger(__name__)


def test_filter_lon_lat():
    (lat, long, lat_min, lat_max, long_min, long_max) = (1, 2, 0, 4, 0, 6)
    assert filter_lon_lat(lat, long, lat_min, lat_max,
                          long_min, long_max) == True

    (lat, long, lat_min, lat_max, long_min, long_max) = (1, 3, 0, 4, 0, 2)
    assert filter_lon_lat(lat, long, lat_min, lat_max,
                          long_min, long_max) == False
