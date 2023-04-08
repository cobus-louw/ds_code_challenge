import pytest
import logging
from cptcc.anonymize import add_distance, add_random_distance

logger = logging.getLogger(__name__)


def test_add_distance():
    lon_start = -122
    lat_start = 37

    distance = 10_000
    angle = 45

    lon_end, lat_end = add_distance(lon_start, lat_start, distance, angle)

    logger.debug(f'lon_end: {lon_end}, lat_end: {lat_end}')

    assert lon_end == pytest.approx(-121.9205, 0.001)
    assert lat_end == pytest.approx(37.0637, 0.001)


def test_add_random_distance():
    lon_start = -122
    lat_start = 37

    min_distance = 100
    max_distance = 100

    lon_end, lat_end = add_random_distance(
        lon_start, lat_start, min_distance, max_distance)

    logger.debug(f'lon_end: {lon_end}, lat_end: {lat_end}')

    assert lon_end == pytest.approx(-122.0009, 0.001)
    assert lat_end == pytest.approx(37.0009, 0.001)
