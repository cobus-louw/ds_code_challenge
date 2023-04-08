import geopy
import geopy.distance
import numpy as np


def add_distance(lon, lat, distance, bearing):
    '''
    Takes a longitude and latitude and adds a distance at a given angle

    Parameters
    ----------
    lon : float
    lat : float
    distance : float
    angle : float

    Returns
    -------
    float: A new longitude
    float: A new latitude
    '''

    start_point = geopy.Point(latitude=lat, longitude=lon)
    end_point = geopy.distance.geodesic(
        meters=distance).destination(start_point, bearing)
    return end_point.longitude, end_point.latitude


def add_random_distance(lon, lat, min_distance, max_distance):
    '''
    Used with pd.DataFrame.apply to add a random distance to a given longitude and latitude

    Parameters
    ----------
    lon : float
    lat : float
    min_distance : float
    max_distance : float

    Returns
    -------
    float: A new longitude
    float: A new latitude
    '''
    
    distance = np.random.uniform(min_distance, max_distance)
    bearing = np.random.uniform(0, 360)
    return add_distance(lon, lat, distance, bearing)
