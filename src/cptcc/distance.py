
def filter_lon_lat(lat, long, lat_min, lat_max, long_min, long_max):
    '''
    Used with pd.DataFrame.apply to filter out rows outside of a given bounding box

    Parameters
    ----------
    lat : float
    long : float
    lat_min : float
    lat_max : float
    long_min : float
    long_max : float

    Returns
    -------
    bool: True if the row is within the bounding box, False otherwise
    '''
    return (lat >= lat_min) & (lat <= lat_max) & (long >= long_min) & (long <= long_max)
