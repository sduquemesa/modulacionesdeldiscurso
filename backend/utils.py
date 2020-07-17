"""Some helper functions



@Author: Sebastian Duque Mesa

"""

import numpy as np

def bbox_centroid(bounding_box: list) -> list:
    '''Calculates the centroid of a bounding box'''

    # list of unique bbox longitudes
    longs = np.unique( [point[0] for point in bounding_box])
    # list of unique bbox latitudes
    lats =  np.unique( [point[1] for point in bounding_box])
    
    # Centroid is the average of longs and lats
    centroid_long = np.sum(longs)/2
    centroid_lat = np.sum(lats)/2

    return [centroid_long,centroid_lat]