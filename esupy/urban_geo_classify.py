# urban_geo_classify.py (esupy)
# !/usr/bin/env python3
# coding=utf-8

"""
Module to classify lat/long pairs as lying w/i urban or rural Census block area
"""
import pandas as pd
import geopandas as gpd
import shapely.geometry as sgm
from shapely.prepared import prep
from pathlib import Path

datapath = Path(__file__).parent/'census_shp'
# print(datapath)

def urb_intersect(gdf_pt, year):
    """
    Classify lat/long points as urban/rural via intersection with 
    urban polygons from a specified data year {2000, 2010}.
    :param gdf_pt: GeoPandas GeoDatFrame with point geometry values
    :param year: data year of Census urban area/cluster polygons
    """
    urb_2000 = read_census_shp(datapath, "ua99_d00_shp.zip")
    urb_2010 = read_census_shp(datapath, "cb_2012_us_uac10_500k.zip")
    urb_2000.crs = urb_2010.crs  # 2000 cb crs missing; inherit 2010 cb crs
    
    if year == 2000:
        gdf_urb = crs_harmonize(urb_2000)  # convert to WGS84
    elif year == 2010:
        gdf_urb = crs_harmonize(urb_2010)  # convert to WGS84
    else: 
        print(f"Data year {year} unavailable")
        return None
    
    mpu = multipoly_agg(gdf_urb)
    mpu = prep(mpu)  # shapely's quick spatial indexing fn
    gdf_pt['urban'] = gdf_pt['geometry'].apply(lambda x: mpu.intersects(x))
    return gdf_pt

def multipoly_agg(gdf):
    """
    Aggregate geometry col of polygons and/or multipolygons (no other types)
    to a single, unified multipolygon collection object
    :param gdf: GeoPandas geodataframe object
    """
    gdf_mp = gdf[gdf['geometry'].type == 'MultiPolygon'] # important to test this piece on urb_2010
    gdf_p = gdf[gdf['geometry'].type == 'Polygon']
    
    if len(gdf) > (len(gdf_mp) + len(gdf_p)):
        print('GeoDataFrame invalid; contains non-polygon geometry entries')
        return None
    
    # extract polygons from multipolygons & concatenate into list
    mp = [poly for multipoly in gdf_mp['geometry'] for poly in multipoly]
    
    p = list(gdf_p['geometry']) # concatenate polygons into a list    
    multipoly = sgm.MultiPolygon(mp + p)  # join lists & convert to mp
    return multipoly

def read_census_shp(datapath, filename):
    """
    Read in shapefile as gpd geodataframe.
    :param datapath: pathlib Path pointing to shapefile dir
    :param filename: file name string with extension
    """ 
    gdf = gpd.read_file(datapath / filename)
    # check for empty/na geom values
    a = sum(gdf.geometry.is_empty | gdf.geometry.isna())
    if a != 0: print(f"{filename} contains {a} empty and/or NA geometries")
    return gdf

def read_point_data(df):
    """
    Convert df containing Lat & Long columns to gpd.GeoDataFrame
    with geometry column
    :param df: pandas dataframe
    """
    # TO DO: thorough review of lat/long headers
        # identify & modify w/ regex on df.columns? 
        # or just a dictionary across sources?
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude))
    return gdf

def crs_harmonize(gdf):
    """
    Convert coordinate reference systems (CRS) to WGS84 (EPSG:4326), 
    and assign WGS84 as assumed default when missing.
    :param gdf: GeoPandas geodataframe object
    """
    WGS84 = "EPSG:4326"  # EPSG code
    if gdf.crs is None:  # if crs attribute is empty
        gdf = gdf.set_crs(WGS84)
    else:
        gdf = gdf.to_crs(WGS84)
    return gdf
    

if __name__ == "__main__":  
    import time    
    start_time = time.time()
    import stewi
    tri_raw = stewi.getInventoryFacilities('TRI',2019)
    tri_pt = read_point_data(tri_raw)
    tri_pt = crs_harmonize(tri_pt)
    tri_urb = urb_intersect(tri_pt, 2010)
    print("--- %s seconds ---" % (time.time() - start_time)) 


