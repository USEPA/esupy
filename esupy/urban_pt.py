# urban_pt.py (esupy)
# !/usr/bin/env python3
# coding=utf-8

"""
Module to classify lat/long pairs as lying within urban or rural Census block area
"""
import geopandas as gpd
import numpy as np
import shapely as sh
import urllib.error
import yaml
from pathlib import Path

datapath = Path(__file__).parent/'data_census'

def urb_intersect(df_pt, year):
    """
    Classify lat/long points as urban, rural, or unspecified 
    via intersection with urban polygons from a user-specified data year.
    :param df: pandas dataframe, with Latitude and Longitude cols
    :param year: data year of Census urban area/cluster polygons
    :return: pandas dataframe with new column of urb_class labels
    """
    gdf_urb = get_census_shp(year)
    mpu = multipoly_agg(gdf_urb) # aggregate to avoid element-wise comparison
    mpu = sh.prepared.prep(mpu)  # shapely's quick spatial indexing fxn
    
    gdf_pt = parse_pt_data(df_pt)
    gdf_pt['urban'] = gdf_pt['geometry'].apply(lambda x: mpu.intersects(x))
    
    # classify 'unspecified' via Lat & Long cols (gpd na/empty check fns won't catch)
    cond = [(gdf_pt['Latitude'].isna() | gdf_pt['Longitude'].isna()),
            (gdf_pt['urban'] == True),
            (gdf_pt['urban'] == False)]
    vals = ['unspecified', 'urban', 'rural']
    gdf_pt['urb_class'] = np.select(cond, vals)
    df_pt = gdf_pt.drop(['geometry','urban'], axis=1)
    return df_pt

def get_census_shp(year):
    """
    Read in shapefile as gpd geodataframe.
    Refer to data_census/README.md for explanation of encoding errors 
    thrown by gpd.read_file() for pre-2015 SHPs
    :param datapath: pathlib Path pointing to shapefile dir
    :param filename: file name string with extension
    """ 
    with open(datapath / 'census_uac_urls.yaml', 'r') as f:
        uac_url = yaml.safe_load(f)
            
    try:
        print(f'Retrieving {year} UAC shapefile from {uac_url[year]}')
        if year in [2010, 2011]:  # data years rely on 2x SHP's
            gdf0 = gpd.read_file(uac_url[year][0])
            gdf1 = gpd.read_file(uac_url[year][1])
            gdf = gdf0.append(gdf1)
            if not gdf.crs == gdf0.crs == gdf1.crs:
                print(f'WARNING Combined {year} gdf CRS is inconsistent')
        else: 
            gdf = gpd.read_file(uac_url[year])
            # gdf = gpd.read_file(uac_url[year], encoding = 'iso-8859-1')
    except KeyError:
        print(f'Census urban area data year {year} unavailable')
    except urllib.error.HTTPError:
        print(f'File unavailable, check Census domain status: \n{uac_url[year]}')
        return None
            
    gdf = crs_harmonize(gdf)  # convert to WGS84
    
    # check for empty/na geom values
    ena = sum(gdf['geometry'].is_empty | gdf['geometry'].isna())
    if ena > 0: print(f"{year} Census SHP has {ena} empty + NA geometries")
    return gdf

def parse_pt_data(df):
    """
    Convert df containing "Latitude" and "Longitude" columns to 
    gpd.GeoDataFrame with geometry column of Points
    :param df: pandas dataframe
    """
    # check for na lat/long values
    pt_na = sum(df['Latitude'].isna() | df['Longitude'].isna()) 
    if pt_na != 0: print(f"Point data contains {pt_na} nan Lat and/or Long values")
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude))
    gdf = crs_harmonize(gdf)
    return gdf

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
    # multipolygons are collections of polygons; extract & concatenate into list
    mp = [poly for multipoly in gdf_mp['geometry'] for poly in multipoly]
    p = list(gdf_p['geometry']) # concatenate single polygons into a list    
    multipoly = sh.geometry.MultiPolygon(mp + p)  # join lists & convert to single mp
    return multipoly

def crs_harmonize(gdf):
    """
    Convert coordinate reference systems (CRS) to WGS84 (EPSG:4326), 
    or if missing, assume WGS84 and assign
    :param gdf: GeoPandas geodataframe object
    """
    WGS84 = "EPSG:4326"  # EPSG code
    if gdf.crs is None:  # if crs attribute is empty
        gdf = gdf.set_crs(WGS84)
    else:
        gdf = gdf.to_crs(WGS84)
    return gdf


if __name__ == "__main__":  
    import stewi
    # import time
    year = 2011 
    pt_raw = stewi.getInventoryFacilities('TRI',year)  # ('TRI', 2019)
    # time_start = time.time()
    pt_urb = urb_intersect(pt_raw, year)
    # print(f'[total] --- {time.time() - time_start} seconds ---')
