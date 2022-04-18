# context_secondary.py (esupy)
# !/usr/bin/env python3
# coding=utf-8

"""
Module to assign secondary FEDEFL contexts, including release height
and population density (urban/rural).
"""
import logging as log
import urllib.error
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

try:
    import geopandas as gpd
    import shapely as sh
    has_geo_pkgs = True
except ImportError:
    log.info('GeoPandas and/or Shapely were not successfully imported,\n'
             'so esupy.context_secondary is unable to assign an urban/rural '
             'compartment.\n See esupy/README.md for install instructions.')
    has_geo_pkgs = False

datapath = Path(__file__).parent/'data_census'


def classify_height(df):
    """
    Assign release height context label via numeric 'StackHeight' (ft) column.
    :param df: pandas dataframe, with <schema_name> column
    :return: pandas dataframe with new column of cmpt_rh labels
    """
    m_to_ft = 3.28  # feet per meter
    cond = [(df['StackHeight'].isna()),
            (df['StackHeight'] < 4*m_to_ft),    # ~13 ft
            (df['StackHeight'] < 25*m_to_ft),   # ~82 ft
            (df['StackHeight'] < 150*m_to_ft),# ~492 ft
            (df['StackHeight'] >= 150*m_to_ft)]
    cmpt = ['unspecified', 'ground', 'low', 'high', 'very high']
    df['cmpt_rh'] = np.select(cond, cmpt)
    return df

def urb_intersect(df_pt, year):
    """
    Classify lat/long points as urban, rural, or unspecified
    via intersection with Census-defined urban polygons in a given data year.
    :param df: pandas dataframe, with Latitude and Longitude cols
    :param year: data year of Census urban area/cluster polygons
    :return: pandas dataframe with new column of cmpt_urb labels
    """
    gdf_urb = get_census_shp(year)
    mpu = multipoly_agg(gdf_urb) # aggregate to avoid element-wise comparison
    mpu = sh.prepared.prep(mpu)  # shapely's quick spatial indexing fxn

    gdf_pt = parse_pt_data(df_pt)
    gdf_pt['urban'] = gdf_pt['geometry'].apply(lambda x: mpu.intersects(x))

    # identify 'unspecified' via Lat+Long cols (gpd is-na/empty fxns won't catch)
    cond = [(gdf_pt['Latitude'].isna() | gdf_pt['Longitude'].isna()),
            (gdf_pt['urban'] == True),
            (gdf_pt['urban'] == False)]
    cmpt = ['unspecified', 'urban', 'rural']
    gdf_pt['cmpt_urb'] = np.select(cond, cmpt)
    df_pt = pd.DataFrame(gdf_pt.drop(columns=['geometry','urban']))
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
        log.info(f'Retrieving {year} UAC shapefile from url:\n{uac_url[year]}')
        if year < 2015:
            log.info('Before 2015, expect series of GeoPandas WARNINGs that '
                     'utf-8 codec fails for certain strings.\n See Text Encoding '
                     'section of esupy/data_census/README.md for explanation.')
        if year in [2010, 2011]:  # data years rely on 2x SHP's
            gdf0 = gpd.read_file(uac_url[year][0])
            gdf1 = gpd.read_file(uac_url[year][1])
            gdf = gdf0.append(gdf1)
            if not gdf.crs == gdf0.crs == gdf1.crs:
                log.error(f'Combined {year} gdf CRS is inconsistent')
        else:
            gdf = gpd.read_file(uac_url[year])
            # gdf = gpd.read_file(uac_url[year], encoding='iso-8859-1')
            # BUG: should accept 'encoding' kwarg but returns error as of 04/15/22
    except KeyError:
        log.error(f'Census urban area data year {year} unavailable')
    except urllib.error.HTTPError:
        log.error('File unavailable, check Census domain status:\n' +
                  uac_url[year])
        return None

    gdf = crs_harmonize(gdf)  # convert to WGS84

    # check for empty/na geom values
    ena = sum(gdf['geometry'].is_empty | gdf['geometry'].isna())
    if ena > 0:
        log.info(f"{year} Census SHP has {ena} empty + NA geometries")
    return gdf

def parse_pt_data(df):
    """
    Convert df containing "Latitude" and "Longitude" columns to
    gpd.GeoDataFrame with geometry column of Points
    :param df: pandas dataframe
    """
    # check for na lat/long values
    pt_na = sum(df['Latitude'].isna() | df['Longitude'].isna())
    if pt_na != 0:
        log.info(f"Point data contains {pt_na} nan Lat and/or Long values")
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
        log.error('GeoDataFrame invalid; contains non-polygon geometries')
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

def main(df, year, *cmpts):
    """
    Handler function to flexibly assign release height ('rh') and/or urban/rural
    (via 'urb', which initiates geospatial dependencies check) secondary compartments.
    :param df: pd.DataFrame
    :param year: int, data year
    :param cmpts: str, flag(s) for compartment assignment
    """
    if 'urb' not in cmpts and 'rh' not in cmpts:
        log.error('Please pass one or more valid *cmpts string codes: {urb, rh}')
        return df
    if 'urb' in cmpts and has_geo_pkgs:
        df = urb_intersect(df, year)
    if 'rh' in cmpts:
        df = classify_height(df)
    return df


if __name__ == "__main__":
    import stewi
    # import time
    year = 2017
    df_raw = stewi.getInventoryFacilities('NEI', year)  # ('TRI', 2019)
    # time_start = time.time()
    # df_urb = urb_intersect(pt_raw, year)
    df_cmpts = main(df_raw, year, 'urb', 'rh')
    # print(f'[total] --- {time.time() - time_start} seconds ---')
