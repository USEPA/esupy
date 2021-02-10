# processed_data_mgmt.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to manage querying, retrieving and storing preprocessed data in local directories
Currently assumes parquet format
"""
import logging as log
import os
import pandas as pd
import re
#from esupy.remote import make_http_request
#from esupy.util import strip_file_extension
import appdirs

class Paths:
    local_path = appdirs.user_data_dir()

class FileMeta:
    tool = ""
    category = ""
    name_data = ""
    tool_version = ""
    git_hash = ""

def load_preprocessed_output(file_meta, paths):
    """
    Loads a preprocessed file
    :param datafile: a data file name with any preceeding relative file
    :param paths: instance of class Paths
    :return: a pandas dataframe of the datafile
    """
    f = find_file(file_meta,paths)
    df = read_into_df(f)
    return df


def find_file(meta,paths,force_version=False):
    path = os.path.realpath(paths.local_path + "/" + meta.category)
    if os.path.exists(path):
        fs = os.listdir(path)
        search_words = meta.name_data
        #Get a list of all matching files
        r = list(filter(lambda x: re.search(search_words, x), fs))
        if len(r)==0:
            return FileNotFoundError
        elif len(r)>1:
            f = os.path.realpath(path + "/" + r[0])
            return f
        else:
            f = os.path.realpath(path + "/" + r[0])
            return f

def read_into_df(file):
    """
    Based on a file extension use the appropriate function to read in file
    :param file:
    :return:
    """
    name,ext = os.path.splitext(file)
    if ext==".parquet":
        df = pd.read_parquet(file)
    elif ext==".csv":
        df = pd.read_csv(file)
    else:
        log.error("No reader specified for extension"+ext)
    return df

#def define_metafile(datafile,paths):
#    data = strip_file_extension(datafile)
#    metafile = os.path.realpath(paths.local_path + "/" + data + '_metadata.json')
#    return metafile

def create_paths_if_missing(file):
    """
    Creates paths is missing. Paths are created recursivley by os.makedirs
    :param file:
    :return:
    """
    dir = os.path.dirname(file)
    if not os.path.exists(dir):
        os.makedirs(dir)

