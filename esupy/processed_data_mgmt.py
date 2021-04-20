# processed_data_mgmt.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to manage querying, retrieving and storing of preprocessed data in local directories
"""
import logging as log
import os
import pandas as pd
import re
import json
#from esupy.remote import make_http_request
from esupy.util import supported_ext
import appdirs


class Paths:
    def __init__(self):
        self.local_path = appdirs.user_data_dir()


class FileMeta:
    def __init__(self):
        self.tool = ""
        self.category = ""
        self.name_data = ""
        self.tool_version = ""
        self.git_hash = ""
        self.ext = ""
        self.tool_meta = ""



def load_preprocessed_output(file_meta, paths):
    """
    Loads a preprocessed file
    :param file_meta: populated instance of class FileMeta
    :param paths: instance of class Paths
    :return: a pandas dataframe of the datafile if exists or None if it doesn't exist
    """
    f = find_file(file_meta,paths)
    if os.path.exists(f):
        df = read_into_df(f)
        return df
    else:
        return None


def find_file(meta,paths):
    """
    Searches for file within path.local_path based on file metadata, if metadata matches,
     returns most recently created file name
    :param meta: populated instance of class FileMeta
    :param paths: populated instance of class Paths
    :param force_version: boolean on whether or not to include version number in search
    :return: str with the file path if found, otherwise an empty string
    """
    path = os.path.realpath(paths.local_path + "/" + meta.category)
    if os.path.exists(path):
        search_words = meta.name_data
        fs = {}
        for f in os.scandir(path):
            name = f.name
            # get file creation time
            st = f.stat().st_ctime
            fs[name]=st
            matches = []
            for k in fs.keys():
                if re.search(search_words, k):
                    if re.search(meta.ext, k, re.IGNORECASE):
                        matches.append(k)                    
        if len(matches) == 0:
            f = ""
        else:
            # Filter the dict by matches
            r = {k:v for k,v in fs.items() if k in matches}
            # Sort the dict by matches, return a list
            #r = {k:v for k,v in sorted(r.items(), key=lambda item: item[1], reverse=True)}
            rl = [k for k,v in sorted(r.items(), key=lambda item: item[1], reverse=True)]
            f = os.path.realpath(path + "/" + rl[0])
    else:
        f = ""
    return f


def write_df_to_file(df,paths,meta):
    """
    Writes a data frame to the designated local folder and file name created using paths and meta
    :param df: a pandas dataframe
    :param paths: populated instance of class Paths
    :param meta: populated instance of class FileMeta
    :return: None
    """
    folder = os.path.realpath(paths.local_path + "/" + meta.category)
    file = folder + "/" + meta.name_data + "_v" + meta.tool_version
    if meta.git_hash is not None:
        file = file + "_" + meta.git_hash
    try:
        create_paths_if_missing(file)
        if meta.ext=="parquet":
            file = file + ".parquet"
            file = os.path.realpath(file)
            df.to_parquet(file, engine="pyarrow")
        elif meta.ext == "csv":
            file = file + ".csv"
            file = os.path.realpath(file)
            df.to_csv(file, index=False)
        else:
            log.error('Failed to save ' + file + '. ' + "Meta data lacks 'ext' property")
    except:
        log.error('Failed to save '+ file + '.')

def read_into_df(file):
    """
    Based on a file extension use the appropriate function to read in file
    :param file: str with a file path
    :return: a pandas dataframe with the file data if extension is handled, else an error
    """
    name,ext = os.path.splitext(file)
    ext = ext.lower()
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

def write_metadata_to_file(paths, meta):
    """
    Writes the metadata of class FileMeta to JSON
    :param paths: populated instance of class Paths
    :param meta: populated instance of class FileMeta
    """
    folder = os.path.realpath(paths.local_path + "/" + meta.category)
    file = folder + "/" + meta.name_data + '_metadata.json'
    with open(file, 'w') as file:
        file.write(json.dumps(meta.__dict__, indent = 4))

def create_paths_if_missing(file):
    """
    Creates paths is missing. Paths are created recursivley by os.makedirs
    :param file:
    :return:
    """
    dir = os.path.dirname(file)
    if not os.path.exists(dir):
        os.makedirs(dir)

