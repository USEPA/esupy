# processed_data_mgmt.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to manage querying, retrieving and storing preprocessed data in both local directories
and on a remote data server. The functions also write a JSON file in the form
of a dictionary with metadata corresponding to the processed data, including the LastUpdated time
on the remote server.
Currently only valid for the EPA Data Commons remote server, and assumes parquet format
"""
import datetime as dt
import json
import logging as log
import os
import pandas as pd
from esupy.remote import make_http_request
from esupy.util import strip_file_extension
import appdirs


class Paths:
    local_path = appdirs.user_data_dir()
    remote_path = ""

def load_preprocessed_output(datafile, paths):
    """
    Loads a preprocessed file
    :param datafile: a data file name with any preceeding relative file
    :param paths: instance of class Paths
    :return: a pandas dataframe of the datafile
    """
    local_file = os.path.realpath(paths.local_path + "/" + datafile)
    remote_file = paths.remote_path + datafile
    if os.path.exists(local_file) & local_copy_is_current(datafile, paths):
            log.debug('Loading ' + datafile + ' from local repository')
            df = pd.read_parquet(local_file)
    else:
        try:
            log.info(datafile + ' not found in local folder; loading from remote server...')
            df = pd.read_parquet(remote_file)
            #Now write it to local
            create_paths_if_missing(local_file)
            df.to_parquet(local_file)
            write_datafile_meta(datafile, paths)
            log.info(datafile + ' saved in ' + paths.local_path)
        except FileNotFoundError:
            log.error("No file found for " + datafile)
    return df

def local_copy_is_current(datafile, path):
    """
    :param datafile:
    :param path:
    :return:
    """
    remote_time = get_file_update_time_from_DataCommons(datafile)
    meta_file = define_metafile(datafile, path)

    if os.path.exists(meta_file):
        local_time = get_file_update_time_from_local(datafile, path)
        if local_time >= remote_time:
            return True
        else:
            return False
    else:
        return False


def get_file_update_time_from_DataCommons(datafile):
    """
    Gets a datetime object for the file on the DataCommons server
    :param datafile: the file name to be searched on the remote
    :return: a datetime object
    """
    base_url = "https://xri9ebky5b.execute-api.us-east-1.amazonaws.com/api/?"
    search_param = "searchvalue"
    url = base_url + search_param + "=" + datafile + "&place=&searchfields=filename"
    r = make_http_request(url)
    date_str = r.json()[0]["LastModified"]
    file_upload_dt = dt.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S%z')
    return file_upload_dt

def get_file_update_time_from_local(datafile, paths):
    """
    Gets a datetime object for the metadata file in the local directory
    :param datafile: the file name to be searched for on local
    :return: a datetime object
    """
    meta = read_datafile_meta(datafile, paths)
    file_upload_dt = dt.datetime.strptime(meta["LastUpdated"], '%Y-%m-%d %H:%M:%S%z')
    return file_upload_dt

def write_datafile_meta(datafile, paths):
    """
    Writes a JSON file with metadata for processed data
    :param datafile: the file to be created on local
    :return: None
    """
    file_upload_dt = get_file_update_time_from_DataCommons(datafile)
    d = {}
    d["LastUpdated"] = format(file_upload_dt)
    metafile = define_metafile(datafile,paths)
    with open(metafile, 'w') as file:
        file.write(json.dumps(d))

def read_datafile_meta(datafile, paths):
    """
    Reads local JSON metadata file
    :param datafile: the file to be read on local
    :return: JSON metadata file
    """
    metafile = define_metafile(datafile, paths)
    try:
        with open(metafile, 'r') as file:
            file_contents = file.read()
            metadata = json.loads(file_contents)
        return metadata
    except FileNotFoundError:
        log.error("Local metadata file for " + datafile + " is missing.")


def define_metafile(datafile,paths):
    data = strip_file_extension(datafile)
    metafile = os.path.realpath(paths.local_path + "/" + data + '_metadata.json')
    return metafile

def create_paths_if_missing(file):
    """
    Creates paths is missing. Paths are created recursivley by os.makedirs
    :param file:
    :return:
    """
    dir = os.path.dirname(file)
    if not os.path.exists(dir):
        os.makedirs(dir)

