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
import appdirs
import xml.etree.ElementTree as ET
from esupy.remote import make_http_request
from esupy.util import strip_file_extension

class Paths:
    def __init__(self):
        self.local_path = appdirs.user_data_dir()
        self.remote_path = 'https://edap-ord-data-commons.s3.amazonaws.com/'


class FileMeta:
    def __init__(self):
        self.tool = ""
        self.category = ""
        self.name_data = ""
        self.tool_version = ""
        self.git_hash = ""
        self.ext = ""
        self.date_created = ""
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


def download_from_remote(file_meta, paths, **kwargs):
    """
    Downloads one or more files from remote and stores locally based on the
    most recent instance of that file. All files that share name_data, version, and
    hash will be downloaded together.
    :param file_meta: populated instance of class FileMeta
    :param paths: instance of class Paths
    :param kwargs: option to include 'subdirectory_dict', a dictionary that
         directs local data storage location based on extension
    """
    category = file_meta.tool + '/'
    if file_meta.category != '':
        category = category + file_meta.category + '/'
    base_url = paths.remote_path + category
    files = get_most_recent_from_index(file_meta.name_data, category, paths)
    if files == []:
        log.info('%s not found in %s', file_meta.name_data, base_url)
    else:
        for f in files:
            url = base_url + f
            r = make_http_request(url)
            if r is not None:
                # set subdirectory
                subdirectory = file_meta.category
                # if there is a dictionary with specific subdirectories
                # based on end of filename, modify the subdirectory
                if kwargs != {}:
                    if 'subdirectory_dict' in kwargs:
                        for k, v in kwargs['subdirectory_dict'].items():
                            if f.endswith(k):
                                subdirectory = v
                folder = os.path.realpath(paths.local_path + '/' + subdirectory)
                file = folder + "/" + f
                create_paths_if_missing(file)
                log.info('%s saved to %s', f, folder)
                with open(file, 'wb') as f:
                    f.write(r.content)


def remove_extra_files(file_meta, paths):
    """
    Removes all but the most recent file within paths.local_path based on
    file metadata. Does not discern by version number
    :param file_meta: populated instance of class FileMeta
    :param paths: populated instance of class Paths
    """
    path = os.path.realpath(paths.local_path + "/" + file_meta.category)
    if not(os.path.exists(path)):
        return
    fs = {}
    file_name = file_meta.name_data + "_v"
    for f in os.scandir(path):
        name = f.name
        # get file creation time
        st = f.stat().st_ctime
        if name.startswith(file_name):
            fs[name]=st
    keep = max(fs, key=fs.get)
    log.debug("found %i files", len(fs))
    count = 0
    for f in fs.keys():
        if f is not keep:
            os.remove(path + "/" + f)
            count +=1
    log.debug("removed %i files", count)


def find_file(meta,paths):
    """
    Searches for file within path.local_path based on file metadata, if metadata matches,
     returns most recently created file name
    :param meta: populated instance of class FileMeta
    :param paths: populated instance of class Paths
    :return: str with the file path if found, otherwise an empty string
    """
    path = os.path.realpath(paths.local_path + "/" + meta.category)
    if os.path.exists(path):
        search_words = meta.name_data
        matches = []
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


def get_most_recent_from_index(file_name, category, paths):
    """
    Sorts the data commons index by most recent date and returns
    the matching files of that name that share the same version and hash
    :param file_name:
    :param category:
    :param paths:
    :return: list, most recently created datafiles, metadata, log files
    """

    file_df = get_data_commons_index(paths, category)
    if file_df is None:
        return None
    file_df = parse_data_commons_index(file_df)
    df = file_df[file_df['name']==file_name]
    if len(df) == 0:
        return None
    else:
        df = df.sort_values(by='date', ascending=False).reset_index(drop=True)
        # select first file name in list, extract the file version and git hash,
        # return list of files that include version/hash (to include metadata and log files)
        recent_file = df['file_name'][0]
        vh = "_".join(strip_file_extension(recent_file).replace(f'{file_name}_', '').split("_", 2)[:2])
        if vh != '':
            df_sub = [string for string in df['file_name'] if vh in string]
        else:
            df_sub = [recent_file]
        return df_sub


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
    df = pd.DataFrame()
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
    file = folder + "/" + meta.name_data + "_v" + meta.tool_version
    if meta.git_hash is not None:
        file = file + "_" + meta.git_hash
    file = file + '_metadata.json'
    with open(file, 'w') as file:
        file.write(json.dumps(meta.__dict__, indent = 4))


def read_source_metadata(paths, meta, force_JSON = False):
    """return the locally saved metadata dictionary from JSON,
    meta should reflect the outputfile for which the metadata is associated
    
    :param meta: object of class FileMeta used to load the outputfile
    :param paths: object of class Paths
    :param force_JSON: bool, searches based on named JSON instead of outputfile
    :return: metadata dictionary
    """
    if force_JSON:
        meta.ext = 'json'
        path = find_file(meta, paths)
    else:
        path = find_file(meta, paths)
        # remove the extension from the file and add _metadata.json
        path = strip_file_extension(path)
        path = f'{path}_metadata.json'
    try:
        with open(path, 'r') as file:
            file_contents = file.read()
            metadata = json.loads(file_contents)
            return metadata
    except FileNotFoundError:
        log.warning("metadata not found for %s", meta.name_data)
        return None
    

def create_paths_if_missing(file):
    """
    Creates paths is missing. Paths are created recursivley by os.makedirs
    :param file:
    :return:
    """
    dir = os.path.dirname(file)
    if not os.path.exists(dir):
        os.makedirs(dir)


def get_data_commons_index(paths, category):
    """Returns a dataframe of files available on data commmons for the
    particular category
    :param paths: instance of class Path
    :param category: str of the category to search e.g. 'flowsa/FlowByActivity'
    :return: dataframe with 'date' and 'file_name' as fields
    """
    index_url = '?prefix='
    url = paths.remote_path + index_url + category
    listing = make_http_request(url)
    # Code to convert XML to pd df courtesy of
    # https://stackabuse.com/reading-and-writing-xml-files-in-python-with-panda
    contents = ET.XML(listing.text)
    data = []
    cols = []
    for i, child in enumerate(contents):
        data.append([subchild.text for subchild in child])
        cols.append(child.tag)
    df = pd.DataFrame(data)
    df.dropna(inplace = True)
    try:
        # only get first two columns and rename them name and last modified
        df = df[[0, 1]]
    except KeyError:
        # no data found at url
        return None
    df.columns = ['file_name', 'last_modified']
    # Reformat the date to a pd datetime
    df['date'] = pd.to_datetime(df['last_modified'],
                                format = '%Y-%m-%dT%H:%M:%S')
    # Remove the category name and trailing slash from the file name
    df['file_name'] = df['file_name'].str.replace(category,"")
    # Reset the index and return
    df = df[['date','file_name']].reset_index(drop=True)
    return df


def parse_data_commons_index(df):
    """Parse a df from data_commons_index into separate columns"""
    df['ext'] = df['file_name'].str.rsplit(".", n=1, expand=True)[1]
    df['file'] = df['file_name'].str.rsplit(".", n=1, expand=True)[0]
    df['git_hash'] = df['file'].str.rsplit("_", n=1, expand=True)[1]
    df['git_hash'].fillna('', inplace=True)
    df.loc[df['git_hash'].map(len)!=7, 'git_hash'] = ''
    try:
        df['version'] = df['file'].str.split("_v", n=1, expand=True)[1].str.split(
            "_", expand=True)[0]
        df['name'] = df['file'].str.split("_v", n=1, expand=True)[0]
    except KeyError:
        df['version'] = ''
        df['name'] = df['file']
    df = df[['name','version','git_hash',
             'ext','date', 'file_name']].reset_index(drop=True)
    df.fillna('', inplace=True)
    return df

