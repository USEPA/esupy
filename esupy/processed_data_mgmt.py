# processed_data_mgmt.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to manage querying, retrieving and storing of preprocessed data in
local directories
"""
import logging as log
import os
import pandas as pd
import json
import appdirs
import boto3
from botocore.handlers import disable_signing
from esupy.remote import make_url_request
from esupy.util import strip_file_extension


class Paths:
    def __init__(self):
        self.local_path = appdirs.user_data_dir()
        self.remote_path = 'https://dmap-data-commons-ord.s3.amazonaws.com/'


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
    :return: a pandas dataframe of the datafile if exists or None if it
        doesn't exist
    """
    f = find_file(file_meta, paths)
    if os.path.exists(f):
        log.info(f'Returning {f}')
        df = read_into_df(f)
        return df
    else:
        return None


def download_from_remote(file_meta, paths, **kwargs):
    """
    Downloads one or more files from remote and stores locally based on the
    most recent instance of that file. Most recent is determined by max
    version number. All files that share name_data, version, and hash will
    be downloaded together.
    :param file_meta: populated instance of class FileMeta
    :param paths: instance of class Paths
    :param kwargs: option to include 'subdirectory_dict', a dictionary that
         directs local data storage location based on extension
    :return: bool False if download fails, True if successful
    """
    status = False
    base_url = paths.remote_path + file_meta.tool + '/'
    if file_meta.category != '':
        base_url = base_url + file_meta.category + '/'
    files = get_most_recent_from_index(file_meta, paths)
    if files is None:
        log.info(f'{file_meta.name_data} not found in {base_url}')
    else:
        for f in files:
            url = base_url + f
            r = make_url_request(url)
            if r is not None:
                status = True
                # set subdirectory
                subdirectory = file_meta.category
                # if there is a dictionary with specific subdirectories
                # based on end of filename, modify the subdirectory
                if kwargs != {}:
                    if 'subdirectory_dict' in kwargs:
                        for k, v in kwargs['subdirectory_dict'].items():
                            if f.endswith(k):
                                subdirectory = v
                folder = os.path.realpath(paths.local_path
                                          + '/' + subdirectory)
                file = folder + "/" + f
                create_paths_if_missing(file)
                log.info(f'{f} downloaded from'
                         f' {paths.remote_path}index.html?prefix='
                         f'{file_meta.tool}/{file_meta.category} and saved to '
                         f'{folder}')
                with open(file, 'wb') as f:
                    f.write(r.content)
    return status


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
            fs[name] = st
    keep = max(fs, key=fs.get)
    log.debug("found %i files", len(fs))
    count = 0
    for f in fs.keys():
        if f is not keep:
            os.remove(path + "/" + f)
            count += 1
    log.debug("removed %i files", count)


def find_file(meta, paths):
    """
    Searches for file within path.local_path based on file metadata, if
    metadata matches, returns most recently created file name
    :param meta: populated instance of class FileMeta
    :param paths: populated instance of class Paths
    :return: str with the file path if found, otherwise an empty string
    """
    path = os.path.realpath(f'{paths.local_path}/{meta.category}')
    if os.path.exists(path):
        with os.scandir(path) as files:
            # List all file satisfying the criteria in the passed metadata
            matches = [f for f in files
                       if f.name.startswith(meta.name_data)
                       and meta.ext.lower() in f.name.lower()]
            # Sort files in reverse order by ctime (creation time on Windows,
            # last metadata modification time on Unix)
            sorted_matches = sorted(matches,
                                    key=lambda f: f.stat().st_ctime,
                                    reverse=True)
        # Return the path to the most recent matching file, or '' if no
        # match exists.
        if sorted_matches:
            return os.path.realpath(f'{path}/{sorted_matches[0].name}')
    return ''


def get_most_recent_from_index(file_meta, paths):
    """
    Sorts the data commons index by most recent date for the required extension
    and returns the matching files of that name that share the same version
    and hash
    :param file_meta:
    :param paths:
    :return: list, most recently created datafiles, metadata, log files
    """

    file_df = get_data_commons_index(file_meta, paths)
    if file_df is None:
        return None
    file_df = parse_data_commons_index(file_df)
    df = file_df[file_df['name'].str.startswith(file_meta.name_data)]
    df_ext = df[df['ext'] == file_meta.ext]
    if len(df_ext) == 0:
        return None
    else:
        df_ext = (df_ext.sort_values(by=["version", "date"], ascending=False)
                  .reset_index(drop=True))
        # select first file name in list, extract the file version and git
        # hash, return list of files that include version/hash (to include
        # metadata and log files)
        recent_file = df_ext['file_name'][0]
        vh = "_".join(strip_file_extension(recent_file).replace(
            f'{file_meta.name_data}_', '').split("_", 2)[:2])
        if vh != '':
            df_sub = [string for string in df['file_name'] if vh in string]
        else:
            df_sub = [recent_file]
        return df_sub


def write_df_to_file(df, paths, meta):
    """
    Writes a data frame to the designated local folder and file name created
    using paths and meta
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
        if meta.ext == "parquet":
            file = file + ".parquet"
            file = os.path.realpath(file)
            df.to_parquet(file)
        elif meta.ext == "csv":
            file = file + ".csv"
            file = os.path.realpath(file)
            df.to_csv(file, index=False)
        else:
            log.error('Failed to save ' + file + '. '
                      + "Meta data lacks 'ext' property")
    except Exception as e:
        log.exception('Failed to save ' + file + '.')
        raise e


def read_into_df(file):
    """
    Based on a file extension use the appropriate function to read in file
    :param file: str with a file path
    :return: a pandas dataframe with the file data if extension is handled,
        else an error
    """
    df = pd.DataFrame()
    name, ext = os.path.splitext(file)
    ext = ext.lower()
    if ext == ".parquet":
        df = pd.read_parquet(file)
    elif ext == ".csv":
        df = pd.read_csv(file)
    elif ext == ".rds":
        try:
            import rpy2.robjects as robjects
            from rpy2.robjects import pandas2ri
        except ImportError:
            log.error("Must install rpy2 to read .rds files")
        pandas2ri.activate()
        readRDS = robjects.r['readRDS']
        df = readRDS(file)
    else:
        log.error(f"No reader specified for extension {ext}")
    return df

# def define_metafile(datafile,paths):
#    data = strip_file_extension(datafile)
#    metafile = os.path.realpath(paths.local_path + "/" + data
#                                + '_metadata.json')
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
        file.write(json.dumps(meta.__dict__, indent=4))


def read_source_metadata(paths, meta, force_JSON=False):
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


def get_data_commons_index(file_meta, paths):
    """
    Returns a dataframe of files available on data commmons for the
    particular category
    :param file_meta: instance of class FileMeta
    :param paths: instance of class Path
    :param category: str of the category to search e.g. 'flowsa/FlowByActivity'
    :return: dataframe with 'date' and 'file_name' as fields
    """
    subdirectory = file_meta.tool + '/'
    if file_meta.category != '':
        subdirectory = subdirectory + file_meta.category + '/'

    s3 = boto3.Session().resource('s3')
    s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

    bucket = s3.Bucket('dmap-data-commons-ord')
    d = {}
    for item in bucket.objects.filter(Prefix=subdirectory):
        d[item.key] = item.last_modified
    df = pd.DataFrame.from_dict(d, orient='index').reset_index()

    df.columns = ['file_name', 'last_modified']
    # Reformat the date to a pd datetime
    df['date'] = pd.to_datetime(df['last_modified'],
                                format='%Y-%m-%dT%H:%M:%S')
    # Remove the category name and trailing slash from the file name
    df['file_name'] = df['file_name'].str.replace(subdirectory, "")
    # Reset the index and return
    df = df[['date', 'file_name']].reset_index(drop=True)
    return df


def parse_data_commons_index(df):
    """Parse a df from data_commons_index into separate columns"""
    df['ext'] = df['file_name'].str.rsplit(".", n=1, expand=True)[1]
    df['file'] = df['file_name'].str.rsplit(".", n=1, expand=True)[0]
    df['git_hash'] = df['file'].str.rsplit("_", n=1, expand=True)[1]
    df['git_hash'].fillna('', inplace=True)
    df.loc[df['git_hash'].map(len) != 7, 'git_hash'] = ''
    try:
        df['version'] = (df['file']
                         .str.split("_v", n=1, expand=True)[1]
                         .str.split("_", expand=True)[0])
        df['name'] = df['file'].str.split("_v", n=1, expand=True)[0]
    except KeyError:
        df['version'] = ''
        df['name'] = df['file']
    df = df[['name', 'version', 'git_hash',
             'ext', 'date', 'file_name']].reset_index(drop=True)
    df.fillna('', inplace=True)
    return df
