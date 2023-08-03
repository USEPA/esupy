# processed_data_mgmt.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to manage querying, retrieving and storing of preprocessed data in
local directories
"""
import json
import logging as log
import os
from pathlib import Path

import appdirs
import boto3
import pandas as pd
from botocore.handlers import disable_signing

from esupy.remote import make_url_request
from esupy.util import strip_file_extension


class Paths:
    def __init__(self):
        self.local_path = Path(appdirs.user_data_dir())
        self.remote_path = 'https://dmap-data-commons-ord.s3.amazonaws.com/'
    # TODO: rename as DataPaths {.local, .remote}

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
    if isinstance(f, Path):
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
    :param kwargs: option to include 'subdir_dict', a dictionary that
         directs local data storage location based on extension
    :return: bool False if download fails, True if successful
    """
    status = False
    base_url = paths.remote_path + file_meta.tool + '/'
    if file_meta.category != '':
        base_url = base_url + file_meta.category + '/'
    ## TODO: re-implement URL handling via f-strings and/or urllib
    # base_url = f'{paths.remote_path}/{file_meta.tool}'
    # if not file_meta.category == '':
    #     base_url = f'{base_url}/{file_meta.category}'
    files = get_most_recent_from_index(file_meta, paths)
    if files is None:
        log.info(f'{file_meta.name_data} not found in {base_url}')
    else:
        for fname in files:
            url = base_url + fname
            r = make_url_request(url)
            if r is not None:
                status = True
                # set subdirectory
                subdir = file_meta.category
                # if there is a dictionary with specific subdirectories
                # based on end of filename, modify the subdirectory
                if kwargs != {}:
                    if 'subdir_dict' in kwargs:
                        for k, v in kwargs['subdir_dict'].items():
                            if fname.endswith(k):
                                subdir = v
                folder = paths.local_path / subdir
                mkdir_if_missing(folder)
                file = folder / fname
                with file.open('wb') as fi:
                    fi.write(r.content)
                log.info(f'{fname} downloaded from '
                         f'{paths.remote_path}index.html?prefix='
                         f'{file_meta.tool}/{file_meta.category} and saved to '
                         f'{folder}')
    return status


def remove_extra_files(file_meta, paths):
    """
    Removes all but the most recent file within paths.local_path based on
    file metadata. Does not discern by version number
    :param file_meta: populated instance of class FileMeta
    :param paths: populated instance of class Paths
    """
    path = paths.local_path / file_meta.category
    fs = {}
    file_name = file_meta.name_data + '_v'
    for f in os.scandir(str(path)):
        name = f.name
        # get file creation time (Windows) or most recent metadata change time (Unix)
        # TODO: refactor w/ recursive glob: path.rglob('*')
        st = f.stat().st_ctime
        if name.startswith(file_name):
            fs[name] = st
    keep = max(fs, key=fs.get)
    log.debug(f'found {len(fs)} files')
    count = 0
    for f in fs.keys():
        if f is not keep:
            os.remove(path + '/' + f)
            count += 1
    log.debug(f'removed {count} files')


def find_file(meta, paths):
    """
    Searches for file within path.local_path based on file metadata; if
    metadata matches, returns most recently created file path object
    :param meta: populated instance of class FileMeta
    :param paths: populated instance of class Paths
    :return: str with the file path if found, otherwise an empty string
    """
    path = paths.local_path / meta.category
    if path.exists():
        # TODO: refactor w/ recursive glob: path.rglob('*')
        with os.scandir(str(path)) as files:
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
            return path / sorted_matches[0].name
    return None


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
        vh = '_'.join(strip_file_extension(recent_file).replace(
            f'{file_meta.name_data}_', '').split('_', 2)[:2])
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
    folder = paths.local_path / meta.category
    fname = f'{meta.name_data}_v{meta.tool_version}'
    if meta.git_hash is not None:
        fname = f'{fname}_{meta.git_hash}'
    try:
        mkdir_if_missing(folder)
        if meta.ext == "parquet":
            df.to_parquet(folder / f'{fname}.parquet')
        elif meta.ext == "csv":
            df.to_csv(folder / f'{fname}.csv', index=False)
        else:
            log.error(f'Failed to save {fname}; metadata lacks "ext" property')
    except Exception as e:
        log.exception(f'Failed to save {fname}')
        raise e


def read_into_df(fpath):
    """
    Based on a file extension use the appropriate function to read in file
    :param fpath: pathlib.Path, file path object
    :return: a pandas dataframe with the file data if extension is handled,
        else an error
    """
    ext = fpath.suffix.lower()
    if ext == '.parquet':
        df = pd.read_parquet(fpath)
    elif ext == '.csv':
        df = pd.read_csv(fpath)
    elif ext == '.rds':
        try:
            import rpy2.robjects as robjects
            from rpy2.robjects import pandas2ri
        except ImportError:
            log.error('Must install rpy2 to read .rds files')
        pandas2ri.activate()
        readRDS = robjects.r['readRDS']
        df = readRDS(str(fpath))
        # ^ readRDS can not handle Path objects
    else:
        log.error(f'No reader specified for extension {ext}')
    return df

# def define_metafile(datafile,paths):
#    data = strip_file_extension(datafile)
#    metafile = paths.local_path / f'{data}_metadata.json'
#    return metafile


def write_metadata_to_file(paths, meta):
    """
    Writes the metadata of class FileMeta to JSON
    :param paths: populated instance of class Paths
    :param meta: populated instance of class FileMeta
    """
    folder = paths.local_path / meta.category
    fname = f'{meta.name_data}_v{meta.tool_version}'
    if meta.git_hash is not None:
        fname = f'{fname}_{meta.git_hash}'
    fname = f'{fname}_metadata.json'
    file = folder / fname
    with file.open('w') as fi:
        fi.write(json.dumps(meta.__dict__, indent=4))


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
        p = find_file(meta, paths)
        path = p.parent / f'{p.stem}_metadata.json' if p else None
    try:
        metadata = json.loads(path.read_text())
        return metadata
    except (FileNotFoundError, AttributeError):
        log.warning(f'metadata not found for {meta.name_data}')
        return None


def mkdir_if_missing(folder):
    """
    Via pathlib, create a new dir (and parents) if not in existence
    https://docs.python.org/3/library/pathlib.html#pathlib.Path.mkdir
    :param folder: pathlib.Path, a path to a directory (not a file)
    """
    if not folder.exists() and not folder.is_file():
        folder.mkdir(parents=True, exist_ok=True)


def get_data_commons_index(file_meta, paths):
    """
    Returns a dataframe of files available on data commmons for the
    particular category
    :param file_meta: instance of class FileMeta
    :param paths: instance of class Path
    :param category: str of the category to search e.g. 'flowsa/FlowByActivity'
    :return: dataframe with 'date' and 'file_name' as fields
    """
    subdir = file_meta.tool + '/'
    if file_meta.category != '':
        subdir = subdir + file_meta.category + '/'

    s3 = boto3.Session().resource('s3')
    s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

    bucket = s3.Bucket('dmap-data-commons-ord')
    d = {}
    for item in bucket.objects.filter(Prefix=subdir):
        d[item.key] = item.last_modified
    df = pd.DataFrame.from_dict(d, orient='index').reset_index()

    df.columns = ['file_name', 'last_modified']
    # Reformat the date to a pd datetime
    df['date'] = pd.to_datetime(df['last_modified'],
                                format='%Y-%m-%dT%H:%M:%S')
    # Remove the category name and trailing slash from the file name
    df['file_name'] = df['file_name'].str.replace(subdir, "")
    # Reset the index and return
    df = df[['date', 'file_name']].reset_index(drop=True)
    return df


def parse_data_commons_index(df):
    """Parse a df from data_commons_index into separate columns"""
    df['ext'] = df['file_name'].str.rsplit('.', n=1, expand=True)[1]
    df['file'] = df['file_name'].str.rsplit('.', n=1, expand=True)[0]
    df['git_hash'] = df['file'].str.rsplit('_', n=1, expand=True)[1]
    df['git_hash'].fillna('', inplace=True)
    df.loc[df['git_hash'].map(len) != 7, 'git_hash'] = ''
    try:
        df['version'] = (df['file']
                         .str.split('_v', n=1, expand=True)[1]
                         .str.split('_', expand=True)[0])
        df['name'] = df['file'].str.split('_v', n=1, expand=True)[0]
    except KeyError:
        df['version'] = ''
        df['name'] = df['file']
    df = df[['name', 'version', 'git_hash',
             'ext', 'date', 'file_name']].reset_index(drop=True)
    df.fillna('', inplace=True)
    return df
