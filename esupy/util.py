# util.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Simple utility functions for reuse in tools
"""
import inspect
import os
import subprocess
import uuid

supported_ext = ["parquet", "csv"]


def strip_file_extension(filename):
    """
    Simple function to strip file extension identified by last . in file name
    :param filename: a string representing a file name; can contain path
    :return: string, filename without extension
    """
    return filename.rsplit(".", 1)[0]


def is_git_directory(path='.'):
    """
    Returns True if path contains git directory
    """
    return subprocess.call(['git', '-C', path, 'status'],
                           stderr=subprocess.STDOUT,
                           stdout=open(os.devnull, 'w')) == 0


def get_git_hash(length='short'):
    """
    Returns git_hash of current directory or None if no git found
    :param length: str, 'short' for 7-digit, 'long' for full git hash
    :return git_hash: str
    """
    git_hash = None
    if is_git_directory():
        try:
            # Define the directory path where this function is called.
            # Necessary when running scripts located outside of a repository,
            # but want the git hash of the repository
            dirpath = os.path.dirname(os.path.abspath((inspect.stack()[1])[1]))

            git_hash = subprocess.check_output(
                ['git', 'rev-parse', 'HEAD'], cwd=dirpath).strip().decode(
                'ascii')
            if length == 'short':
                git_hash = git_hash[0:7]
        except:
            pass
    return git_hash


def as_path(*args: str) -> str:
    """Converts strings to lowercase path-like string
    Take variable order of string inputs
    :param args: variable-length of strings
    :return: string
    """
    return "/".join([x.strip().lower() for x in map(str, args)])


def make_uuid(*args: str) -> str:
    """
    Generic wrapper of uuid.uuid3 method for uuid generation
    :param args: variable list of strings
    :return: string uuid
    """
    path = as_path(*args)
    return str(uuid.uuid3(uuid.NAMESPACE_OID, path))
