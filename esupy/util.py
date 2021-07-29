# util.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Simple utility functions for reuse in tools
"""
import os
import subprocess

supported_ext = ["parquet","csv"]


def strip_file_extension(filename):
    """
    Simple function to strip file extension identified by last . in file name
    :param filename: a string representing a file name; can contain path
    :return: string, filename without extension
    """
    return(filename.rsplit(".", 1)[0])


def is_git_directory(path = '.'):
    """
    Returns True if path contains git directory
    """
    return subprocess.call(['git', '-C', path, 'status'],
                           stderr=subprocess.STDOUT,
                           stdout = open(os.devnull, 'w')) == 0

def get_git_hash():
    """
    Returns 7 digit git_hash of current directory or None if no git found
    """
    git_hash = None
    if is_git_directory():
        try:
            git_hash = subprocess.check_output(
                ['git', 'rev-parse', 'HEAD']).strip().decode(
                'ascii')[0:7]
        except: pass
    return git_hash
