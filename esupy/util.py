# util.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Simple utility functions for reuse in tools
"""
def strip_file_extension(filename):
    """
    Simple function to strip file extension identified by last . in file name
    :param filename: a string representing a file name; can contain path
    :return: string, filename without extension
    """
    return(filename.rsplit(".", 1)[0])


