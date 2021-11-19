# remote.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions for handling remote requests and parsing
"""
import logging as log
import requests
import requests_ftp
from urllib.parse import urlsplit


def make_http_request(url):
    """
    Makes http request using requests library
    :param url: URL to query
    :return: request Object
    """
    session = (requests_ftp.ftp.FTPSession if urlsplit(url).scheme == 'ftp'
               else requests.Session)
    with session() as s:
        try:
            response = s.get(url)
            response.raise_for_status()
        except requests.exceptions.ConnectionError:
            log.error("URL Connection Error for %s", url)
        except requests.exceptions.HTTPError:
            log.error('Error in URL request!')
    return response
