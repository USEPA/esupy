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
import time


def make_url_request(url, *, set_cookies=False, confirm_gdrive=False,
                     max_attempts=3):
    """
    Makes http request using requests library
    :param url: URL to query
    :param set_cookies:
    :param confirm_gdrive:
    :param max_attempts: int number of retries allowed in query
    :return: request Object
    """
    session = (requests_ftp.ftp.FTPSession if urlsplit(url).scheme == 'ftp'
               else requests.Session)
    with session() as s:
        for attempt in range(max_attempts):
            try:
                # The session object s preserves cookies, so the second s.get()
                # will have the cookies that came from the first s.get()
                response = s.get(url)
                if set_cookies:
                    response = s.get(url)
                if confirm_gdrive:
                    response = s.get(url, params={'confirm': 't'})
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as err:
                if attempt < max_attempts - 1:
                    log.debug(err)
                    time.sleep(5)
                    continue
                else:
                    log.exception(err)
                    raise
    return response


# Alias for backward compatibility
def make_http_request(url):
    log.warning('esupy.remote.make_http_request() has been renamed to '
                'esupy.remote.make_url_request(). '
                'esupy.remote.make_http_request() will be removed in the '
                'future. Please modify your code accordingly.')
    return make_url_request(url)
