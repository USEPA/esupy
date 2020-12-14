import logging as log
import requests
import requests_ftp

def make_http_request(url):
    """
    Makes http request using requests library
    :param url: URL to query
    :return: request Object
    """
    r = []
    try:
        r = requests.get(url)
    except requests.exceptions.InvalidSchema: # if url is ftp rather than http
        requests_ftp.monkeypatch_session()
        r = requests.Session().get(url)
    except requests.exceptions.ConnectionError:
        log.error("URL Connection Error for " + url)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        log.error('Error in URL request!')
    return r

