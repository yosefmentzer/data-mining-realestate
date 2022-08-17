"""
module to query data.gov.il's API and get demographic data per city/ishuv.
This module defines functions to be used by realestatescraper.py, thus there is no main() function.
"""


import requests


def get_records_next_url(url, domain):
    """
    The API returns records with a default batch size of 100.
    In each record, it includes a link to the next batch.
    This function, for a given url, queries the API and gets records
    and the url to the next batch of records.
    :param url: url to query the API
    :param domain: API domain
    :return: a list of dictionaries (records) and he url to the next batch of records
    """
    r = requests.get(url)
    records = r.json()['result']['records']
    next_link = r.json()['result']['_links']['next']
    next_url = ''.join([domain, next_link])

    if records:
        return records, next_url
    else:
        return None, None


def get_all_records(url, domain):
    """
    This function iteratively calls get_records_next_url() and stores records.
    Stop iteration when we get no results, meaning that previous batch of records was the last one.
    :param url: url to the first batch of results
    :param domain: API domain
    :return: list of dictionaries with all records for a given API query.
    """
    print('loading data from API...')
    all_records = []
    next_url = url
    while True:
        records, next_url = get_records_next_url(next_url, domain)
        if not records:
            break
        all_records.extend(records)
    return all_records
