import requests


def get_records_next_url(url, domain):
    r = requests.get(url)
    records = r.json()['result']['records']
    next_link = r.json()['result']['_links']['next']
    next_url = ''.join([domain, next_link])

    if records:
        return records, next_url
    else:
        return None, None


def get_all_records(url, domain):

    print('loading data from API...')
    all_records = []
    next_url = url
    while True:
        records, next_url = get_records_next_url(next_url, domain)
        if not records:
            break
        all_records.extend(records)
    return all_records
