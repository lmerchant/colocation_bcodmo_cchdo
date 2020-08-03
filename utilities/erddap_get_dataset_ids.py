import re
import urllib.request
import requests
import json


def search_json_for_dataset_ids(page_json):

    rows = page_json['table']['rows']

    # text of form bcodmo_dataset_2388
    dataset_ids = [re.findall('bcodmo_dataset_(\d+)',row[-1])[0] for row in rows]

    return dataset_ids


def are_results(page_json):
    
    # More results if find 'matching datasets' text
    found_text = re.findall('matching datasets', html)

    if found_text:
        are_results = True
    else:
        are_results = False

    return are_results


def get_search_page_json(url):

    r = requests.get(url)

    if r.status_code != 200:
        return None

    return r.json()


def get_erddap_url(page, num_items_per_page):

    search_term = 'ctd'
  
    return f"https://erddap.bco-dmo.org/erddap/search/index.json?page={str(page)}&itemsPerPage={str(num_items_per_page)}&searchFor={search_term}"


def get_ctd_dataset_ids():

    all_ctd_dataset_ids = []
 
    page = 1
    num_items_per_page = 1000

    while (page != -1):

        # Get url of ERDDAP page
        url = get_erddap_url(page, num_items_per_page)

        page_json = get_search_page_json(url)

        if page_json:
            page = page + 1
        else:
            page = -1
            continue

        # Scrape url for ctd dataset ids
        dataset_ids = search_json_for_dataset_ids(page_json)

        all_ctd_dataset_ids.extend(dataset_ids)

    return all_ctd_dataset_ids





