"""

Get dataset_ids, titles, and investigators from dataset search pages

"""

import re
import urllib.request
import requests
import json


def get_titles(html):

    titles = re.findall(r'https://www.bco-dmo.org/dataset/\d+">(.*?)</a>', html)
    return titles


def get_dataset_ids(html):

    dataset_ids = re.findall(r'https://www.bco-dmo.org/dataset/(\d+)', html)

    return dataset_ids


def get_dataset_ids_from_json(page_json):
    rows = page_json['table']['rows']

    # of form bcodmo_dataset_2388
    dataset_ids = [re.search('bcodmo_dataset_(\d+)',row[-1]) for row in rows]

    return dataset_ids


def get_investigators(html):

    investigators = re.findall(r'>Principal Investigator</em>.*?person.+?">(.*?)</a>', html)
    return investigators


def are_results(html):
    
    # No more results if find text "No results found"
    found_text = re.findall('No results found', html)

    if found_text:
        are_results = False
    else:
        are_results = True

    return are_results


def get_dataset_ids_search_page_json(url):

    r = requests.get(url)

    if r.status_code != 200:
        return None

    return r.json()


def get_dataset_ids_search_page_html(url):

    #req = urllib.request.Request(url)

    try:
        response = requests.get(url)
        html = response.text

        return html

    except requests.exceptions.RequestException as e:
        print(e)
        with open(processing_log, 'a+') as f:
            log_text = f"Can't read html file. {e} error at dataset id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
            f.write(log_text) 


def get_search_url(page):

    search_term = 'ctd'

    return f"https://www.bco-dmo.org/search/dataset/{search_term}?page={str(page)}&size=20"

    # page starts at 1 and is only page at the moment
    
    # return f"https://erddap.bco-dmo.org/erddap/search/index.json?page={str(page)}&itemsPerPage=1000&searchFor={search_term}"


def get_json_results(page):

    # get json listing of datasets

    url = get_search_url(page)

    json = get_dataset_ids_search_page_json(url)

    if json:
        page = page + 1
    else: 
        page = -1

    return page, json


def get_html_results(page):

    url = get_search_url(page)

    html = get_dataset_ids_search_page_html(url)

    if are_results(html):
        page = page + 1
    else:
        page = -1

    return page, html


def get_metadata(page):

    all_page_dataset_ids = []
    all_titles = []
    all_investigators = []

    while(page != -1):

        # if page == 2:
        #     break

        print("Processing page #: ", page+1)

        # TODO Fix to increment page at end of while loop and 
        # not inside get_html_results

        page, html = get_html_results(page)
        #page_erddap, page_json = get_json_results(page)

        dataset_ids = get_dataset_ids(html) 
        # erdapp_dataset_ids = get_dataset_ids_from_json(page_json)

        page_list = [(page - 1) for id in dataset_ids]

        page_dataset_ids = list(zip(page_list, dataset_ids))

        all_page_dataset_ids.extend(page_dataset_ids)

        titles = get_titles(html)
        all_titles.extend(titles)

        investigators = get_investigators(html)
        all_investigators.extend(investigators)

    return all_page_dataset_ids, all_titles, all_investigators

