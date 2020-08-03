import requests
from bs4 import BeautifulSoup
import json


def is_ctd_value(parameter_names, needles, exclude):

    parameter_names = [name.lower() for name in parameter_names]

    is_ctd_value = False

    needle_vals_found = []
    exclude_vals_found = []

    # Find excluded values if any
    for val in exclude:
        #val_found = [name for name in parameter_names if name.startswith(val)]
        val_found = [name for name in parameter_names if val in name]

        if val_found:
            exclude_vals_found.extend(val_found)

    # Find needle values if any
    for val in needles:
        #val_found = [name for name in parameter_names if name.startswith(val)]
        val_found = [name for name in parameter_names if val in name]

        if val_found:
            needle_vals_found.extend(val_found)

    # remove exclude cols from needle cols
    for exclude in exclude_vals_found:
        if exclude in needle_vals_found:
            needle_vals_found.remove(exclude)

    if needle_vals_found:
        is_ctd_value = True

    print('needle vals found', needle_vals_found)

    return is_ctd_value


def check_parameter_names_for_ctd(parameter_names):

    pressure_exists = is_ctd_value(parameter_names, ['ctdprs', 'press'], [])
    temperature_exists = is_ctd_value(parameter_names, ['ctdtmp', 'temp'], [])
    salinity_exists = is_ctd_value(parameter_names, ['ctdsal', 'sal'], [])
    latitude_exists = is_ctd_value(parameter_names, ['lat'], ['lat_range'])
    longitude_exists = is_ctd_value(parameter_names, ['lon'], ['lon_range'])

    is_ctd = all([pressure_exists, temperature_exists, salinity_exists, latitude_exists, longitude_exists])

    return is_ctd


def get_dataset_soup(url):

    try:
        response = requests.get(url)
        dataset_soup = BeautifulSoup(response.text, 'html.parser')

    except requests.exceptions.RequestException as e:
        # response doesn't exist
        dataset_soup = None

        # # Can't reach data page
        # # Log it
        # with open(processing_log, 'a+') as f:
        #     text = f"Site not reached for dataset id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
        #     f.write(text)

    # No dataset available
    if dataset_soup and not dataset_soup(text='Data URL:'):

        # with open(processing_log, 'a+') as f:
        #     text = f"No data set for id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
        #     f.write(text)

        dataset_soup = None

    return dataset_soup


def get_jsonld(url):

    # Get html soup of dataset web page 
    dataset_soup = get_dataset_soup(url)

    try:
        data = dataset_soup.select("[type='application/ld+json']")[1]

        jsonld = json.loads(data.text)

        return jsonld

    except:

        return None   


def get_dataset_url(dataset_id):

    url = 'https://www.bco-dmo.org/dataset/' + dataset_id
    return url


def check_if_ctd(dataset_id, processing_log):

    url = get_dataset_url(dataset_id)

    print(url)

    jsonld = get_jsonld(url)

    variables_measured = jsonld["variableMeasured"]

    parameter_names = []

    for variable in variables_measured:
        parameter = variable["name"]
        # name = parameter.split(',')[0]
        # name = parameter.split(' ')[0]
        parameter_names.append(parameter)

    is_ctd = check_parameter_names_for_ctd(parameter_names)

    return is_ctd


