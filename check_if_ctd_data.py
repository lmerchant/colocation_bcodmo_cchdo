import urllib3
import pandas as pd
import io
from itertools import compress
import os
import tempfile
import requests
import zipfile
import gzip
from gzip import decompress
from io import StringIO


def check_zip_files(url):

    # data = response.read()      # a `bytes` object

        #request = requests.get(url)

        #file = zipfile.ZipFile(io.BytesIO(request.content))

        # https://stackoverflow.com/questions/55718917/download-zip-file-locally-to-tempfile-extract-files-to-tempfile-and-list-the-f
        results = requests.get(url)

        tmpdir = tempfile.mkdtemp()

        zip_path = os.path.join(tmpdir, 'zip_folder.zip')

        with open(zip_path, 'wb') as f:
            f.write(results.content)

        file = zipfile.ZipFile(tmpdir + '/zip_folder.zip')
        file.extractall(path=tmpdir)

        files = os.listdir(tmpdir)
        for file in files:
            if 'txt' in file:
                pass
                #print(file)


    # if content_encoding == 'gzip' or 'zip'

    #     # Skip file but keep record to a zip file log
    #     # Store, page, dataset id, and content type

    #     # if zip file
    #     file = zipfile.ZipFile(io.BytesIO(response.content))

    #     # if gzip file
    #     # compressedFile = StringIO(response.text)
    #     # decompressedFile = gzip.GzipFile(fileobj=compressedFile)

    #     # if gzip file
    #     # file = decompress(response.content)    


def is_ctd_column(column_names, needles, exclude):

    column_names = [name.lower() for name in column_names]

    is_ctd_column = False

    needle_vals_found = []
    exclude_vals_found = []

    # Find excluded values if any
    for val in exclude:
        val_found = [name for name in column_names if name.startswith(val)]

        if val_found:
            exclude_vals_found.extend(val_found)

    # Find needle values if any
    for val in needles:
        val_found = [name for name in column_names if name.startswith(val)]

        if val_found:
            needle_vals_found.extend(val_found)

    # remove exclude cols from needle cols
    for exclude in exclude_vals_found:
        if exclude in needle_vals_found:
            needle_vals_found.remove(exclude)

    if needle_vals_found:
        is_ctd_column = True

        # Find index of name
        ctd_col_index = [column_names.index(val) for val in needle_vals_found][0]

    else: 
        ctd_col_index = None


    print('needle vals found', needle_vals_found)

    return is_ctd_column, ctd_col_index


def check_column_names_for_ctd(column_names):

    pressure_exists, pressure_col_index = is_ctd_column(column_names, ['ctdprs', 'press'], [])
    temperature_exists, temperature_col_index = is_ctd_column(column_names, ['ctdtmp', 'temp'], [])
    salinity_exists, salinity_col_index = is_ctd_column(column_names, ['ctdsal', 'sal'], [])
    latitude_exists, latitude_col_index = is_ctd_column(column_names, ['lat'], ['lat_range', 'lat_start'])
    longitude_exists, longitude_col_index = is_ctd_column(column_names, ['lon'], ['lon_range', 'lon_start'])

    # TODO. Exclude lat_start or not? maybe lat_end?

    is_ctd = all([pressure_exists, temperature_exists, salinity_exists, 
                  latitude_exists, longitude_exists])

    print(pressure_col_index, latitude_col_index, longitude_col_index)

    return is_ctd, pressure_col_index, latitude_col_index, longitude_col_index


def create_dataset_dataframe(header_list, data_list):

    try:

        df = pd.DataFrame(data_list, columns = header_list)

    except:
        df = None
    
    return df


def get_data_list(text, line_count):

    data_list = []

    split_text = text.split('\n')

    for line in split_text[line_count + 2:]:
        line_split = line.split('\t')
        data_list.append(line_split)

    return data_list


def fix_two_headers_into_one(text):

    split_text = text.split('\n')

    # newline in header so  header is over two lines
    # Combine two header lines
    line_count = 0
    for line in split_text:
        if '#' in line:
            line_count = line_count + 1
            continue
        else:
            break

    # TODO
    # assuming only two header lines. Check if 
    # more comment lines and log out can't get data.

    first_line = split_text[line_count].rstrip('\n')
    second_line = split_text[line_count + 1].rstrip('\n')

    first_line_list = first_line.split('\t')
    second_line_list = second_line.split('\t')
    first_line_list.extend(second_line_list)
    header_list = first_line_list

    return header_list, line_count


def get_data_text(url, processing_log, dataset_id, page, number_of_datasets_per_page):

    try:
        # response = urllib3.request.urlopen(url)
        # data = response.read()      

        response = requests.get(url, timeout=600)
        data_text = response.text

    except requests.exceptions.RequestException as e:

        print(e)

        with open(processing_log, 'a+') as f:
            log_text = f"Can't read text file. {e} error at dataset id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
            f.write(log_text) 

    
    # try:
    #     # data is a `bytes` object
    #     text = data.decode('utf-8')
    # except:
    #     text = None


    return data_text


def get_data_url(dataset_id):
    url = f'http://www.bco-dmo.org/dataset/{dataset_id}/data/download'
    return url    


def put_dataset_into_dataframe(dataset_id, processing_log, page, index, number_of_datasets_per_page):

    url = get_data_url(dataset_id)

    # To get content-type and see if it is zip
    try:
        response = requests.get(url, timeout=600)
        content_type = response.headers['Content-Type']

        # response = urllib3.request.urlopen(url)
        # content_type = response.headers['Content-Type']
        
    except requests.exceptions.RequestException as e:
        with open(processing_log, 'a+') as f:
            log_text = f"Can't read text file. {e} error at dataset id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
            f.write(log_text)

        return None

    # try:
    #     #response = urllib3.request.urlopen(url).read()

    # except HTTPError as e:
    #     with open(processing_log, 'a+') as f:
    #         log_text = f"Can't read text file. {e} error at dataset id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
    #         f.write(log_text)

    #     return None


    # TODO. Get ctd data out of zip file to get lon/lat data
    if content_type == 'application/zip':
        #check_zip_files(url)
        return None
        
    text = get_data_text(url, processing_log, dataset_id, page, number_of_datasets_per_page)

    if text is None:
        with open(processing_log, 'a+') as f:
            log_text = f"Can't read text file. UTF-8 decode error at dataset id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
            f.write(log_text)

        return None

    try:
        # If fail, dataset usually has two headers where they
        # should be one
        dataset_df = pd.read_csv(io.StringIO(text), comment="#", sep="\t")

    except:
        # Log two headers occuring 
        with open(processing_log, 'a+') as f:
            log_text = f"Can't read data into pandas. Try to account for header split over two lines at dataset id: {dataset_id} on page {page + 1} with {number_of_datasets_per_page} data sets on page\n"
            f.write(log_text)

        header_list, line_count = fix_two_headers_into_one(text)
        data_list = get_data_list(text, line_count)

        dataset_df = create_dataset_dataframe(header_list, data_list)


    return dataset_df

