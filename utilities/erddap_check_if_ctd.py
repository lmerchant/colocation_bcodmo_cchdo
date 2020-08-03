import urllib
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
    latitude_exists, latitude_col_index = is_ctd_column(column_names, ['lat'], ['lat_range'])
    longitude_exists, longitude_col_index = is_ctd_column(column_names, ['lon'], ['lon_range'])

    is_ctd = all([pressure_exists, temperature_exists, salinity_exists, 
                  latitude_exists, longitude_exists])

    return is_ctd


def create_ctd_dataframe(header_list, data_list):

    df = pd.DataFrame(data_list, columns = header_list)

    return df


def get_data_list(text, count):

    data_list = []

    split_text = text.split('\n')

    for line in split_text[count + 2:]:
        line_split = line.split('\t')
        data_list.append(line_split)

    return data_list


def fix_two_headers_into_one(text):

    split_text = text.split('\n')

    # newline in header so  header is over two lines
    # Combine two header lines
    count = 0
    for line in split_text:
        if '#' in line:
            count = count + 1
            continue
        else:
            break
 
    first_line = split_text[count].rstrip('\n')
    second_line = split_text[count + 1].rstrip('\n')

    first_line_list = first_line.split('\t')
    second_line_list = second_line.split('\t')
    first_line_list.extend(second_line_list)
    header_list = first_line_list

    return header_list, count


def get_data_text(url):

    response = urllib.request.urlopen(url)
    data = response.read()      # a `bytes` object
    try:
        text = data.decode('utf-8')
    except:
        text = None

    return text


def get_data_url(dataset_id):
    url = f'http://www.bco-dmo.org/dataset/{dataset_id}/data/download'
    return url    


def get_ctd_dataframe(dataset_id, processing_log):

    url = get_data_url(dataset_id)

    response = urllib.request.urlopen(url)

    content_type = response.headers['Content-Type']

    response = urllib.request.urlopen(url).read()

    # TODO. Get data out of zip file to check if ctd
    if content_type == 'application/zip':
        #check_zip_files(url)
        with open(processing_log, 'a+') as f:
            text = f"Zip file at dataset id: {dataset_id}\n"
            f.write(text)
        return None
        
    text = get_data_text(url)

    if text is None:
        with open(processing_log, 'a+') as f:
            log_text = f"Can't read text file. UTF-8 decode error at dataset id: {dataset_id}\n"
            f.write(log_text)
        return None

    try:
        ctd_df = pd.read_csv(io.StringIO(text), comment="#", sep="\t")

    except:

        # Log if multiple headers
        with open(processing_log, 'a+') as f:
            log_text = f"Two headers at dataset id: {dataset_id}\n"
            f.write(log_text)

        header_list, count = fix_two_headers_into_one(text)
        data_list = get_data_list(text, count)

        ctd_df = create_ctd_dataframe(header_list, data_list)


    return ctd_df


def check_if_ctd(dataset_id, processing_log):

      # Check if data is ctd
    ctd_df = get_ctd_dataframe(dataset_id, processing_log)
    
    if ctd_df is None:
        return False

    column_names = ctd_df.columns

    is_ctd = check_column_names_for_ctd(column_names)

    return is_ctd
