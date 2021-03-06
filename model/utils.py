import logging
import shutil
import os
from os.path import join

import pandas as pd
from hdx.data.dataset import Dataset

logger = logging.getLogger()


def query_api(folder, hdx_address, dataset_names=None):
    if dataset_names is None:
        dataset_names = []
    dataset = Dataset.read_from_hdx(hdx_address)
    resources = dataset.get_resources()
    filenames = {}
    for resource in resources:
        if resource['name'] in dataset_names or not dataset_names:
            _, path = resource.download()
            filename = os.path.basename(path)
            shutil.move(path, join(folder, filename))
            filenames[resource['name']] = filename
            logging.info(f'Saved {resource["name"]} to {folder}/{filename}')
    return filenames


def get_latest_year(df, country, start_year=2019, min_year=2009):
    country_row = df.loc[df['Country Name'] == country]
    y = start_year
    while y > min_year:
        try:
            if country_row[str(y)].values[0].astype(str) != 'nan':
                return str(y)
        except IndexError:
            pass
        y -= 1
    return str(min_year)


def get_pop_data(countries, data):
    # apply latest year to population data
    data_current = pd.DataFrame()
    for c in countries:
        year = get_latest_year(data, c)
        current_c = data[data['Country Name'] == c][['Country Name', 'Country Code', year]]
        current_c.columns = ['Country Name', 'Country Code', 'latest population']
        data_current = data_current.append(current_c)
    return data_current