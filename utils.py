import logging
import shutil
import os

import pandas as pd
from hdx.hdx_configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.easy_logging import setup_logging

import config


HDX_SITE = 'prod'
USER_AGENT = 'Centre COVID-19 Dashboard'

setup_logging()
logger = logging.getLogger()

Configuration.create(hdx_site=HDX_SITE, hdx_read_only=True, user_agent=USER_AGENT)


def query_api(hdx_address, dataset_names=None):
    if dataset_names is None:
        dataset_names = []
    dataset = Dataset.read_from_hdx(hdx_address)
    resources = dataset.get_resources()
    filenames = {}
    for resource in resources:
        if resource['name'] in dataset_names or not dataset_names:
            _, path = resource.download()
            filename = os.path.basename(path)
            shutil.move(path, f'data/{filename}')
            filenames[resource['name']] = filename
            logging.info(f'Saved {resource["name"]} to data/{filename}')
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


def get_pop_data(data):
    # apply latest year to population data
    data_current = pd.DataFrame()
    for c in config.countries:
        year = get_latest_year(data, c)
        current_c = data[data['Country Name'] == c][['Country Name', 'Country Code', year]]
        current_c.columns = ['Country Name', 'Country Code', 'latest population']
        data_current = data_current.append(current_c)
    return data_current