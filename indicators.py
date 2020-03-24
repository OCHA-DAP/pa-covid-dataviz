import shutil
import os
import logging

import pandas as pd
from hdx.hdx_configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.easy_logging import setup_logging

import config


HDX_SITE = 'prod'
USER_AGENT = 'Centre COVID-19 Dashboard'

WORLD_BANK_HDX_ADDRESS = 'world-bank-indicators-of-interest-to-the-covid-19-outbreak'
WORLD_BANK_DATASET_NAMES = [
    'Age and Population - Population ages 65 and above (% of total)',
    'Water & Sanitation - People with basic handwashing facilities including soap and water (% of population)',
    # Placeholder for HIV indicator name
    'Health - Hospital beds (per 1,000 people)',
    'Health - Physicians (per 1,000 people)',
    'Health - Nurses and midwives (per 1,000 people)',
    'Health - Out-of-pocket expenditure per capita, PPP (current international $)'
]

PEOPLE_IN_NEED_HDX_ADDRESS = 'global-humanitarian-overview-2020-figures'
PEOPLE_IN_NEED_INDICATOR = 'Number of people in need'

COLUMNS = ['Indicator', 'ISO3', 'Country', 'Value', 'Last Updated']

setup_logging()
logger = logging.getLogger()
Configuration.create(hdx_site=HDX_SITE, hdx_read_only=True, user_agent=USER_AGENT)


def create_dataframe(debug=False):
    df_main = pd.DataFrame(columns=COLUMNS)
    if not debug:
        filenames = query_api_for_world_bank_data()
    else:
        filenames = {d: f'{d}.XLSX' for d in WORLD_BANK_DATASET_NAMES}
    for indicator, filename in filenames.items():
        data_current = extract_data_from_excel(f'data/{filename}')
        data_current['Indicator'] = indicator
        df_main = df_main.append(data_current)
    # Get people in need
    if not debug:
        needs_filename = query_api_for_people_in_need()
    else:
        needs_filename = f'{PEOPLE_IN_NEED_INDICATOR}.XLSX'
    df_main = df_main.append(get_number_of_people_in_need_per_country(needs_filename))
    df_main.to_excel(config.output_filename, sheet_name='Indicator', index=False)


def query_api_for_world_bank_data():
    dataset = Dataset.read_from_hdx(WORLD_BANK_HDX_ADDRESS)
    resources = dataset.get_resources()
    filenames = {}
    for resource in resources:
        if resource['name'] in WORLD_BANK_DATASET_NAMES:
            _, path = resource.download()
            filename = os.path.basename(path)
            shutil.move(path, f'data/{filename}')
            filenames[resource['name']] = filename
            logging.info(f'Saved {resource["name"]} to data/{filename}')
    return filenames


def extract_data_from_excel(excel_path):
    output_columns = ['Country', 'ISO3', 'Value', 'Last Updated']
    data = pd.read_excel(excel_path, sheet_name='Data', header=3)
    data_current = pd.DataFrame(columns=output_columns)
    for c in config.countries:
        year = get_latest_year(data, c)
        current_c = data[data['Country Name'] == c][['Country Name', 'Country Code', year]]
        current_c['year'] = year
        current_c.columns = output_columns
        data_current = data_current.append(current_c)
    global_vals = []
    for c in data['Country Name'].unique():
        year = get_latest_year(data, c)
        val = data.loc[data['Country Name'] == c][year].values[0].astype(str)
        if val != 'nan':
            global_vals = global_vals+[data.loc[data['Country Name']==c][year].tolist()[0]]
    global_baseline = sum(global_vals)/len(global_vals)
    data_current = data_current.append(
        pd.DataFrame({'Country': 'Global', 'ISO3': 'Global',
                      'Value': global_baseline}, index=[0]))
    return data_current


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


def query_api_for_people_in_need():
    dataset = Dataset.read_from_hdx(PEOPLE_IN_NEED_HDX_ADDRESS)
    resource = dataset.get_resources()[0]
    _, path = resource.download()
    filename = f'{PEOPLE_IN_NEED_INDICATOR}.XLSX'
    shutil.move(path, f'data/{filename}')
    logging.info(f'Saved {resource["name"]} to data/{filename}')
    return filename


def get_number_of_people_in_need_per_country(filename):
    data = pd.read_excel(f'data/{filename}', sheet_name='Raw Data').sort_values(by='Year')
    output_columns = ['Country', 'ISO3', 'Value', 'Last Updated']
    data_current = pd.DataFrame(columns=output_columns)
    for c in config.countries:
        current_c = data[(data['Crisis Country'] == c) &
                         (data['Metric'] == 'People in need')][
            ['Crisis Country', 'Country Code', 'Value', 'Year']].tail(1)
        current_c.columns = output_columns
        data_current = data_current.append(current_c)
    data_current['Indicator'] = PEOPLE_IN_NEED_INDICATOR
    return data_current


if __name__ == "__main__":
    create_dataframe()