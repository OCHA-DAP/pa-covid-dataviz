import pandas as pd

import utils


WORLD_BANK_HDX_ADDRESS = 'world-bank-indicators-of-interest-to-the-covid-19-outbreak'
WORLD_BANK_DATASET_NAMES = [
    'Age and Population - Population ages 65 and above (% of total)',
    'Water & Sanitation - People with basic handwashing facilities including soap and water (% of population)',
    'Health - Prevalence of HIV, total (% of population ages 15-49)',
    'Health - Hospital beds (per 1,000 people)',
    'Health - Physicians (per 1,000 people)',
    'Health - Nurses and midwives (per 1,000 people)',
    'Health - Out-of-pocket expenditure per capita, PPP (current international $)'
]

PEOPLE_IN_NEED_HDX_ADDRESS = 'global-humanitarian-overview-2020-figures'
PEOPLE_IN_NEED_INDICATOR = 'Number of people in need'
PEOPLE_IN_NEED_FILENAME = 'Humanitarian Needs and Funding 2011-2020.xlsx'  # for debug mode

COLUMNS = ['Indicator', 'ISO3', 'Country', 'Value', 'Last Updated']


def create_dataframe(countries, debug=False):
    df_main = pd.DataFrame(columns=COLUMNS)
    if not debug:
        filenames = utils.query_api(WORLD_BANK_HDX_ADDRESS, dataset_names=WORLD_BANK_DATASET_NAMES)
    else:
        filenames = {d: f'{d}.XLSX' for d in WORLD_BANK_DATASET_NAMES}
    for indicator, filename in filenames.items():
        data_current = extract_data_from_excel(countries, f'data/{filename}')
        data_current['Indicator'] = indicator
        df_main = df_main.append(data_current)
    # Get people in neeonfirmed.csvd
    #if not debug:
    needs_filename = list(utils.query_api(PEOPLE_IN_NEED_HDX_ADDRESS).values())[0]
    #else:
    #   needs_filename = f'{PEOPLE_IN_NEED_INDICATOR}.XLSX'
    df_main = df_main.append(get_number_of_people_in_need_per_country(countries, needs_filename))
    return df_main


def extract_data_from_excel(countries, excel_path):
    output_columns = ['Country', 'ISO3', 'Value', 'Last Updated']
    data = pd.read_excel(excel_path, sheet_name='Data', header=3)
    data_current = pd.DataFrame(columns=output_columns)
    for c in countries:
        year = utils.get_latest_year(data, c)
        current_c = data[data['Country Name'] == c][['Country Name', 'Country Code', year]]
        current_c['year'] = year
        current_c.columns = output_columns
        data_current = data_current.append(current_c)
    global_vals = []
    for c in data['Country Name'].unique():
        year = utils.get_latest_year(data, c)
        val = data.loc[data['Country Name'] == c][year].values[0].astype(str)
        if val != 'nan':
            global_vals = global_vals+[data.loc[data['Country Name']==c][year].tolist()[0]]
    global_baseline = sum(global_vals)/len(global_vals)
    data_current = data_current.append(
        pd.DataFrame({'Country': 'Global', 'ISO3': 'Global',
                      'Value': global_baseline}, index=[0]))
    return data_current


def get_number_of_people_in_need_per_country(countries, filename):
    data = pd.read_excel(f'data/{filename}', sheet_name='Raw Data').sort_values (by='Year')
    output_columns = ['Country', 'ISO3', 'Value', 'Last Updated']
    data_current = pd.DataFrame(columns=output_columns)
    for c in countries:
        current_c = data[(data['Crisis Country'] == c) &
                         (data['Metric'] == 'People in need')][
            ['Crisis Country', 'Country Code', 'Value', 'Year']].tail(1)
        current_c.columns = output_columns
        data_current = data_current.append(current_c)
    data_current['Indicator'] = PEOPLE_IN_NEED_INDICATOR
    return data_current
