from os.path import join

import pandas as pd

from model import utils

CUMULATIVE_HDX_ADDRESS = 'coronavirus-covid-19-cases-data-for-china-and-the-rest-of-the-world'
CUMULATIVE_DATASET_NAME = 'covid-19 cases by country.csv'

POP_HDX_ADDRESS = 'world-bank-indicators-of-interest-to-the-covid-19-outbreak'
POP_DATASET_NAME = 'Total Population'

COLUMNS = ['Date', 'ISO3', 'Country', 'Active Cases', 'Total Deaths', 'Quart ile']


def create_dataframe(folder, countries, palestine_country_code, debug=False):
    # Download data and read in
    if not debug:
        filename = list(utils.query_api(folder, CUMULATIVE_HDX_ADDRESS, [CUMULATIVE_DATASET_NAME]).values())[0]
        filename_pop = list(utils.query_api(folder, POP_HDX_ADDRESS, [POP_DATASET_NAME]).values())[0]
    else:
        filename = f'{CUMULATIVE_DATASET_NAME}.CSV'
        filename_pop = f'{POP_DATASET_NAME}.XLSX'
    # read them in
    case_data = pd.read_csv(join(folder, filename))
    case_data['ADM0_NAME'] = case_data['ADM0_NAME'].replace({
        'Syrian Arab Republic': 'Syria',
        'Venezuela (Bolivarian Republic of)': 'Venezuela'
         })
    pop = pd.read_excel(join(folder, filename_pop), sheet_name='Data', header=3)
    pop['Country Name'] = pop['Country Name'].replace({
                                                       'Syrian Arab Republic': 'Syria',
                                                       'Venezuela, RB': 'Venezuela',
                                                       'Congo, Dem. Rep.': 'Democratic Republic of the Congo',
                                                       'Yemen, Rep.': 'Yemen',
                                                       'West Bank and Gaza': 'occupied Palestinian territory'
    })
    # Get cumulative data
    cumulative = case_data.loc[case_data['ADM0_NAME'].isin(countries),
                               ['ADM0_NAME', 'cum_conf', 'cum_death', 'DateOfReport']]
    cumulative.columns = ['Country', 'confirmed cases', 'deaths', 'last_updated']
    # Combine with pop
    pop_data = utils.get_pop_data(countries, pop)
    cumulative = cumulative.merge(pop_data[['Country Name', 'Country Code', 'latest population']], left_on='Country',
                                  right_on='Country Name', how='left')
    cumulative = cumulative.drop('Country Name', axis=1)
    cumulative['Country Code'].loc[cumulative['Country']
                                   == 'occupied Palestinian territory'] = palestine_country_code
    # Get per 100000
    cumulative['pop 100000'] = cumulative['latest population']/100000
    cumulative['confirmed cases per 100000'] = cumulative['confirmed cases']/cumulative['pop 100000']
    cumulative['deaths per 100000'] = cumulative['deaths']/cumulative['pop 100000']
    # Get global cases
    global_cases_and_deaths = case_data[['cum_conf', 'cum_death']].sum()
    n_countries = len(case_data['ADM0_NAME'].unique())
    cumulative = cumulative.append({'Country': 'Global',
                                    'confirmed cases': global_cases_and_deaths['cum_conf'],
                                    'deaths': global_cases_and_deaths['cum_death']}, ignore_index=True)
    cumulative['n_countries'] = 1
    cumulative.loc[cumulative['Country'] == 'Global', 'n_countries'] = n_countries
    return cumulative
