import pandas as pd

import config
import utils


CUMULATIVE_HDX_ADDRESS = 'coronavirus-covid-19-cases-data-for-china-and-the-rest-of-the-world'
CUMULATIVE_DATASET_NAME = 'covid-19 cases by country.csv'

POP_HDX_ADDRESS = 'world-bank-indicators-of-interest-to-the-covid-19-outbreak'
POP_DATASET_NAME = 'Total Population'

COLUMNS = ['Date', 'ISO3', 'Country', 'Active Cases', 'Total Deaths', 'Quart ile']


def create_dataframe(debug=False):
    # Download data and read in
    if not debug:
        filename = list(utils.query_api(CUMULATIVE_HDX_ADDRESS, [CUMULATIVE_DATASET_NAME]).values())[0]
        filename_pop = list(utils.query_api(POP_HDX_ADDRESS, [POP_DATASET_NAME]).values())[0]
    else:
        filename = f'{CUMULATIVE_DATASET_NAME}.CSV'
        filename_pop = f'{POP_DATASET_NAME}.XLSX'
    # read them in
    case_data = pd.read_csv(f'data/{filename}')
    case_data['ADM0_NAME'] = case_data['ADM0_NAME'].replace({
        'Venezuela (Bolivarian Republic of)': 'Venezuela',
        'occupied Palestinian territory': 'Occupied Palestinian Territory'
         })
    pop = pd.read_excel(f'data/{filename_pop}', sheet_name='Data', header=3)
    pop['Country Name'] = pop['Country Name'].replace({
                                                       'Syrian Arab Republic': 'Syria',
                                                       'Venezuela, RB': 'Venezuela',
                                                       'Congo, Dem. Rep.': 'Democratic Republic of the Congo'
                                                       })
    # Get cumulative data
    cumulative = case_data.loc[case_data['ADM0_NAME'].isin(config.countries), ['ADM0_NAME', 'cum_conf', 'cum_death']]
    cumulative.columns = ['Country', 'confirmed cases', 'deaths']
    # Combine with pop
    pop_data = utils.get_pop_data(pop)
    cumulative = cumulative.merge(pop_data[['Country Name', 'Country Code', 'latest population']], left_on='Country',
                                  right_on='Country Name', how='left')
    cumulative = cumulative.drop('Country Name', axis=1)
    # Get per 100000
    cumulative['pop 100000'] = cumulative['latest population']/100000
    cumulative['confirmed cases per 100000'] = cumulative['confirmed cases']/cumulative['pop 100000']
    cumulative['deaths per 100000'] = cumulative['deaths']/cumulative['pop 100000']
    return cumulative
