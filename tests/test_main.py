from os.path import join

import pytest
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.path import temp_dir

from main import get_indicators


class TestCovid:
    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}, {'name': 'pse', 'title': 'State of Palestine'}])
        Country.countriesdata(use_live=False)

    @pytest.fixture(scope='function')
    def folder(self):
        return join('tests', 'fixtures')

    def test_get_indicators(self, configuration, folder):
        configuration = Configuration.read()
        countries = configuration['countries']
        palestine_country_code, _ = Country.get_iso3_country_code_fuzzy('Palestine')
        df_indicators, df_timeseries, df_cumulative = get_indicators(folder, countries, palestine_country_code, True)
        with temp_dir('PA-COVID-TEST') as outdir:
            filename = 'indicators.csv'
            outputfile = join(outdir, filename)
            df_indicators.to_csv(outputfile)
            assert_files_same(join(folder, filename), outputfile)
            filename = 'timeseries.csv'
            outputfile = join(outdir, filename)
            df_timeseries.to_csv(outputfile)
            assert_files_same(join(folder, filename), outputfile)
            filename = 'cumulative.csv'
            outputfile = join(outdir, filename)
            df_cumulative.to_csv(outputfile)
            assert_files_same(join(folder, filename), outputfile)
