import timeseries, cumulative, indicators


def get_indicators(countries, palestine_country_code, debug):
    df_indicators = indicators.create_dataframe(countries, debug)
    df_timeseries = timeseries.create_dataframe(countries, palestine_country_code, debug)
    df_cumulative = cumulative.create_dataframe(countries, palestine_country_code, debug)
    return df_indicators, df_timeseries, df_cumulative
