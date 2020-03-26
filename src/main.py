import timeseries, cumulative, indicators


def get_indicators(folder, countries, palestine_country_code, debug):
    df_indicators = indicators.create_dataframe(folder, countries, debug=debug)
    df_timeseries = timeseries.create_dataframe(folder, countries, palestine_country_code, debug=debug)
    df_cumulative = cumulative.create_dataframe(folder, countries, palestine_country_code, debug=debug)
    return df_indicators, df_timeseries, df_cumulative
