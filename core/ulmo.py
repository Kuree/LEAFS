"""
    ulmo.ncdc.ghcn_daily.core
    ~~~~~~~~~~~~~~~~~~~~~~~~~
    This module provides direct access to `National Climatic Data Center`_
    `Global Historical Climate Network - Daily`_ dataset.
    .. _National Climatic Data Center: http://www.ncdc.noaa.gov
    .. _Global Historical Climate Network - Daily: http://www.ncdc.noaa.gov/oa/climate/ghcn-daily/
"""
import itertools
import os

import numpy as np
import pandas


GHCN_DAILY_DIR = "E:\\SensorData\\ghcnd_all"


def get_data(station_id, elements=None):
    """Retrieves data for a given station.
    Parameters
    ----------
    station_id : str
        Station ID to retrieve data for.
    elements : ``None``, str, or list of str
        If specified, limits the query to given element code(s).
    Returns
    -------
    site_dict : dict
    """

    start_columns = [
        ('year', 11, 15, int),
        ('month', 15, 17, int),
        ('element', 17, 21, str),
    ]
    value_columns = [
        ('value', 0, 5, float),
        ('mflag', 5, 6, str),
        ('qflag', 6, 7, str),
        ('sflag', 7, 8, str),
    ]
    columns = list(itertools.chain(start_columns, *[
        [(name + str(n), start + 13 + (8 * n), end + 13 + (8 * n), converter)
         for name, start, end, converter in value_columns]
        for n in range(1, 32)
    ]))

    station_file_path = _get_ghcn_file(station_id + '.dly')
    station_data = parse_fwf(station_file_path, columns, na_values=[-9999])

    dataframes = {}

    for element_name, element_df in station_data.groupby('element'):
        if not elements is None and element_name not in elements:
            continue

        element_df['month_period'] = element_df.apply(
                lambda x: pandas.Period('%s-%s' % (x['year'], x['month'])),
                axis=1)
        element_df = element_df.set_index('month_period')
        monthly_index = element_df.index

        # here we're just using pandas' builtin resample logic to construct a daily
        # index for the timespan
        daily_index = element_df.resample('D').index.copy()

        # XXX: hackish; pandas support for this sort of thing will probably be
        # added soon
        month_starts = (monthly_index - 1).asfreq('D') + 1
        dataframe = pandas.DataFrame(
                columns=['value', 'mflag', 'qflag', 'sflag'], index=daily_index)

        for day_of_month in range(1, 32):
            dates = [date for date in (month_starts + day_of_month - 1)
                    if date.day == day_of_month]
            if not len(dates):
                continue
            months = pandas.PeriodIndex([pandas.Period(date, 'M') for date in dates])
            for column_name in dataframe.columns:
                col = column_name + str(day_of_month)
                dataframe[column_name][dates] = element_df[col][months]

        dataframes[element_name] = dataframe

    return dataframes


def _get_ghcn_file(filename):
    # TODO:
    # HERE HARD CODED PATH IS USED BECAUSE THE DATA FILE IS HUGE
    # CONSIDER MOVE IT TO ONLINE DATABASE
    path = os.path.join(GHCN_DAILY_DIR, filename)
    return path



def parse_fwf(file_path, columns, na_values=None):
    """Convenience function for parsing fixed width formats. Wraps the pandas
    read_fwf parser but allows all column information to be kept together.
    Columns should be an iterable of lists/tuples with the format (column_name,
    start_value, end_value, converter). Returns a pandas dataframe.
    """
    names, colspecs = zip(*[(name, (start, end))
        for name, start, end, converter in columns])

    converters = dict([
        (name, converter)
        for name, start, end, converter in columns
        if not converter is None
    ])

    return pandas.io.parsers.read_fwf(file_path,
        colspecs=colspecs, header=None, na_values=na_values, names=names,
        converters=converters)