# dqi.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions for processing and reporting life cycle data quality indicators
"""

import pandas as pd
import numpy as np


temporal_correlation_to_dqi = {3: 1,
                               6: 2,
                               10: 3,
                               15: 4,
                               None: 5}

data_collection_to_dqi = {0.4: 4,
                          0.6: 3,
                          0.8: 2,
                          1: 1,
                          None: 5}

dqi_dict = {'DataReliability':None,
            'TemporalCorrelation':temporal_correlation_to_dqi,
            'GeographicalCorrelation':None,
            'TechnologicalCorrelation':None,
            'DataCollection':data_collection_to_dqi
            }


def apply_dqi_to_series(source_series, indicator, bound_to_dqi=None):
    """
    Returns a series of indicator scores based on dictionary boundaries
    applied to the source_series
    e.g. df['TemporalCorrelation'] = apply_dqi_to_series(
        df['Year2']-df['Year1'],'TemporalCorrelation')
    """
    if bound_to_dqi is None:
        bound_to_dqi = _return_bound_key(indicator)
    source_series = pd.to_numeric(source_series, errors = 'coerce')
    indicator_score = source_series.apply(lambda x:
                                    _lookup_score_with_bound_key(x, bound_to_dqi))
    return indicator_score

def apply_dqi_to_field(df, field, indicator, bound_to_dqi=None):
    """
    Applies a data quality indicator field to a passed dataframe
    e.g. df = apply_dqi_to_field(df, 'Age','TemporalCorrelation')
    """
    if indicator in dqi_dict.keys():
        df[indicator] = apply_dqi_to_series(df[field], indicator,
                                            bound_to_dqi=bound_to_dqi)
    return df

def adjust_dqi_scores(df, source_series, indicator, bound_to_dqi=None):
    """
    Adjusts a data quality indicator field in a passed dataframe based
    on dictionary applied to a source series. The dqi score is increased
    for each value above 1 up to a maxium of 5
    """
    if bound_to_dqi is None:
        bound_to_dqi = _return_bound_key(indicator)
    source_series = pd.to_numeric(source_series, errors = 'coerce')
    if (len(source_series) != len(df)):
        print('check length')
    df[indicator] = (
        np.maximum(
            np.minimum(
                (df[indicator]
                 + apply_dqi_to_series(
                    source_series, indicator, bound_to_dqi)
                 - 1),
            5), # Sets maximum value to 5
        1)) # Sets minimum value to 1
    return df

def _lookup_score_with_bound_key(raw_score, bound_to_dqi):
    """
    Returns the score based on the passed dictionary applied to raw_score
    """
    if bound_to_dqi is None:
        return None
    else:
        breakpoints = list(bound_to_dqi.keys())
    if raw_score <= breakpoints[0]:
        score = bound_to_dqi[breakpoints[0]]
    elif (raw_score > breakpoints[0]) & (raw_score <= breakpoints[1]):
        score = bound_to_dqi[breakpoints[1]]
    elif (raw_score > breakpoints[1]) & (raw_score <= breakpoints[2]):
        score = bound_to_dqi[breakpoints[2]]
    elif (raw_score > breakpoints[2]) & (raw_score <= breakpoints[3]):
        score = bound_to_dqi[breakpoints[3]]
    else:
        score = bound_to_dqi[None]
    return score

def _return_bound_key(indicator):
    if indicator in dqi_dict.keys():
        return dqi_dict[indicator]
    return None


def get_weighted_average(df, data_col, weight_col, agg_cols):
    """
    Generates a weighted average result as a series based on passed columns
    and the dataframe prior to aggregation
    param df: Dataframe prior to aggregating from which a weighted average
        is calculated
    param data_col : str, Name of column to be averaged.
    param weight_col : str, Name of column to serve as the weighting.
    param agg_col : list, List of columns on which the dataframe is aggregated.
    returns result : series reflecting the weighted average values for the
        data_col, at length consistent with the aggregated dataframe, to be
        reapplied to the data_col in the aggregated dataframe.

    e.g.
    df_agg = df.groupby(agg_cols).agg({weight_col: ['sum']})
    df_agg[data_col] = get_weighted_average(df, data_col,
                                            weight_col, agg_cols)
    """
    df.loc[:, '_data_times_weight'] = df[data_col] * df[weight_col]
    df.loc[:, '_weight_where_notnull'] = df[weight_col] * pd.notnull(df[data_col])
    calc_cols = ['_weight_where_notnull', '_data_times_weight']
    df[calc_cols] = df[calc_cols].applymap(float)
    g = df.groupby(agg_cols)
    wt_avg = np.divide(g['_data_times_weight'].sum(), g['_weight_where_notnull'].sum(),
                       out=np.zeros_like(g['_data_times_weight'].sum()),
                       where=g['_weight_where_notnull'].sum() != 0)
    del df['_data_times_weight'], df['_weight_where_notnull']
    return wt_avg
