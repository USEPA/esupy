# dqi.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions for processing and reporting life cycle data quality indicators
"""

import pandas as pd


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

