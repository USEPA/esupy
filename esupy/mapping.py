# mapping.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to facilitate flow mapping from fedelemflowlist and material flow list
"""
import pandas as pd
import logging as log

def apply_flow_mapping(df, source, flow_type, keep_unmapped_rows = False,
                       field_dict = None, ignore_source_name = False):
    """
    Maps a dataframe using a flow mapping file from fedelemflowlist or
    materialflowlist.
    
    :param df: dataframe to be mapped
    :param source: list or str, name of mapping file(s)
    :param flow_type: str either 'ELEMENTARY_FLOW', 'TECHNOSPHERE_FLOW',
        or 'WASTE_FLOW'
    :param keep_unmaped_rows: bool, False if want unmapped rows
        dropped, True if want to retain
    :param field_dict: dictionary of field names in df containing the following keys:
        'SourceName',
        'FlowableName',
        'FlowableUnit',
        'FlowableContext',
        'FlowableQuantity',
        'UUID'.
        If None, uses the default fields of 'SourceName','Flowable',
        'Unit','Context','FlowAmount','FlowUUID'
    :param ignore_source_name: bool, False if flows should be mapped based on
        SourceName. (E.g., should be False when mapping across multiple datasets)
        
    """

    if field_dict is None:
        # Default field dictionary for mapping
        field_dict = {'SourceName':'SourceName',
                      'FlowableName':'Flowable',
                      'FlowableUnit':'Unit',
                      'FlowableContext':'Context',
                      'FlowableQuantity':'FlowAmount',
                      'UUID':'FlowUUID'}
    
    mapping_fields = ["SourceListName",
                      "SourceFlowName",
                      "SourceFlowContext",
                      "SourceUnit",
                      "ConversionFactor",
                      "TargetFlowName",
                      "TargetFlowContext",
                      "TargetUnit",
                      "TargetFlowUUID"]
    
    if flow_type == 'ELEMENTARY_FLOW':
        try:
            import fedelemflowlist as fedefl
            mapping = fedefl.get_flowmapping(source)
        except ImportError:
            log.warning('Error importing fedelemflowlist, install fedelemflowlist '
                        'to apply flow mapping to elementary flows: '
                        'https://github.com/USEPA/Federal-LCA-Commons-Elementary-Flow-List/wiki/GitHub-Contributors#install-for-users')
            return None
    else:
        try:
            import materialflowlist as mfl
            mapping = mfl.get_flowmapping(source)
        except ImportError:
            log.warning('Error importing materialflowlist, install materialflowlist '
                        'to apply flow mapping to waste and technosphere flows: '
                        'https://github.com/USEPA/materialflowlist/wiki')
            return None
    if len(mapping) == 0:
        # mapping not found
        return None
    
    mapping = mapping[mapping_fields]    
    mapping[['ConversionFactor']] = mapping[['ConversionFactor']].fillna(value=1)
    if keep_unmapped_rows is False:
        merge_type = 'inner'
    else:
        merge_type = 'left'

    map_to = [field_dict['SourceName'],
              field_dict['FlowableName'],
              field_dict['FlowableContext'],
              field_dict['FlowableUnit']]
    
    map_from = ["SourceListName",
                "SourceFlowName",
                "SourceFlowContext",
                "SourceUnit"]
    
    if ignore_source_name:
        map_to.remove(field_dict['SourceName'])
        map_from.remove('SourceListName')
    
    for field in map_to:
        df[field].fillna('', inplace=True)
    for field in map_from:
        mapping[field].fillna('', inplace=True)

    # merge df with flows    
    mapped_df = pd.merge(df, mapping,
                             left_on=map_to,
                             right_on=map_from,
                             how=merge_type)
    
    criteria = mapped_df['TargetFlowName'].notnull()
    
    mapped_df.loc[criteria, field_dict['FlowableName']] = mapped_df["TargetFlowName"]
    mapped_df.loc[criteria, field_dict['FlowableContext']] = mapped_df["TargetFlowContext"]
    mapped_df.loc[criteria, field_dict['FlowableUnit']] = mapped_df["TargetUnit"]
    mapped_df.loc[criteria, field_dict["FlowableQuantity"]] = \
        mapped_df[field_dict["FlowableQuantity"]] * mapped_df["ConversionFactor"]
    mapped_df.loc[criteria, field_dict['UUID']] = mapped_df["TargetFlowUUID"]

    # drop mapping fields
    mapped_df = mapped_df.drop(columns=mapping_fields)

    return mapped_df
