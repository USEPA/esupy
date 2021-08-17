# mapping.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to facilitate flow mapping from fedelemflowlist and material flow list
"""
import pandas as pd

def apply_flow_mapping(df, source, flow_type, keep_unmapped_rows = False,
                       field_dict = None):
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
        'FlowableName',
        'FlowableUnit',
        'FlowableContext',
        'FlowableQuantity',
        'UUID'.
        If None, uses the default fields of 'Flowable','Unit','Context',
        'FlowAmount','FlowUUID'
        
    """

    if field_dict is None:
        # Default field dictionary for mapping
        field_dict = {'FlowableName':'Flowable',
                      'FlowableUnit':'Unit',
                      'FlowableContext':'Context',
                      'FlowableQuantity':'FlowAmount',
                      'UUID':'FlowUUID'}
    
    mapping_fields = ["SourceFlowName",
                      "SourceFlowContext",
                      "SourceUnit",
                      "ConversionFactor",
                      "TargetFlowName",
                      "TargetFlowContext",
                      "TargetUnit",
                      "TargetFlowUUID"]
    
    if flow_type == 'ELEMENTARY_FLOW':
        import fedelemflowlist as fedefl
        mapping = fedefl.get_flowmapping(source)
    else:
        import materialflowlist as mfl
        mapping = mfl.get_flowmapping(source)
    if len(mapping) == 0:
        return None
    
    mapping = mapping[mapping_fields]    
    mapping[['ConversionFactor']] = mapping[['ConversionFactor']].fillna(value=1)
    if keep_unmapped_rows is False:
        merge_type = 'inner'
    else:
        merge_type = 'left'
    # merge df with flows
    mapped_df = pd.merge(df, mapping,
                             left_on=[field_dict['FlowableName'],
                                      field_dict['FlowableContext'],
                                      field_dict['FlowableUnit']],
                             right_on=["SourceFlowName",
                                       "SourceFlowContext",
                                       "SourceUnit"],
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
