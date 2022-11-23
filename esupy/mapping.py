# mapping.py (esupy)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to facilitate flow mapping from fedelemflowlist and material flow list
"""
import pandas as pd
import logging as log


def apply_flow_mapping(df, source, flow_type, keep_unmapped_rows=False,
                       field_dict=None, ignore_source_name=False, **_):
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
        field_dict = {'SourceName': 'SourceName',
                      'FlowableName': 'Flowable',
                      'FlowableUnit': 'Unit',
                      'FlowableContext': 'Context',
                      'FlowableQuantity': 'FlowAmount',
                      'UUID': 'FlowUUID'}

    mapping_fields = ["SourceListName",
                      "SourceFlowName",
                      "SourceFlowContext",
                      "SourceUnit",
                      "ConversionFactor",
                      "TargetFlowName",
                      "TargetFlowContext",
                      "TargetUnit",
                      "TargetFlowUUID"]

    # load mapping file if specified in the method yaml
    mapping_file = _.get('material_crosswalk')
    if mapping_file is not None:
        mapping = pd.read_csv(mapping_file)
    else:
        if flow_type == 'ELEMENTARY_FLOW':
            try:
                import fedelemflowlist as fedefl
                mapping = fedefl.get_flowmapping(source)
            except ImportError:
                log.warning('Error importing fedelemflowlist, install '
                            'fedelemflowlist to apply flow mapping to elementary '
                            'flows: https://github.com/USEPA/Federal-LCA-Commons-'
                            'Elementary-Flow-List/wiki/GitHub-Contributors#install'
                            '-for-users')
                return None
        else:
            try:
                import materialflowlist as mfl
                mapping = mfl.get_flowmapping(source)
            except ImportError:
                log.warning('Error importing materialflowlist, install '
                            'materialflowlist to apply flow mapping to waste and '
                            'technosphere flows: '
                            'https://github.com/USEPA/materialflowlist/wiki')
                return None
    if len(mapping) == 0:
        # mapping not found
        return None

    mapping = mapping[mapping_fields]
    mapping[['ConversionFactor']] = mapping[['ConversionFactor']].fillna(
        value=1)
    if keep_unmapped_rows is False:
        merge_type = 'inner'
    else:
        merge_type = 'left'

    map_to = [field_dict.get('SourceName'),
              field_dict.get('FlowableName'),
              field_dict.get('FlowableContext'),
              field_dict.get('FlowableUnit')]

    map_from = ["SourceListName",
                "SourceFlowName",
                "SourceFlowContext",
                "SourceUnit"]

    if ignore_source_name:
        map_to.remove(field_dict['SourceName'])
        map_from.remove('SourceListName')

    del_list = []
    for i in range(len(map_to)):
        if map_to[i] is not None:
            df[map_to[i]].fillna('', inplace=True)
            mapping[map_from[i]].fillna('', inplace=True)
        else:
            del_list.append(i)
    del_list.reverse()
    for x in del_list:
        del map_to[x]
        del map_from[x]

    # merge df with flows
    mapped_df = pd.merge(df, mapping,
                             left_on=map_to,
                             right_on=map_from,
                             how=merge_type)

    criteria = mapped_df['TargetFlowName'].notnull()

    replacement_dict = {'FlowableName': 'TargetFlowName',
                        'FlowableContext': 'TargetFlowContext',
                        'FlowableUnit': 'TargetUnit',
                        'UUID': 'TargetFlowUUID'}

    for k, v in replacement_dict.items():
        try:
            mapped_df.loc[criteria, field_dict[k]] = mapped_df[v]
        except KeyError:
            pass # Not mapping on that field
    mapped_df.loc[criteria, field_dict["FlowableQuantity"]] = \
        mapped_df[field_dict["FlowableQuantity"]] * mapped_df["ConversionFactor"]

    # drop mapping fields
    mapped_df = mapped_df.drop(columns=mapping_fields)

    return mapped_df
