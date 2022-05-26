"""Test functions"""

import esupy.processed_data_mgmt as es_dt


def test_data_commons_access():
    """Confirms succesful access to and download from data commons files"""
    path = es_dt.Paths()
    path.local_path = path.local_path + '/flowsa'
    meta = es_dt.FileMeta()
    meta.tool = 'flowsa'
    meta.category = 'FlowByActivity'
    meta.name_data = 'USDA_CoA_Cropland_2017'
    meta.ext = 'parquet'

    resp1 = es_dt.download_from_remote(meta, path)
    df1 = es_dt.load_preprocessed_output(meta, path)

    meta.name_data = 'NOT_A_FILE'
    resp2 = es_dt.download_from_remote(meta, path)
    df2 = es_dt.load_preprocessed_output(meta, path)

    assert(df1 is not None and df2 is None)
