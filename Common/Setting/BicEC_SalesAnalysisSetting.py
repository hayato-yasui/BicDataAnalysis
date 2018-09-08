import datetime


class BicECSalesAnalysisSetting(object):
    OUTPUT_DIR = './data/OUTPUT/' + 'BicEC_SalesAnalysis' + '/'


class PreprocessSetting(object):
    RAW_DATA_DIR = './data/Input/raw_data/'
    # DATA_FILES_TO_FETCH = ['売上データ詳細_' + TGT_STORE + '_20180401-0630.csv', ]
    # DATA_FILES_TO_FETCH = ['定楽屋 金山店2018-04-01-2018-06-30_before_grouping.csv', ]
    # PROCESSED_DATA_DIR = './data/Input/processed_data/'+ TGT_STORE +'/'
