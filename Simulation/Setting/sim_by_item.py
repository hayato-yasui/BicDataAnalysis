import datetime


class SimByItemSetting(object):
    TGT_STORE_DISTRO_ITEM_CD = [['861', '094', '4901609001619'], ]
    PRDCT_DATE = datetime.date(2018, 7, 31)
    TGT_FLOOR_DATE = datetime.date(2018, 8, 1)
    TGT_UPPER_DATE = datetime.date(2018, 8, 31)
    OUTPUT_DIR = './data/OUTPUT/Simulation/' + 'ByItem/'


class PreprocessSetting(object):
    RAW_DATA_DIR = './data/Input/raw_data/'
    EXTRACT_PERIOD_FLOOR_DATE = datetime.date(2018, 7, 1)
    EXTRACT_PERIOD_UPPER_DATE = datetime.date(2018, 7, 31)

    # DATA_FILES_TO_FETCH = ['売上データ詳細_' + TGT_STORE + '_20180401-0630.csv', ]
    # DATA_FILES_TO_FETCH = ['定楽屋 金山店2018-04-01-2018-06-30_before_grouping.csv', ]
    # PROCESSED_DATA_DIR = './data/Input/processed_data/'+ TGT_STORE +'/'
