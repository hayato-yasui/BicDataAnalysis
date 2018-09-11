import datetime


class SimByItemSetting(object):
    PRDCT_DATE = datetime.date(2018, 7, 31)
    TGT_FLOOR_DATE = datetime.date(2018, 8, 1)
    TGT_UPPER_DATE = datetime.date(2018, 8, 31)
    OUTPUT_DIR = './data/OUTPUT/Simulation/' + 'ByItem/'


class PreprocessSetting(object):
    TGT_ITEM_FILE_DIR = './data/Input/tgt_itm_cd/tgt_item_by_item.csv'
    RAW_DATA_DIR = './data/Input/raw_data/'
    EXTRACT_PERIOD_FLOOR_DATE = datetime.date(2018, 7, 1)
    EXTRACT_PERIOD_UPPER_DATE = datetime.date(2018, 7, 31)
    USE_CALCED_SEASON_WEEKEND_FCTR = True
    USE_CALCED_SALES_AVG = True
    USE_CALCED_SALES_STD = True
    USE_CALCED_SAFTY_INV = True
    USE_CALCED_MIN_INV = True
    USE_CALCED_MAX_INV = True


    # DATA_FILES_TO_FETCH = ['売上データ詳細_' + TGT_STORE + '_20180401-0630.csv', ]
    # DATA_FILES_TO_FETCH = ['定楽屋 金山店2018-04-01-2018-06-30_before_grouping.csv', ]
    # PROCESSED_DATA_DIR = './data/Input/processed_data/'+ TGT_STORE +'/'
