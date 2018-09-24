import datetime


class NormalDistributionSetting(object):
    # OUTPUT_DIR = './data/OUTPUT/' + '需要予測' + '/'
    a = 2
    p_lower_limit = 0.05
    TGT_UPPER_DATE = datetime.date(2018, 7, 31)
    TGT_FLOOR_DATE = datetime.date(2018, 7, 1)
