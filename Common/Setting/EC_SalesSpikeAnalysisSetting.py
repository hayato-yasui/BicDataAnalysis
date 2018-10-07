import datetime


class ECSalesSpikeAnalysisSetting(object):
    OUTPUT_DIR = './data/OUTPUT/' + 'EC_SalesSpikeAnalysis' + '/'
    TGT_FLOOR_DATE = datetime.date(2018, 1, 1)
    TGT_UPPER_DATE = datetime.date(2018, 8, 31)
    MALL_GP = {
        '自社EC': ['BicEC', 'モバイル', 'アキバソフマップ', 'ソフマップドットコム', 'コジマダイレクトＳＰ', 'コジマネット']
        , '楽天': ['ＢＩＣ楽天市場店', '楽天ビック', 'ソフマップ楽天', 'デジタルコレクション', 'コジマ楽天市場店']
        , 'Yahoo!': ['ドットコムY！SHP', 'コジマＹａｈｏｏ！店']
        , 'Wowma!': ['コジマWowma！店']
        , 'Amazon': ['ＢＩＣアマゾン店', 'ソフマップアマゾン店', 'コジマアマゾン店']
    }
    AMOUNT_DTYPE = {'ＢＩＣ楽天市場店': 'float', 'BicEC': 'float', 'モバイル': 'float', 'ＢＩＣアマゾン店': 'float',
             '楽天ビック': 'float', 'ソフマップアマゾン店': 'float', 'ソフマップ楽天': 'float', 'アキバソフマップ': 'float',
             'デジタルコレクション': 'float', 'ドットコムY！SHP': 'float', 'ソフマップドットコム': 'float',
             'コジマダイレクトＳＰ': 'float', 'コジマWowma！店': 'float', 'コジマネット': 'float',
             'コジマ楽天市場店': 'float', 'コジマＹａｈｏｏ！店': 'float', 'コジマアマゾン店': 'float', '楽天': 'float',
             '自社EC': 'float', 'Amazon': 'float', 'Yahoo!': 'float', 'Wowma!': 'float',
             'その他': 'float', '全販売': 'float'}


class PreprocessSetting(object):
    TGT_FLOOR_DATE = datetime.date(2018, 1, 1)
    TGT_UPPER_DATE = datetime.date(2018, 8, 31)
    RAW_DATA_DIR = './data/Input/raw_data/'
