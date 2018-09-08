import datetime
import numpy as np


class SrcConversion(object):
    REPLACE_UNEXPECTED_VAL_TO_ALT_VAL = {'D.オーダー日時': [['1203:アルバイト１', np.nan],
                                                      ['1204:アルバイト２', np.nan]], 'D.数量': [['0:設定なし', 0]], }
    REPLACE_NAN_TO_ALT_VAL = {'D.数量': 0, 'D.価格': 0, 'C.客層': '登録なし'}
    CONVERT_DTYPE = {'D.数量': 'numeric', 'D.価格': 'numeric', 'D.オーダー日時': 'datetime', 'H.伝票発行日': 'datetime',
                     'H.伝票処理日': 'datetime', 'H.集計対象営業年月日': 'datetime', 'H.伝票金額': 'numeric',
                     'H.客数（合計）': 'numeric', 'H.客数（男）': 'numeric', 'H.客数（女）': 'numeric', }

    DIVIDE_NECESSARY_COLS = ['D.商品', 'H.店舗', 'H.テーブル番号']
    UNNECESSARY_COLS_FOR_ALL_ANALYSIS = ['親カテゴリ', 'H.集計フラグ', 'H.伝票番号枝番', 'H.元伝票番号', 'H.合算先伝票番号', 'H.支払ステータス',
                                         'H.深夜料金', 'H.テーブルチャージ料金', 'H.サービス料金', 'H.免税額（一般品）', 'H.免税額（消耗品）', 'D.免税区分',
                                         'D.免税額', 'H.値割引科目別金額（割引_*nothing*）', 'H.支払金額（ポイント）', 'H.支払金額（電子マネー）',
                                         'H.支払金額（商品券：釣り無）', 'H.支払金額（商品券：釣り有）', 'H.支払金額（その他）', 'H.レジ担当者ID',
                                         'H.スタイリスト（理美容のみ）', 'H.受付番号', 'H.テーブル合算後伝票番号', 'H.税集計区分', 'H.支払メモ', 'H.領収書発行回数',
                                         'H.領収書発行最終日付', 'H.キッチンプリンタ印字成否', 'H.キッチンプリンタ印字日付', 'H.レシートプリンタ印字成否',
                                         'H.レシートプリンタ印字日付', 'H.基幹転送成否', 'H.基幹転送日付', 'C.お客様', 'C.お客様代表', 'D.商品カテゴリ3',
                                         'D.商品カテゴリ4', 'D.商品カテゴリ5', 'D.商品印字成否', 'D.サブメニュー印字成否', 'D.優先ステータス', 'D.オーダーメモ',
                                         'D.オーダー担当者', 'D.配膳済担当者', 'D.オーダーキャンセル担当者', 'D.オーダーステータス', ]
    DEL_REC_DICT = {'H.元伝票番号': None, 'D.オーダーステータス': ['30:キャンセル済', ], }


class GroupingUnit(object):
    DAY_BILL_ORDER = ['H.集計対象営業年月日', 'H.伝票番号', 'D.オーダー日時']
    DAY_BILL = ['H.集計対象営業年月日', 'H.伝票番号']
    ITEM_CATEGORY2 = ['D.商品カテゴリ2']
    DOW = ['H.曜日', ]
    BILL = ['H.伝票番号', ]
    DOW_ITEM = ['H.曜日', 'D.商品名']
    DAY_ITEM = ['H.集計対象営業年月日','D.商品名']
    DAY_ITEM_CATEGORY2 = ['H.集計対象営業年月日','D.商品カテゴリ2']
    ITEM = ['D.商品名']
    STORE = ['H.店舗名']
    DOW_ITEM_CATEGORY2 = ['H.曜日','D.商品カテゴリ2']
    DAY = ['H.集計対象営業年月日']
    STORE_DAY = ['H.店舗名','H.集計対象営業年月日']

class MergeMasterTableSetting(object):
    # Store master
    F_PATH_STORE = './data/Input/master/store/store.csv'
    NECESSARY_COLS = ['店舗名', '都道府県', '営業開始時間', '営業締め時間', 'サービス料金掛け率', '席数', ]

    # weather master
    DIR_WEATHER = './data/Input/master/weather/'

    # calender maseter
    F_PATH_CALENDER = './data/Input/master/calender/calender.csv'

    # chanel maseter
    F_PATH_CHANEL = './data/Input/master/chanel/chanel.csv'


class ColumnGroup(object):
    ITEM = ['']
    STORE = []
    CUSTM = []
    WEATHER = ['']
