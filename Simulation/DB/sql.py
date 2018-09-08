SQL_DICT = {
    'select_inv_qty_by_item': """
        SELECT [dtIfBusinessDate]
              ,[vcSiteCd]
              ,[vcItemCd]
              ,[nInvNum]
              ,[nOpenInvNum]
              ,[nNonSaleInvNum]
              ,[nInvPrice]
          FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Inv]
          WHERE [dtIfBusinessDate] BETWEEN '{floor_date}' AND '{upper_date}'
            AND vcSiteCd = '{store_cd}'
            AND [vcItemCd] = '{item_cd}'
        ;
    """,
    'select_prdct_date_inv_qty_by_item': """
        SELECT [dtBusinessDate]
              ,[vcItemCd]
              ,[nInvNum] AS 総在庫量
              ,[nInvNum] AS 販売可能在庫量
              ,[nTransportNum] AS 移送中
              ,[nBackOrderNum] AS 発注残
        FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_ShopInv]
          WHERE [dtBusinessDate] = '{prdct_date}'
            AND [vcShopCd] = '{store_cd}'
            AND [vcItemCd] = '{item_cd}'
        ;
    """,
    "select_lot_num": """
        SELECT [dtIfBusinessDate]
          ,[vcItemCd]
          ,[nLotNum] AS 発注単位
        FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Item]
        WHERE [dtIfBusinessDate] = '{tgt_date}'
        AND [vcItemCd] = '{item_cd}'
    """,
    'select_ord_delivery_timing_and_lt': """
    SELECT [dtBusinessDate]
          ,[vcItemCd]
          ,[nInvNum] AS 総在庫量
          ,[nInvNum] AS 販売可能在庫量
          ,[nTransportNum] AS 移送中
          ,[nBackOrderNum] AS 発注残
    FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_ShopInv]
      WHERE [dtBusinessDate] = '{prdct_date}'
        AND [vcShopCd] = '{store_cd}'
        AND [vcItemCd] = '{item_cd}'
    ;
""",
}
