SIM_SQL_DICT = {
    'select_inv_qty_by_item': """
        SELECT [dtBusinessDate] AS 日付
              ,[vcShopCd] AS store_cd
              ,[vcItemCd] AS item_cd
              ,[nInvNum] AS 総在庫量
              ,[nIfInvNum] AS 販売可能在庫量
              ,[nTransportNum] AS 移送中
              ,[nBackOrderNum] AS 発注残
        FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_ShopInv]
          WHERE [dtBusinessDate] = '{tgt_date}'
            AND [vcShopCd] = '{store_cd}'
            AND [vcItemCd] = '{item_cd}'
        
    """,
    'select_ord_lot_num': """
        SELECT [dtIfBusinessDate] AS 日付
            ,'{store_cd}' AS store_cd
            ,[vcItemCd] AS item_cd
            ,[nLotNum] AS 発注単位
        FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Item]
        WHERE [dtIfBusinessDate] = '{tgt_date}'
        AND [vcItemCd] = '{item_cd}'
        
    """,
    'select_ord_delivery_timing_and_lt': """
    SELECT '{dummy_date}' AS 日付
          ,[vcSupplierCd] AS supplier_cd
          ,[vcSiteCd] as store_cd
          ,'{item_cd}' as item_cd
          ,[nSupplierLeadTime] AS 総LT
          ,[nShipmentLeadTime] AS 出荷LT
          ,[nDeliveryLeadTime] AS 納品LT
          ,CASE DATEPART(WEEKDAY, '{dummy_date}')
                WHEN 1 THEN  [nOrderDecision_Sun] 
                WHEN 2 THEN  [nOrderDecision_Mon] 
                WHEN 3 THEN  [nOrderDecision_Tue] 
                WHEN 4 THEN  [nOrderDecision_Wed] 
                WHEN 5 THEN  [nOrderDecision_Thu] 
                WHEN 6 THEN  [nOrderDecision_Fri] 
                WHEN 7 THEN  [nOrderDecision_Sat] 
            END 発注可能
          ,CASE DATEPART(WEEKDAY, '{dummy_date}')
                WHEN 1 THEN  [nDeliveryDecision_Sun] 
                WHEN 2 THEN  [nDeliveryDecision_Mon] 
                WHEN 3 THEN  [nDeliveryDecision_Tue] 
                WHEN 4 THEN  [nDeliveryDecision_Wed] 
                WHEN 5 THEN  [nDeliveryDecision_Thu] 
                WHEN 6 THEN  [nDeliveryDecision_Fri] 
                WHEN 7 THEN  [nDeliveryDecision_Sat] 
            END 納品可能
  FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Supplier]
        WHERE [dtIfBusinessDate] = '{tgt_date}'
        AND [vcSupplierCd] = '{supplier_cd}'
        AND [vcSiteCd] = '{store_cd}'
    
""",
    'select_supplier_special_holiday': """
  SELECT 
        [vcSupplierCd] AS supplier_cd
        ,[vcSiteCd] as store_cd
        ,'{item_cd}' as item_cd
        ,CONVERT(NVARCHAR, [dtNonOrderStartDate], 112) AS 発注不能開始日
        --,[dtNonOrderEndDate] AS 発注不能終了日
        ,DATEDIFF(day,dtNonOrderEndDate,dtNonOrderStartDate) AS 発注不能日数
        ,,CONVERT(NVARCHAR,[dtNonDeliveryStartDate], 112) AS 納品不能開始日
        --,[dtNonDeliveryEndDate] AS 納品不能終了日
        ,DATEDIFF(day,dtNonDeliveryEndDate,dtNonDeliveryStartDate) AS 納品不能日数
  FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_SupplierHoliday]
      WHERE [dtIfBusinessDate] = '{tgt_date}'
      AND [vcSupplierCd] = '{supplier_cd}'
      AND [vcSiteCd] = '{store_cd}'

    """,
    'select_ord_div': """
    SELECT [dtIfBusinessDate] AS 日付
        ,'{store_cd}' AS store_cd
        ,'{item_cd}' AS item_cd
        ,[nOrderDiv] AS 仕入れ先区分
    FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Definition]
    WHERE [dtIfBusinessDate] = '{tgt_date}'
    AND [vcSiteCd] = '{store_cd}'
    AND [vcItemCd] = '{item_cd}'

""",
    'select_supplier_cd': """
   SELECT  [dtIfBusinessDate] AS 日付
         ,[vcItemCd] AS item_cd
         ,[vcSiteCd] AS store_cd
         ,[nSupplierCd] AS supplier_cd
         ,'1' AS 仕入れ先区分
         --,[nSellingPrice]
     FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_ShopItem]
       WHERE [dtIfBusinessDate] = '{tgt_date}'
       AND [vcSiteCd] = '{store_cd}'
       AND [vcItemCd] = '{item_cd}'
   """,

}
