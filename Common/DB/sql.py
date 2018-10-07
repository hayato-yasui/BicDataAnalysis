SQL_DICT = {
    'select_ec_sales': """
    SELECT [dtIfBusinessDate] AS 日付
              ,'861'AS store_cd
              ,[vcSiteCd] AS chanel_cd
              ,'{item_cd}'AS item_cd
              ,[nSalesNum] AS 販売数
          FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_DistributionBySales]
          WHERE [dtIfBusinessDate] IN ({tgt_date})
                AND vcLogisticsCd = '861'
                AND [vcItemCd] = '{item_cd}'
                
    """,
    'select_tgt_mall_sales': """
    SELECT [dtIfBusinessDate] AS 日付
          ,'861'AS store_cd
          --,[vcSiteCd] AS chanel_cd
          ,'{item_cd}'AS item_cd
          ,sum([nSalesNum]) AS 販売数
      FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_DistributionBySales]
      WHERE [dtIfBusinessDate] IN ({tgt_date})
            AND vcLogisticsCd = '861'
            AND [vcItemCd] = '{item_cd}'
            AND [vcSiteCd] IN ({chanel_cd})
    GROUP BY
        [dtIfBusinessDate]
            

""",
    'select_ec_inv_by_item': """
     SELECT [dtBusinessDate] AS 日付
            ,[vcShopCd] AS store_cd
            ,[vcItemCd] AS item_cd
            ,[nInvNum] AS 総在庫量
            ,[nIfInvNum] AS 販売可能在庫
            ,[nTransportNum] AS 移送中
            ,[nBackOrderNum] AS 発注残
        FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_ShopInv]
        WHERE [dtBusinessDate] IN ({tgt_date})
        AND [vcShopCd] = '{store_cd}'
        AND [vcItemCd] = '{item_cd}'


""",

    'select_item_info': """
        SELECT T2.vcDepartmentCd AS dept_cd
            ,T2.vcItemCategory1Name AS 部門名
            ,T1.vcItemCd AS item_cd
            ,T1.vcItemName AS 商品名
        FROM
        (SELECT [vcDepartmentCd]
              ,[vcItemCd]
              ,[vcItemName]
              --,avg([nLotNum]) as [nLotNum]
          FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Item]
          WHERE  [dtIfBusinessDate] = '{tgt_date}' 
                AND  [vcItemCd] IN ({item_cd})
          GROUP BY [vcDepartmentCd]
              ,[vcItemCd]
              ,[vcItemName]
        ) AS T1
        INNER JOIN
        (SELECT [vcDepartmentCd]
              ,[vcItemCategory1Name]
          FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Department]
          WHERE [dtIfBusinessDate] = '{tgt_date}'
          GROUP BY [vcDepartmentCd]
              ,[vcItemCategory1Name]
        ) AS T2
        ON T1.vcDepartmentCd = T2.vcDepartmentCd

    ;
    """,
    'select_all_auto_item_using_dept': """
        SELECT T2.vcDepartmentCd AS dept_cd
            ,T2.vcItemCategory1Name AS 部門名
            ,'861' AS store_cd
            ,T1.vcItemCd AS item_cd
            ,T1.vcItemName AS 商品名
    FROM
    (SELECT [vcDepartmentCd]
          ,[vcItemCd]
          ,[vcItemName]
      FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Item]
      WHERE  [dtIfBusinessDate] = '{tgt_date}' 
      GROUP BY [vcDepartmentCd]
          ,[vcItemCd]
          ,[vcItemName]
    ) AS T1
    INNER JOIN
    (SELECT [vcDepartmentCd]
          ,[vcItemCategory1Name]
      FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Department]
      WHERE [dtIfBusinessDate] = '{tgt_date}'
            AND vcDepartmentCd IN ({dept_cd})
      GROUP BY [vcDepartmentCd]
          ,[vcItemCategory1Name]
    ) AS T2
    ON T1.vcDepartmentCd = T2.vcDepartmentCd
    INNER JOIN
    (SELECT [vcItemCd]
      FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_AutoOrder]
      WHERE [dtIfBusinessDate] = '{tgt_date}'
        AND [vcSiteCd] = '861'
    ) AS T3
    ON T1.[vcItemCd] = T3.[vcItemCd]
    ;
    """,
 'select_ec_sales_amount': """
    SELECT  T1.[vcItemCd] AS item_cd
          ,T1.[vcLogisticsCd] AS store_cd
          ,T1.vcSiteCd AS chanel_cd
          ,SUM(T1.[nSalesNum]) AS 販売量
        FROM
        (SELECT [dtIfBusinessDate]
              ,[vcSiteCd]
              ,[vcItemCd]
              ,[vcLogisticsCd]
              ,[nSalesNum]
          FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_DistributionBySales]
          WHERE [dtIfBusinessDate] BETWEEN '{floor_date}' AND '{upper_date}'
                AND vcLogisticsCd = '861'
                AND [vcItemCd] IN ({item_cd})
        )AS T1
        GROUP BY
            T1.[vcItemCd]
            ,T1.[vcLogisticsCd]
            ,T1.vcSiteCd 
        ;
    """,
 'select_auto_order_start_end_date': """
    SELECT [vcShopCd] AS store_cd
          ,[vcItemCd] as item_cd
          ,[dtSalesStartDate] AS 自発対象開始日
          ,[dtSalesEndDate] AS 自発対象終了日
      FROM [AFSForBiccamera_DataStore].[dbo].[M_MST_ShopItem_Store]
      WHERE [dtSnapshotDate] = DATEADD(day,1,'{tgt_date}')
            AND vcShopCd = '{store_cd}'
            AND vcItemCd IN ({item_cd})
        ;
    """,
    'select_ec_trun_data': """
        SELECT T1.*,T2.vcLogisticsCd,T2.nSalesNum
        FROM 
        (SELECT [dtBusinessDate]
              ,[vcSiteCd]
              ,[vcItemCd]
              ,[nSiteDiv]
              ,CASE
                WHEN [bAdjustmentFlg] = 1 THEN [nRecommendedOrderNum]
                ELSE 0
                END [nRecommendedOrderNum]
              ,[nOrgRecommendedOrderNum]
              ,[nMaxRecommendedInvNum]
              ,[nMinRecommendedInvNum]
              ,[nPreRecommendedOrderNum]
              ,[nPreMaxRecommendedInvNum]
              ,[nRequiredQuantity]
              ,[nMaxRequiredInvQuantity]
              ,[nEstimatedSalesAverage]
              ,[nAdjustedEstimatedSalesAvg]
              ,[nAdjustedEstimatedSalesSd]
              ,[nPreCoefAlpha]
              ,[nCoefAlpha]
              ,[nPreCoefBeta]
              ,[nCoefBeta]
              ,[nCoefGamma]
              ,[nCoefRho]
              ,[nCoefDelta]
              ,[nCoefF]
              ,[nCoefG]
              ,[nCoefTrend]
              ,[nShopPriorityCoef]
              ,[nOrderUnit]
              ,[nInvNum]
              ,[nMinDisplayNum]
              ,[nEstimatedSales]
              ,[nEstimatedSales_Last]
              ,[nEstimatedSales_BeforeLast]
              ,[nEstimatedSales_3WeeksBefore]
              ,[nAdjustmentNum]
              ,[nDailySalesAvgOfLast7Days]
              ,[nDailySalesForecastNumToNextOrderArrival]
              ,[nDailySalesForecastTotalToNextOrderArrival]
              ,[dtFinalDayToNextOrderArrival]
              ,[bCoefRhoFlg]
              ,[bUpdateFlg]
              ,[bAdjustmentFlg]
              ,[nBulkExecuteType]
              ,[bSentFlg]
              ,[bAutoOrderFlg]
              ,[nSupplierDiv]
              ,[vcCenterCd]
              ,[vcSupplierCd]
              ,[vcFixedPerson]
              ,[nItemCategory1Id]
              ,[nItemCategory2Id]
              ,[nItemCategory3Id]
              ,[bSupplierNonOrderReceiptFlg]
              ,[nMaxDisplayNum]
              ,[bAlertOverstockFlg]
              ,[nSellingPrice]
              ,[nCost]
              ,[nSlipCost]
          FROM [AFSForBiccamera_DataStore].[dbo].[T_CLC_RecommendedOrder]
           WHERE 1=1
                --AND vcSiteCd = '861'
                AND [dtBusinessDate] BETWEEN '{floor_date}' AND '{upper_date}'
                AND [vcItemCd] IN ({item_cd})
        ) AS T1
        INNER JOIN
        (SELECT [dtIfBusinessDate]
              ,[vcSiteCd]
              ,[vcItemCd]
              ,[vcLogisticsCd]
              ,[nSalesNum]
          FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_DistributionBySales]
          WHERE 1=1
                --AND vcSiteCd = '861'
                AND [dtIfBusinessDate] BETWEEN '{floor_date}' AND '{upper_date}'
                AND [vcItemCd] IN ({item_cd}) )AS T2
        ON T1.[dtBusinessDate] = T2.[dtIfBusinessDate]
        AND T1.[vcSiteCd] = T2.[vcSiteCd]
        AND T1.[vcItemCd] = T2.[vcItemCd]
    ;
    """,
    "select_ec_total_sales_by_chanel": """
    SELECT 
        src.vcOrderGPName AS 発注GP,
        src.vcDepartmentCd AS 部門コード,
        src.vcItemCategory1Name AS 部門名,
        src.vcLogisticsCd AS　物流コード,
        src.vcSiteCd AS 店舗コード,
        AVG(src.nSellingPrice) AS  平均売価,
        SUM(src.nSalesNum) AS 販売数,
        SUM(src.売上金額) AS 売上金額
    FROM
    (SELECT				 
        T1.[dtIfBusinessDate],
        T3.vcOrderGPName,
        T3.vcDepartmentCd,
        T3.vcItemCategory1Name,
        T1.vcItemCd,
        T2.vcItemName,
        T1.vcLogisticsCd,
        T1.vcSiteCd,
        AVG(T4.nSellingPrice) AS nSellingPrice,
        SUM(T1.nSalesNum) AS nSalesNum,
        AVG(T4.nSellingPrice) * SUM(T1.nSalesNum) AS 売上金額
    FROM 
    (SELECT [dtIfBusinessDate]
          ,[vcSiteCd]
          ,[vcItemCd]
          ,[vcLogisticsCd]
          ,[nSalesNum]
      FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_DistributionBySales]
      WHERE 1=1
            AND [dtIfBusinessDate] BETWEEN '{floor_date}' AND '{upper_date}'
            AND [vcLogisticsCd] =  '861'
    ) AS T1
    INNER JOIN
    (SELECT [vcDepartmentCd]
           ,[vcItemCd]
           ,[vcItemName]
    FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Item]
    WHERE  [dtIfBusinessDate]  = '{upper_date}'
    ) AS T2
    ON 1=1
    AND T1.[vcItemCd] = T2.[vcItemCd]
    INNER JOIN
    (SELECT [vcDepartmentCd]
           ,[vcItemCategory1Name]
           ,vcOrderGPName
    FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Department]
    WHERE  1=1
        AND [dtIfBusinessDate]  = '{upper_date}'
    ) AS T3
    ON 1=1
    AND T2.[vcDepartmentCd] = T3.[vcDepartmentCd]
    -- 自発対象のみ抽出
    INNER JOIN
    (SELECT 
            [dtIfBusinessDate]
            ,vcSiteCd
            ,vcItemCd
            ,[nSellingPrice]
        FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_ShopItem]
        WHERE dtIfBusinessDate BETWEEN  '{floor_date}' AND '{upper_date}'
    ) AS T4
    ON T1.dtIfBusinessDate = T4.dtIfBusinessDate
        AND T1.vcLogisticsCd = T4.vcSiteCd
        AND T1.vcItemCd = T4.vcItemCd
    GROUP BY 
        T1.dtIfBusinessDate,
        T3.vcOrderGPName,
        T3.vcDepartmentCd,
        T3.vcItemCategory1Name,
        T1.vcItemCd,
        T2.vcItemName,
        T1.vcLogisticsCd,
        T1.vcSiteCd
    ) AS src
    GROUP BY 
        src.vcOrderGPName,
        src.vcDepartmentCd,
        src.vcItemCategory1Name,
        src.vcLogisticsCd,
        src.vcSiteCd
    ;
    """,
 "select_jan_num_by_dept": """
    SELECT T1.部門コード
            ,count(1) AS JAN数
    FROM
    (SELECT [vcDepartmentCd] AS 部門コード
            ,vcItemCd
        FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Item]
        WHERE  [dtIfBusinessDate]  = '{upper_date}'
    ) AS T1
    INNER JOIN
    (SELECT [vcItemCd]
      FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_AutoOrder]
      WHERE [dtIfBusinessDate] BETWEEN '{floor_date}' AND '{upper_date}'
        AND [vcSiteCd] = '861'
    GROUP BY [vcItemCd]
    ) AS T2
    ON T1.[vcItemCd] = T2.[vcItemCd]
    GROUP BY 
        T1.部門コード
    ;
    """,
 'select_min_max_inv': """
    SELECT 
          [vcSiteCd] AS store_cd
          ,[vcItemCd] AS item_cd
          ,[nMinInvNum] AS 最低在庫
          ,[nMaxInvNum] AS 最高在庫
  FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_AutoOrder]
        WHERE [dtIfBusinessDate] = '{tgt_date}'
        AND [vcSiteCd] = '{store_cd}'
        AND [vcItemCd] = '{item_cd}'
    """,
 'select_shortage_day_count': """
    SELECT 
        {store_cd} AS store_cd
        ,{dept_cd} AS dept_cd
        ,T2.[vcItemCd] AS item_cd
        ,COUNT(1) AS 欠品マス目
    FROM
    (
    SELECT [vcItemCd]
        ,[vcDepartmentCd]
      FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Item]
      WHERE [dtIfBusinessDate] = '{upper_date}' 
      AND [vcDepartmentCd] = '{dept_cd}'
    ) AS T1
    INNER JOIN
    (SELECT [dtBusinessDate]
          ,[vcShopCd]
          ,[vcItemCd]
          ,[nIfInvNum]
          ,[nInvNum]
          ,[nTransportNum]
          ,[nBackOrderNum]
      FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_ShopInv]
      WHERE [dtBusinessDate] IN ({tgt_date}) 
      AND  [vcShopCd] ='{store_cd}'
      AND [nIfInvNum] <=0
      ) AS T2
      ON T1.[vcItemCd] = T2.[vcItemCd]
      GROUP BY
          T2.[vcItemCd]
    ;
    """,
 'select_inv_by_item': """
     SELECT [dtBusinessDate] AS 日付
        ,[vcShopCd] AS store_cd
        ,[vcItemCd] AS item_cd
        ,[nInvNum] AS 総在庫量
        ,[nIfInvNum] AS 販売可能在庫
        ,[nTransportNum] AS 移送中
        ,[nBackOrderNum] AS 発注残
    FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_ShopInv]
    WHERE [dtBusinessDate] = '{tgt_date}'
    AND [vcShopCd] = '{store_cd}'
    AND [vcItemCd] = '{item_cd}'
      
  """,
 'select_sales_amount_by_item_chanel': """
    SELECT [dtIfBusinessDate] AS 日付
          ,[vcSiteCd] AS chanel_cd
          ,[vcItemCd] AS item_cd
          ,[vcLogisticsCd] AS store_cd
          ,[nSalesNum] AS 販売数
      FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_DistributionBySales]
      WHERE [dtIfBusinessDate] IN ({tgt_date})
            AND [vcLogisticsCd] =  '{store_cd}'
            AND [vcItemCd] = '{item_cd}'
    ;
    """,
 'select_sales_amount_by_item': """
      SELECT '{tgt_date}' AS 日付
             ,'{store_cd}' AS store_cd
             ,[vcSiteCd] AS chanel_cd
             ,'{item_cd}'AS item_cd
             ,CASE WHEN sum([nSalesNum]) IS NULL THEN 0
               ELSE sum([nSalesNum])
              END 販売数
      FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_DistributionBySales]
      WHERE [dtIfBusinessDate] = '{tgt_date}'
      AND [vcLogisticsCd] =  '{store_cd}'
      AND [vcItemCd] = '{item_cd}'
      GROUP BY
             [dtIfBusinessDate]
             ,[vcLogisticsCd]
             ,[vcSiteCd]
      """,
    "select_ec_total_sales_by_chanel_and_item": """
    SELECT 
        src.vcOrderGPName AS 発注GP,
        src.vcDepartmentCd AS dept_cd,
        src.vcItemCategory1Name AS 部門名,
        src.vcLogisticsCd AS　chanel_cd,
        src.vcSiteCd AS store_cd,
        src.vcItemCd AS item_cd,
        src.vcItemName AS 商品名,
        AVG(src.nSellingPrice) AS  平均売価,
        SUM(src.nSalesNum) AS 販売数,
        SUM(src.売上金額) AS 売上金額
    FROM
    (SELECT				 
        T1.[dtIfBusinessDate],
        T3.vcOrderGPName,
        T3.vcDepartmentCd,
        T3.vcItemCategory1Name,
        T1.vcItemCd,
        T2.vcItemName,
        T1.vcLogisticsCd,
        T1.vcSiteCd,
        AVG(T4.nSellingPrice) AS nSellingPrice,
        SUM(T1.nSalesNum) AS nSalesNum,
        AVG(T4.nSellingPrice) * SUM(T1.nSalesNum) AS 売上金額
    FROM 
    (SELECT [dtIfBusinessDate]
          ,[vcSiteCd]
          ,[vcItemCd]
          ,[vcLogisticsCd]
          ,[nSalesNum]
      FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_DistributionBySales]
      WHERE 1=1
            AND [dtIfBusinessDate] IN ({tgt_date})
            AND [vcLogisticsCd] =  '861'
    ) AS T1
    INNER JOIN
    (SELECT [vcDepartmentCd]
           ,[vcItemCd]
           ,[vcItemName]
    FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Item]
    WHERE  [dtIfBusinessDate]  = '{upper_date}'
    ) AS T2
    ON 1=1
    AND T1.[vcItemCd] = T2.[vcItemCd]
    INNER JOIN
    (SELECT [vcDepartmentCd]
           ,[vcItemCategory1Name]
           ,vcOrderGPName
    FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Department]
    WHERE  1=1
        AND [dtIfBusinessDate]  = '{upper_date}'
    ) AS T3
    ON 1=1
    AND T2.[vcDepartmentCd] = T3.[vcDepartmentCd]
    -- 自発対象のみ抽出
    INNER JOIN
    (SELECT 
            [dtIfBusinessDate]
            ,vcSiteCd
            ,vcItemCd
            ,[nSellingPrice]
        FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_ShopItem]
        WHERE dtIfBusinessDate IN ({tgt_date})
    ) AS T4
    ON T1.dtIfBusinessDate = T4.dtIfBusinessDate
        AND T1.vcLogisticsCd = T4.vcSiteCd
        AND T1.vcItemCd = T4.vcItemCd
    GROUP BY 
        T1.dtIfBusinessDate,
        T3.vcOrderGPName,
        T3.vcDepartmentCd,
        T3.vcItemCategory1Name,
        T1.vcItemCd,
        T2.vcItemName,
        T1.vcLogisticsCd,
        T1.vcSiteCd
    ) AS src
    GROUP BY 
        src.vcOrderGPName,
        src.vcDepartmentCd,
        src.vcItemCategory1Name,
        src.vcLogisticsCd,
        src.vcSiteCd,
        src.vcItemCd,
        src.vcItemName
    ;
    """,
    "select_ec_total_sales_qty_by_chanel_and_item": """
        SELECT 
            T1.dtIfBusinessDate AS 日付,
            T3.vcOrderGPName AS 発注GP,
            T3.vcDepartmentCd AS dept_cd,
            T3.vcItemCategory1Name AS 部門名,
            T1.vcLogisticsCd AS store_cd,
            T1.vcSiteCd AS chanel_cd,
            T1.vcItemCd AS item_cd,
            T2.vcItemName AS 商品名,
            T1.nSalesNum AS 販売数
        FROM 
        (SELECT [dtIfBusinessDate]
              ,[vcSiteCd]
              ,[vcItemCd]
              ,[vcLogisticsCd]
              ,[nSalesNum]
          FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_DistributionBySales]
          WHERE 1=1
                AND [dtIfBusinessDate] IN ({tgt_date})
                AND [vcLogisticsCd] =  '861'
        ) AS T1
        INNER JOIN
        (SELECT [vcDepartmentCd]
               ,[vcItemCd]
               ,[vcItemName]
        FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Item]
        WHERE  [dtIfBusinessDate]  = '{upper_date}'
        ) AS T2
        ON T1.[vcItemCd] = T2.[vcItemCd]
        INNER JOIN
        (SELECT [vcDepartmentCd]
               ,[vcItemCategory1Name]
               ,vcOrderGPName
        FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Department]
        WHERE [dtIfBusinessDate]  = '{upper_date}'
        ) AS T3
        ON  T2.[vcDepartmentCd] = T3.[vcDepartmentCd]
        -- 自発対象のみ抽出
        INNER JOIN
        (SELECT 
                --[dtIfBusinessDate],
                vcSiteCd
                ,vcItemCd
                --,[nSellingPrice]
            FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_ShopItem]
            WHERE dtIfBusinessDate = '{upper_date}'
        ) AS T4
        ON --T1.dtIfBusinessDate = T4.dtIfBusinessDate AND
            T1.vcLogisticsCd = T4.vcSiteCd
            AND T1.vcItemCd = T4.vcItemCd
        ;
        """,

    'select_price_by_item': """
        SELECT [dtIfBusinessDate] AS 日付
          ,'{item_cd}' AS item_cd
          ,'{store_cd}' AS store_cd
          --,[nSupplierCd]
          ,[nSellingPrice] AS 売価 -- 当日末時点の売価
      FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_ShopItem]AS t
      WHERE dtIfBusinessDate IN ({tgt_date})
        AND vcSiteCd = '{store_cd}'
        AND vcItemCd = '{item_cd}'
        
    """,
    'select_season_weekend_factor': """
    SELECT  [dtBusinessDate] AS 日付
        ,'{dept_cd}' AS dept_cd
        ,'{store_cd}' AS store_cd
        ,MIN([nFactor]) AS 季節休日係数
    FROM [AFSForBiccamera_DataStore].[dbo].[M_CLC_SeasonWeekendFactor_Source_Store]
    WHERE  [dtSnapshotDate] IN ({dummy_date})
        AND [vcDepartmentCd] = '{dept_cd}'
        AND [vcSiteCd] = '{store_cd}'
        AND [dtBusinessDate] ='{tgt_date}'
    GROUP BY  [dtBusinessDate] 

    """,
    'select_daily_sales_qty_by_chanel': """
    SELECT
        *
    FROM
        [EC調査].[dbo].[日別JAN別モール別販売数]
    WHERE
        日付 BETWEEN '2018-8-1' AND '2018-8-31'
    ;
    """,

}