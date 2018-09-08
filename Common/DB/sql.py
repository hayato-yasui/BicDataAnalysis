
SQL_DICT = {
    'select_item_info': """
        SELECT T2.vcDepartmentCd
            ,T2.vcItemCategory1Name
            ,T1.vcItemCd
            ,T1.vcItemName
        FROM
        (SELECT [vcDepartmentCd]
              ,[vcItemCd]
              ,[vcItemName]
              --,avg([nLotNum]) as [nLotNum]
          FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Item]
          WHERE  [dtIfBusinessDate] BETWEEN '{floor_date}' AND '{upper_date}' 
                AND  [vcItemCd] IN ({item_cd})
          GROUP BY [vcDepartmentCd]
              ,[vcItemCd]
              ,[vcItemName]
        ) AS T1
        INNER JOIN
        (SELECT [vcDepartmentCd]
              ,[vcItemCategory1Name]
          FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_Department]
          WHERE [dtIfBusinessDate] in ({tgt_date})
          GROUP BY [vcDepartmentCd]
              ,[vcItemCategory1Name]
        ) AS T2
        ON T1.vcDepartmentCd = T2.vcDepartmentCd

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
    LEFT OUTER JOIN
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
}
