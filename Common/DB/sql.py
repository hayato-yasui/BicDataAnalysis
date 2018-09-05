
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
}
