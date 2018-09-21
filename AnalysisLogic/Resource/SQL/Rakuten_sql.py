RAKUTEN_SQL_DICT = {
    "select__sales_by_chanel_and_item": """
    SELECT TOP 10
        T1.[dtIfBusinessDate] as 日付,
        T1.vcLogisticsCd as store_cd,
        T3.vcOrderGPName as 発注GP,
        T3.vcDepartmentCd as 部門コード,
        T3.vcItemCategory1Name as 部門名,
        T1.vcItemCd as item_cd,
        T2.vcItemName as 商品名,
        -- T1.vcSiteCd,
        SUM(T1.nSalesNum) AS 販売数
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
            AND vcSiteCd IN ('846','253','736','089')
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
    WHERE  [dtIfBusinessDate]  = '{upper_date}'
    ) AS T3
    ON  T2.[vcDepartmentCd] = T3.[vcDepartmentCd]
    -- 自発対象のみ抽出
    INNER JOIN
    (SELECT 
            vcSiteCd
            ,vcItemCd
        FROM [AFSForBiccamera_DataStore].[dbo].[T_INF_AutoOrder]
        WHERE dtIfBusinessDate = '{upper_date}'
    ) AS T4
    ON  T1.vcLogisticsCd = T4.vcSiteCd
        AND T1.vcItemCd = T4.vcItemCd
    GROUP BY 
        T1.dtIfBusinessDate,
        T3.vcOrderGPName,
        T3.vcDepartmentCd,
        T3.vcItemCategory1Name,
        T1.vcItemCd,
        T2.vcItemName,
        T1.vcLogisticsCd
    ;
    """,

}