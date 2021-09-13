SELECT IF(
            T1.KPI_IS_LOAD=0 AND
            T2.KPI_EXISTS>0,
        'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_A_REPORTING_INTERCO_INTERNATIONAL WHERE PROCESSING_DATE='###SLICE_VALUE###')T1,
(SELECT COUNT(*) KPI_EXISTS FROM MON.SPARK_FT_MSC_TRANSACTION  WHERE TRANSACTION_DATE='###SLICE_VALUE###')T2