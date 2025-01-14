SELECT IF(
            T1.KPI_IS_LOAD=0 AND
            T2.KPI_EXISTS>0 AND
            T3.KPI_EXISTS>0 AND
            T4.KPI_EXISTS>0,
        'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_A_REPORTING_INTERCO WHERE PROCESSING_DATE='###SLICE_VALUE###')T1,
(SELECT COUNT(*) KPI_EXISTS FROM AGG.SPARK_FT_X_INTERCO_FINAL  WHERE SDATE='###SLICE_VALUE###')T2,
(SELECT COUNT(*) KPI_EXISTS FROM AGG.SPARK_FT_QOS_SMSC_SPECIAL_NUMBER  WHERE STATE_DATE='###SLICE_VALUE###')T3,
(SELECT COUNT(*) KPI_EXISTS FROM AGG.SPARK_FT_A_SUBS_SPHERE_TRAFFIC_DAY  WHERE EVENT_DATE='###SLICE_VALUE###')T4