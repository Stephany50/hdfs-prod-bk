SELECT IF(T1.KPI_IS_LOAD=0 AND dist>0 ,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND KPI IN ('PDV_OM_ACTIF_30Jrs'))T1,
(SELECT COUNT(*) dist FROM mon.SPARK_DATAMART_OM_DISTRIB WHERE jour ='###SLICE_VALUE###')T2