SELECT IF(FT_EXISTS=0 AND FT_CONTRACT_SNAP_EXISTS>0 AND T_3.NB_INSERT_DATE= 1 ,'OK','NOK' )
FROM
(SELECT COUNT(*) FT_EXISTS FROM AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY WHERE DATECODE='###SLICE_VALUE###')T1,
(SELECT COUNT(*) FT_CONTRACT_SNAP_EXISTS FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###',-1))T2,
(SELECT COUNT(distinct insert_date) NB_INSERT_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###',-1)) T_3