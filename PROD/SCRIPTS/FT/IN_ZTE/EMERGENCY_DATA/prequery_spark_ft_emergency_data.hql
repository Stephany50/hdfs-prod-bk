SELECT IF(A.FT_EXIST = 0 --and B.CONTRACT_SNAPSHOT_EXIST>0 
and C.CONTRACT_SNAPSHOT_EXIST > 0
AND T1.IT_EXISTS>0,"OK","NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_EMERGENCY_DATA WHERE TRANSACTION_DATE='###SLICE_VALUE###') A,
--(SELECT COUNT(*) CONTRACT_SNAPSHOT_EXIST FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE='###SLICE_VALUE###')B,
(SELECT COUNT(*) CONTRACT_SNAPSHOT_EXIST FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE='###SLICE_VALUE###') C,
(SELECT COUNT(*) IT_EXISTS FROM CDR.SPARK_IT_ZTE_EMERGENCY_DATA WHERE TRANSACTION_DATE="###SLICE_VALUE###" AND ORIGINAL_FILE_DATE="###SLICE_VALUE###")T1

