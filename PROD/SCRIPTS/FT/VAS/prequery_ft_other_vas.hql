SELECT IF(A.COUNT_FT_OTHER_VAS = 0 and B.COUNT_IT_P2P_LOG>0 and C.COUNT_FT_CONTRACT_SNAPSHOT>0,"OK","NOK") FROM
(SELECT count(*) COUNT_FT_OTHER_VAS FROM MON.FT_OTHER_VAS WHERE TRANSACTION_DATE='###SLICE_VALUE###' LIMIT 10) A,
(SELECT COUNT(*) COUNT_IT_P2P_LOG FROM CDR.SPARK_IT_P2P_LOG WHERE START_DATE='###SLICE_VALUE###' LIMIT 10) B,
(SELECT COUNT(*) COUNT_FT_CONTRACT_SNAPSHOT FROM MON.SPARK_FT_CONTRACT_Snapshot WHERE EVENT_DATE='###SLICE_VALUE###' LIMIT 10) C