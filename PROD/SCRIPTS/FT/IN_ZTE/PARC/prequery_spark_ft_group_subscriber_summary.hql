SELECT IF(FT_EXISTS=0 AND FT_CONTRACT_SNAP_EXISTS>0 AND FT_ACCOUNT_ACT_EXISTS>0,'OK','NOK' )
FROM
(SELECT COUNT(*) FT_EXISTS FROM MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY WHERE EVENT_DATE='###SLICE_VALUE###')T1,
(SELECT COUNT(*) FT_CONTRACT_SNAP_EXISTS FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE='###SLICE_VALUE###')T2,
(SELECT COUNT(*) FT_ACCOUNT_ACT_EXISTS FROM MON.SPARK_FT_ACCOUNT_ACTIVITY WHERE EVENT_DATE='###SLICE_VALUE###')T3