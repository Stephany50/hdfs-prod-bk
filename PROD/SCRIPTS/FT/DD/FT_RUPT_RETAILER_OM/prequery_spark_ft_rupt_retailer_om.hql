SELECT IF(A.NB_EVENT_TIME<B.NB_EVENT_TIME AND LOC.NBR > 0 AND C.CDR_EXISTS > 0 , "OK", "NOK")
FROM
(SELECT COUNT(DISTINCT EVENT_TIME) NB_EVENT_TIME FROM DD.SPARK_FT_RUPT_RETAILER_OM WHERE EVENT_DATE = '###SLICE_VALUE###') A,
(SELECT COUNT(DISTINCT FILE_TIME) NB_EVENT_TIME FROM CDR.SPARK_IT_HIERARCHICAL_VIEW_OF_BALANCE WHERE FILE_DATE = '###SLICE_VALUE###') B,
(SELECT COUNT(*) NBR FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###', 1)) LOC,
(SELECT COUNT(*) CDR_EXISTS FROM CDR.SPARK_IT_OM_ALL_USERS WHERE ORIGINAL_FILE_DATE =DATE_SUB('###SLICE_VALUE###', 1)) C