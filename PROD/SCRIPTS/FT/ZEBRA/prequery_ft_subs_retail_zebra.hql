SELECT IF(A.COUNT_FT = 0 and B.COUNT_IT_ZEBRA>100 and C.COUNT_FT_SUBSCRIPTION>100,"OK","NOK") FROM
(SELECT count(*) COUNT_FT FROM MON.FT_SUBS_RETAIL_ZEBRA WHERE TRANSACTION_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) COUNT_IT_ZEBRA FROM CDR.IT_ZEBRA_TRANSAC WHERE TRANSFER_DATE='###SLICE_VALUE###')B,
(SELECT COUNT(*) COUNT_FT_SUBSCRIPTION FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE='###SLICE_VALUE###')C

