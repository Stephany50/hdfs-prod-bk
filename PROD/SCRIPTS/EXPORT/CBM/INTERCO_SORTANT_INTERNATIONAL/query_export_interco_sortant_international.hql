SELECT 
    MSISDN,
    DESTINATION,
    MOU_INTER,
    TRANSACTION_DATE
FROM MON.SPARK_FT_INTERCO_SORTANT_INTERNATIONAL
WHERE TRANSACTION_DATE='###SLICE_VALUE###'