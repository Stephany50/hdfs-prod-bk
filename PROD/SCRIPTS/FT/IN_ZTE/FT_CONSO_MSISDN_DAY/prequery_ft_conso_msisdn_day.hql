SELECT IF(T_1.FT_COUNT = 0 AND T_2.FT_BILLED_TRANSACTION_PREPAID > 1,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) FT_COUNT FROM MON.FT_CONSO_MSISDN_DAY WHERE EVENT_DATE="###SLICE_VALUE###") T_1,
(SELECT COUNT(*) FT_BILLED_TRANSACTION_PREPAID FROM MON.FT_BILLED_TRANSACTION_PREPAID WHERE TRANSACTION_DATE="###SLICE_VALUE###") T_2