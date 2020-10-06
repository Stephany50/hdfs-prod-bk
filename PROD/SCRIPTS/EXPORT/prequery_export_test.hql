SELECT IF(
T_1.FT_EXIST > 0 AND
T_2.NB_EXPORT < 1
,"OK","NOK")
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.spark_ft_billed_transaction_prepaid WHERE transaction_date ='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) NB_EXPORT ) T_2