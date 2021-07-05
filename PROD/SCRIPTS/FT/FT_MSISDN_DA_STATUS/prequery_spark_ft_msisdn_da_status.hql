SELECT IF(
    T_1.FT_EXISTS = 0
    AND T_2.IT_EXISTS > 1
    AND T_3.IT_EXISTS > 1
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) FT_EXISTS FROM mon.spark_ft_msisdn_da_status WHERE EVENT_DATE=date_sub('###SLICE_VALUE###', 1)) T_1,
(SELECT COUNT(*) IT_EXISTS FROM cdr.spark_IT_ZTE_BAL_EXTRACT WHERE original_file_date='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) IT_EXISTS FROM CDR.spark_IT_ZTE_SUBS_EXTRACT WHERE original_file_date='###SLICE_VALUE###') T_3
