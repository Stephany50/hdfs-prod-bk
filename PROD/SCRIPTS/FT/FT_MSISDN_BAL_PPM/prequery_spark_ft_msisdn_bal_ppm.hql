SELECT IF(
    T_1.FT_EXISTS = 0
    AND T_2.FT_EXISTS > 1
    AND T_3.FT_EXISTS > 1
    AND T_4.FT_EXISTS > 1
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) FT_EXISTS FROM mon.spark_ft_msisdn_bal_ppm WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_EXISTS FROM mon.spark_ft_msisdn_bal_constants WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_EXISTS FROM mon.spark_ft_msisdn_bal_usage_day WHERE EVENT_DATE='###SLICE_VALUE###') T_3,
(SELECT COUNT(*) FT_EXISTS FROM mon.spark_ft_msisdn_bal_ppm WHERE EVENT_DATE=date_sub('###SLICE_VALUE###', 1)) T_4
