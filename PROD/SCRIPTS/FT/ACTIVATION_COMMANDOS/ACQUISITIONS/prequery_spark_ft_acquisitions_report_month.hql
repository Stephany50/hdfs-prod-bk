SELECT 
IF
(
    T_1.FT_EXIST = 0 and 
    T_2.FT_EXIST > 0 and 
    T_3.FT_EXIST > 0 and 
    T_4.FT_EXIST > 0 and 
    T_5.FT_EXIST > 0 and
    T_6.FT_EXIST > 0 and
    T_7.FT_EXIST > 0 and
    T_8.FT_EXIST > 0 and
    T_9.FT_EXIST > 0 
    ,"OK"
    ,"NOK"
) 
FROM
(SELECT COUNT(*) FT_EXIST FROM mon.spark_ft_acquisitions_month WHERE event_month = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_EXIST FROM CDR.SPARK_IT_NOMAD_CLIENT_DIRECTORY_30J where original_file_date = last_day(concat('###SLICE_VALUE###', '-01'))) T_2,
(SELECT COUNT(*) FT_EXIST FROM CDR.SPARK_IT_CONT WHERE original_file_date between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_3,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_OMNY_SDT where date_inscript between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_4,
(SELECT COUNT(*) FT_EXIST FROM mon.spark_ft_subscription where transaction_date between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_5,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_FAKE_ACTIVATION_STATUS  where transaction_date between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_6,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CONSO_SIMBOX where transaction_date between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_7,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY where transaction_date between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_8
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY where transaction_date between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_9
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_ACQ_SNAP_MONTH where transaction_date between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_10

