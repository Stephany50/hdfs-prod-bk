SELECT IF(T_1.FT_EXIST = 0 and T_2.Hold_FT_MSC_TRANSACTION>0 and T_3.Hold_FT_snapshot>0 ,"OK","NOK") FT_IMEI_ONLINE_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CELL_TRAFIC_DAYLY WHERE EVENT_DATE = '###SLICE_VALUE###'  ) T_1,
(SELECT COUNT(*) Hold_FT_MSC_TRANSACTION FROM MON.SPARK_FT_MSC_TRANSACTION WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_2,
(SELECT COUNT(*) Hold_FT_snapshot FROM MON.SPARK_ft_contract_snapshot WHERE event_date = '###SLICE_VALUE###') T_3