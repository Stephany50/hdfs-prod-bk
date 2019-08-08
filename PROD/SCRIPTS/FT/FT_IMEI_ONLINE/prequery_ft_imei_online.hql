SELECT IF(T_1.FT_EXIST = 0 and T_2.Hold_FT_MSC_TRANSACTION>0 and T_3.Hold_FT_OTARIE_DATA_TRAFFIC_DAY>0 and T_4.Hold_FT_CRA_GPRS>0 and T_4.Hold_FT_CRA_GPRS_POST>0 ,"OK","NOK") FT_IMEI_ONLINE_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.FT_IMEI_ONLINE WHERE SDATE = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) Hold_FT_MSC_TRANSACTION FROM MON.FT_MSC_TRANSACTION WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_2,
(SELECT COUNT(*) Hold_FT_OTARIE_DATA_TRAFFIC_DAY FROM MON.FT_OTARIE_DATA_TRAFFIC_DAY WHERE TRANSACTION_DATE = DATE_SUB('###SLICE_VALUE###',1)) T_3,
(SELECT COUNT(*) Hold_FT_CRA_GPRS FROM MON.FT_CRA_GPRS WHERE SESSION_DATE = '###SLICE_VALUE###') T_4,
(SELECT COUNT(*) Hold_FT_CRA_GPRS_POST FROM MON.FT_CRA_GPRS_POST WHERE SESSION_DATE = '###SLICE_VALUE###') T_5