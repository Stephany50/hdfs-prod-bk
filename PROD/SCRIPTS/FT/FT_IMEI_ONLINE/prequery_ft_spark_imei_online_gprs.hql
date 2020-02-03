SELECT IF(T_1.FT_EXIST = 0 and T_2.Hold_FT_MSC_TRANSACTION>0 and T_3.Hold_FT_OTARIE_DATA_TRAFFIC_DAY>0 and T_4.Hold_FT_CRA_GPRS>0 and T_5.Hold_FT_CRA_GPRS_POST>0 ,"OK","NOK") FT_IMEI_ONLINE_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_IMEI_ONLINE WHERE SDATE = '###SLICE_VALUE###' AND SRC_TABLE = 'GPRS_POST|'  ) T_1,
(SELECT COUNT(*) Hold_FT_CRA_GPRS_POST FROM MON.SPARK_FT_CRA_GPRS_POST WHERE SESSION_DATE = '###SLICE_VALUE###') T_2
