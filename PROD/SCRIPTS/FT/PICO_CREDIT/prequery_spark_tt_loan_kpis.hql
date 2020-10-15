SELECT IF(TT_NOT_EXISTS = 0 and EQT_EXISTS>0 ,"OK","NOK") TABLES_EXIST
FROM
(SELECT COUNT(*) TT_NOT_EXISTS FROM MON.SPARK_TT_PICO_KPIS WHERE event_month='###SLICE_VALUE###' and KPI='TELCO_LOANS_DELAY') A,
(SELECT COUNT(*) EQT_EXISTS FROM MON.SPARK_FT_EDR_PRPD_EQT WHERE EVENT_DATE='###SLICE_VALUE###-01') B