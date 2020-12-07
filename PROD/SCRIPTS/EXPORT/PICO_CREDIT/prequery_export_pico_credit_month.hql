SELECT IF( 
    T_1_0.KPI_EXIST_0 > 0 AND 
    T_2_0.KPI_EXIST_0 > 0 AND 
    T_3_0.KPI_EXIST_0 > 0 AND 
    T_4_0.KPI_EXIST_0 > 0 AND 
    T_5_0.KPI_EXIST_0 > 0 AND 
    T_1_1.KPI_EXIST_1 > 0 AND 
    T_2_1.KPI_EXIST_1 > 0 AND 
    T_3_1.KPI_EXIST_1 > 0 AND 
    T_4_1.KPI_EXIST_1 > 0 AND 
    T_5_1.KPI_EXIST_1 > 0 AND 
    T_1_2.KPI_EXIST_2 > 0 AND 
    T_2_2.KPI_EXIST_2 > 0 AND 
    T_3_2.KPI_EXIST_2 > 0 AND 
    T_4_2.KPI_EXIST_2 > 0 AND 
    T_5_2.KPI_EXIST_2 > 0 AND 
    T_1_3.KPI_EXIST_3 > 0 AND 
    T_2_3.KPI_EXIST_3 > 0 AND 
    T_3_3.KPI_EXIST_3 > 0 AND 
    T_4_3.KPI_EXIST_3 > 0 AND 
    T_5_3.KPI_EXIST_3 > 0 AND 
    T_1_4.KPI_EXIST_4 > 0 AND 
    T_2_4.KPI_EXIST_4 > 0 AND 
    T_3_4.KPI_EXIST_4 > 0 AND 
    T_4_4.KPI_EXIST_4 > 0 AND 
    T_5_4.KPI_EXIST_4 > 0 AND 
    T_1_5.KPI_EXIST_5 > 0 AND 
    T_2_5.KPI_EXIST_5 > 0 AND 
    T_3_5.KPI_EXIST_5 > 0 AND 
    T_4_5.KPI_EXIST_5 > 0 AND 
    T_5_5.KPI_EXIST_5 > 0 AND 
    T_6.NB_EXPORT < 1 ,"OK","NOK")
FROM
(SELECT COUNT(*) KPI_EXIST_0 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH ='###SLICE_VALUE###' and KPI='TELCO_LOANS_DELAY') T_1_0,
(SELECT COUNT(*) KPI_EXIST_0 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH ='###SLICE_VALUE###' and KPI='ATC_LOANS_AMT') T_2_0,
(SELECT COUNT(*) KPI_EXIST_0 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH ='###SLICE_VALUE###' and KPI='OM_DEPOSIT_AMT') T_3_0,
(SELECT COUNT(*) KPI_EXIST_0 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH ='###SLICE_VALUE###' and KPI='CALLS_ON_N') T_4_0,
(SELECT COUNT(*) KPI_EXIST_0 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH ='###SLICE_VALUE###' and KPI='OM_BANK_TRF_AMT') T_5_0,
(SELECT COUNT(*) KPI_EXIST_1 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -1), 0, 7) and KPI='TELCO_LOANS_DELAY') T_1_1,
(SELECT COUNT(*) KPI_EXIST_1 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -1), 0, 7) and KPI='ATC_LOANS_AMT') T_2_1,
(SELECT COUNT(*) KPI_EXIST_1 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -1), 0, 7) and KPI='OM_DEPOSIT_AMT') T_3_1,
(SELECT COUNT(*) KPI_EXIST_1 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -1), 0, 7) and KPI='CALLS_ON_N') T_4_1,
(SELECT COUNT(*) KPI_EXIST_1 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -1), 0, 7) and KPI='OM_BANK_TRF_AMT') T_5_1,
(SELECT COUNT(*) KPI_EXIST_2 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -2), 0, 7) and KPI='TELCO_LOANS_DELAY') T_1_2,
(SELECT COUNT(*) KPI_EXIST_2 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -2), 0, 7) and KPI='ATC_LOANS_AMT') T_2_2,
(SELECT COUNT(*) KPI_EXIST_2 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -2), 0, 7) and KPI='OM_DEPOSIT_AMT') T_3_2,
(SELECT COUNT(*) KPI_EXIST_2 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -2), 0, 7) and KPI='CALLS_ON_N') T_4_2,
(SELECT COUNT(*) KPI_EXIST_2 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -2), 0, 7) and KPI='OM_BANK_TRF_AMT') T_5_2,
(SELECT COUNT(*) KPI_EXIST_3 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -3), 0, 7) and KPI='TELCO_LOANS_DELAY') T_1_3,
(SELECT COUNT(*) KPI_EXIST_3 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -3), 0, 7) and KPI='ATC_LOANS_AMT') T_2_3,
(SELECT COUNT(*) KPI_EXIST_3 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -3), 0, 7) and KPI='OM_DEPOSIT_AMT') T_3_3,
(SELECT COUNT(*) KPI_EXIST_3 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -3), 0, 7) and KPI='CALLS_ON_N') T_4_3,
(SELECT COUNT(*) KPI_EXIST_3 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -3), 0, 7) and KPI='OM_BANK_TRF_AMT') T_5_3,
(SELECT COUNT(*) KPI_EXIST_4 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -4), 0, 7) and KPI='TELCO_LOANS_DELAY') T_1_4,
(SELECT COUNT(*) KPI_EXIST_4 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -4), 0, 7) and KPI='ATC_LOANS_AMT') T_2_4,
(SELECT COUNT(*) KPI_EXIST_4 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -4), 0, 7) and KPI='OM_DEPOSIT_AMT') T_3_4,
(SELECT COUNT(*) KPI_EXIST_4 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -4), 0, 7) and KPI='CALLS_ON_N') T_4_4,
(SELECT COUNT(*) KPI_EXIST_4 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -4), 0, 7) and KPI='OM_BANK_TRF_AMT') T_5_4,
(SELECT COUNT(*) KPI_EXIST_5 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -5), 0, 7) and KPI='TELCO_LOANS_DELAY') T_1_5,
(SELECT COUNT(*) KPI_EXIST_5 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -5), 0, 7) and KPI='ATC_LOANS_AMT') T_2_5,
(SELECT COUNT(*) KPI_EXIST_5 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -5), 0, 7) and KPI='OM_DEPOSIT_AMT') T_3_5,
(SELECT COUNT(*) KPI_EXIST_5 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -5), 0, 7) and KPI='CALLS_ON_N') T_4_5,
(SELECT COUNT(*) KPI_EXIST_5 FROM MON.SPARK_TT_PICO_KPIS WHERE EVENT_MONTH =SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -5), 0, 7) and KPI='OM_BANK_TRF_AMT') T_5_5,
(select COUNT(*) NB_EXPORT FROM (SELECT * FROM (SELECT * FROM MON.EXPORT_HISTORY WHERE EVENT_DATE='###SLICE_VALUE##' AND JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T WHERE T.STATUS = 'OK') M) T_6
