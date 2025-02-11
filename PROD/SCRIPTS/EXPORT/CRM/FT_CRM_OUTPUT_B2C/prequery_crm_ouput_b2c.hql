SELECT IF( T_1.FT_EXIST > 0 AND T_2.NB_EXPORT < 1 ,"OK","NOK")
FROM (
    SELECT COUNT(*) FT_EXIST 
    FROM MON.SPARK_FT_CRM_OUTPUT_B2C 
    WHERE EVENT_MONTH ='###SLICE_VALUE###') T_1,
(SELECT count(*) NB_EXPORT 
 FROM
    (SELECT * FROM (SELECT * FROM MON.EXPORT_HISTORY 
     WHERE EVENT_DATE='###SLICE_VALUE###' AND JOB_INSTANCEID='LOAD_EXPORT_CRM_OUPUT_B2C' 
     ORDER BY INSERT_DATE DESC LIMIT 1)  T WHERE T.STATUS = 'OK') M) T_2