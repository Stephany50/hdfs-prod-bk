SELECT IF(
    T_2.FT_OTHER_PARTY_SITE_TRAFFIC > DATEDIFF(LAST_DAY('###SLICE_VALUE###'||'-01') , '###SLICE_VALUE###'||'-01') AND T_3.NB_EXPORT < 1
    , "OK"
    , "NOK"
) FT_EXIST
FROM
(SELECT COUNT(DISTINCT EVENT_DATE) FT_OTHER_PARTY_SITE_TRAFFIC FROM MON.SPARK_FT_OTHER_PARTY_SITE_TRAFFIC WHERE EVENT_DATE BETWEEN TO_DATE(CONCAT('###SLICE_VALUE###','-01')) AND  LAST_DAY(TO_DATE(CONCAT('###SLICE_VALUE###','-01')))) T_2,
(
select count(*) NB_EXPORT from
(select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_3