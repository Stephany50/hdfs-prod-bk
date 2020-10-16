SELECT IF(
T_1.accounts_NB > 0 AND
T_2.CLIENT_SITE_NB > 0 AND
T_3.NB_EXPORT < 1
,"OK","NOK")
FROM
(SELECT COUNT(*) accounts_NB FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT WHERE event_date = '###SLICE_VALUE###' ) T_1,
(SELECT COUNT(*) CLIENT_SITE_NB FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE = '###SLICE_VALUE###' ) T_2,
(
select count(*) NB_EXPORT from
(select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='LOAD_EXPORT_CNC_DECLARATION_COMPTE' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_3
