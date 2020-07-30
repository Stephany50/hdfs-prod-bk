
INSERT INTO  TMP.TT_MSISDN_SUMMARY_DATA
select
nvl(a.MSISDN,b.MSISDN) MSISDN,
case
    when a.msisdn is null or b.msisdn is null then nvl(a.COUVERTURE,b.COUVERTURE)
    else a.COUVERTURE -- pas de mise à jour en cas de match
end COUVERTURE,
case
    when a.msisdn is null or b.msisdn is null then nvl(a.TECHNOLOGIE,b.TECHNOLOGIE)
    else a.TECHNOLOGIE -- pas de mise à jour en cas de match
end TECHNOLOGIE,
case
    when a.msisdn is null or b.msisdn is null then nvl(a.STATUS,b.STATUS)
    else a.STATUS -- pas de mise à jour en cas de match
end STATUS,
(case when a.couverture = '4G' then nvl(b.First_couv_4G, a.event_date) else b.First_couv_4G end) FIRST_COUV_4G ,
(case when a.couverture = '4G' then a.event_date else b.Last_couv_4G end) LAST_COUV_4G,
(case when a.couverture = '4G' then nvl(b.Count_couv_4G, 0) +1 else b.Count_couv_4G end) COUNT_COUV_4G,
(case when a.couverture = '3G' then nvl(b.First_couv_3G, a.event_date) else b.First_couv_3G end) FIRST_COUV_3G,
( case when a.couverture = '3G' then a.event_date else b.Last_couv_3G end) LAST_COUV_3G,
(case when a.couverture = '3G' then nvl(b.Count_couv_3G, 0) +1 else b.Count_couv_3G end) COUNT_COUV_3G,
(case when a.couverture = '2G' then nvl(b.First_couv_2G, a.event_date) else b.First_couv_2G end) FIRST_COUV_2G,
(case when a.couverture = '2G' then a.event_date else b.Last_couv_2G  end ) LAST_COUV_2G,
(case when a.couverture = '2G' then nvl(b.Count_couv_2G, 0) +1 else b.Count_couv_2G end ) COUNT_COUV_2G,
(case when a.Technologie = '4G' then nvl(b.First_Phone_4G,  a.event_date) else b.First_Phone_4G end) FIRST_PHONE_4G,
(case when a.Technologie = '4G' then a.event_date else b.Last_Phone_4G end) LAST_PHONE_4G,
( case when a.Technologie = '4G' then nvl(b.Count_Phone_4G, 0) + 1 else b.Count_Phone_4G end) COUNT_PHONE_4G,
(case when a.Technologie in ('3G', '2.75G', '2.5G') then nvl(b.First_Phone_3G,  a.event_date) else b.First_Phone_3G end) FIRST_PHONE_3G,
(case when a.Technologie in ('3G', '2.75G', '2.5G') then a.event_date else b.Last_Phone_3G end) LAST_PHONE_3G,
(case when a.Technologie in ('3G', '2.75G', '2.5G')  then nvl(b.Count_Phone_3G, 0) + 1 else b.Count_Phone_3G end) COUNT_PHONE_3G,
(case when a.Technologie = '2G' then nvl(b.First_Phone_2G,  a.event_date) else b.First_Phone_2G end ) FIRST_PHONE_2G,
(case when a.Technologie = '2G' then a.event_date else b.Last_Phone_2G end) LAST_PHONE_2G,
(case when a.Technologie = '2G' then nvl(b.Count_Phone_2G, 0) + 1 else b.Count_Phone_2G end  ) COUNT_PHONE_2G,
(case when a.status = 'User_Data' then nvl(b.First_using_Data, a.event_date) else b.First_Using_Data end) FIRST_USING_DATA,
(case when a.status = 'User_Data' then a.event_date else b.Last_Using_Data end) LAST_USING_DATA,
(case when a.status = 'User_Data' then nvl(b.count_Using_Data, 0) + 1 else b.Count_Using_Data end) COUNT_USING_DATA,
case
    when a.msisdn is null or b.msisdn is null then nvl(a.BYTES_USED,b.BYTES_USED)
    else a.BYTES_USED -- pas de mise à jour en cas de match
end BYTES_USED,
(case when nvl(a.BYTES_USED, 0) >= nvl(b.MAX_BYTES_USED, 0) then nvl(a.BYTES_USED, 0) else nvl(b.MAX_BYTES_USED, 0) end) MAX_BYTES_USED,
(case when nvl(a.BYTES_USED, 0) >= nvl(b.MAX_BYTES_USED, 0) then a.EVENT_DATE else b.DATE_MAX_BYTES_USED  end ) DATE_MAX_BYTES_USED,
NULL OSP_STATUS ,
NULL OTARIE_FIRST_RAT,
NULL OTARIE_BYTES_2G ,
NULL OTARIE_BYTES_3G ,
NULL OTARIE_BYTES_4G ,
NULL OTARIE_BYTES_UKN ,
case
    when a.msisdn is null or b.msisdn is null then nvl(a.last_imei_used,b.last_imei_used)
    else (nvl(a.last_imei_used, b.last_imei_used))
end LAST_IMEI_USED,
NVL(a.event_date,b.event_date) event_date,
CURRENT_TIMESTAMP insert_date
from (
    SELECT
         event_date,
         msisdn ,
         couverture,
         technologie,
         status,
         bytes_used,
         last_imei_used,
        NULL FIRST_COUV_4G,
        NULL LAST_COUV_4G,
        NULL COUNT_COUV_4G,
        NULL FIRST_COUV_3G,
        NULL LAST_COUV_3G,
        NULL COUNT_COUV_3G,
        NULL FIRST_COUV_2G,
        NULL LAST_COUV_2G,
        NULL COUNT_COUV_2G,
        NULL FIRST_PHONE_4G,
        NULL LAST_PHONE_4G,
        NULL COUNT_PHONE_4G,
        NULL FIRST_PHONE_3G,
        NULL LAST_PHONE_3G,
        NULL COUNT_PHONE_3G,
        NULL FIRST_PHONE_2G,
        NULL LAST_PHONE_2G,
        NULL COUNT_PHONE_2G,
        NULL FIRST_USING_DATA,
        NULL LAST_USING_DATA,
        NULL COUNT_USING_DATA,
        NULL BYTES_USED,
        NULL MAX_BYTES_USED,
        NULL DATE_MAX_BYTES_USED,
        NULL OSP_STATUS,
        NULL OTARIE_FIRST_RAT,
        NULL OTARIE_BYTES_2G,
        NULL OTARIE_BYTES_3G,
        NULL OTARIE_BYTES_4G,
        NULL OTARIE_BYTES_UKN,
        CURRENT_TIMESTAMP INSERT_DATE
    FROM TMP.TT_MSISDN_SUMMARY_DATA_TRAFIC_DATA_SITE
)a
full outer join (
    select * from MON.SPARK_FT_MSISDN_SUMMARY_DATA
    where event_date =date_sub('###SLICE_VALUE###',1) and substring('###SLICE_VALUE###',1,7)=substring(date_sub('###SLICE_VALUE###',1),1,7)
) b on a.msisdn = b.msisdn