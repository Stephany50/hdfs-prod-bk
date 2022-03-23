insert into TMP.tt_localisation_bdi1
select
FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) AS MSISDN,
trim(IMEI) AS IMEI,
trim(SITE_NAME) AS SITE_NAME,
trim(COMMERCIAL_REGION) AS COMMERCIAL_REGION,
trim(ADMINISTRATIVE_REGION) AS ADMINISTRATIVE_REGION,
trim(TOWNNAME) as TOWNNAME,
trim(COMMERCIAL_OFFER) AS COMMERCIAL_OFFER,
activation_date
from (select access_key as msisdn,commercial_offer,
activation_date
from MON.SPARK_FT_CONTRACT_SNAPSHOT
where event_date = date_add('###SLICE_VALUE###',1)) a
left join (
--select msisdn,imei from (select msisdn,imei,
--row_number() over(partition by msisdn order by length(trim(imei)) desc nulls last) as rang
--from MON.SPARK_FT_IMEI_ONLINE
--where sdate = '###SLICE_VALUE###') b1 where rang = 1 
select msisdn, max(imei) imei
from MON.SPARK_FT_IMEI_ONLINE
where sdate = '###SLICE_VALUE###' and trim(IMEI) rlike '^\\d{14,16}$'
group by msisdn 
) b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.msisdn))
left join (select msisdn,site_name,commercial_region,
administrative_region,townname from (select msisdn,site_name,commercial_region,
administrative_region,townname,
row_number() over(partition by msisdn order by last_location_day desc nulls last) as rang
from MON.SPARK_FT_CLIENT_LAST_SITE_DAY
where event_date = '###SLICE_VALUE###') c1
where rang = 1) c
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(c.msisdn))