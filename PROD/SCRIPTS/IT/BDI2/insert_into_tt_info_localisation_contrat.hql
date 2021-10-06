insert into TMP.TT_INFO_LOCALISATION_CONTRAT

select a.msisdn,
a.imei,

from TMP.TT_BDI_TMP a
left join (select msisdn,imei,
row_number() over(partition by msisdn order by last_location_day desc nulls last) as rang
from MON.SPARK_FT_IMEI_ONLINE
where sdate in (select max(sdate) from MON.SPARK_FT_IMEI_ONLINE)) b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.msisdn))
left join (select msisdn,site_name,commercial_region,
administrative_region,townname from (select msisdn,site_name,commercial_region,
administrative_region,townname,
row_number() over(partition by msisdn order by last_location_day desc nulls last) as rang
from MON.SPARK_FT_CLIENT_LAST_SITE_DAY
where event_date = (select max(event_date) from MON.SPARK_FT_CLIENT_LAST_SITE_DAY)) c1
where rang = 1) c
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(c.msisdn))
left join (select access_key,commercial_offer,contract_type,segmentation
from MON.SPARK_FT_CONTRACT_SNAPSHOT
where event_date=(select max(event_date) from MON.SPARK_FT_CONTRACT_SNAPSHOT)) d
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(d.access_key))