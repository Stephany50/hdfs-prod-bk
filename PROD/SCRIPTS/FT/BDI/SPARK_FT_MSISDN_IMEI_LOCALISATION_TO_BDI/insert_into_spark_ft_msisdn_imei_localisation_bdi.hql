insert into MON.SPARK_FT_MSISDN_IMEI_LOCALISATION_TO_BDI


insert into TMP.TT_INFO_LOCALISATION_CONTRAT

select
a.access_key AS MSISDN,
trim(IMEI) AS IMEI,
trim(SITE_NAME) AS SITE_NAME,
trim(COMMERCIAL_REGION) AS REGION_COMMERCIALE,
trim(ADMINISTRATIVE_REGION) AS REGION_ADMINISTRATIVE,
trim(TOWNNAME) AS VILLE_SITE,
trim(COMMERCIAL_OFFER) AS OFFRE_COMMERCIALE,
trim(CONTRACT_TYPE) AS TYPE_CONTRAT,
trim(SEGMENTATION) AS SEGMENTATION
from (select access_key,commercial_offer,contract_type,segmentation
from MON.SPARK_FT_CONTRACT_SNAPSHOT
where event_date = date_add('###SLICE_VALUE###',1)) a
left join (select msisdn,imei,
row_number() over(partition by msisdn order by length(trim(imei)) desc nulls last) as rang
from MON.SPARK_FT_IMEI_ONLINE
where sdate = '###SLICE_VALUE###') b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.msisdn))
left join (select msisdn,site_name,commercial_region,
administrative_region,townname from (select msisdn,site_name,commercial_region,
administrative_region,townname,
row_number() over(partition by msisdn order by last_location_day desc nulls last) as rang
from MON.SPARK_FT_CLIENT_LAST_SITE_DAY
where event_date = '###SLICE_VALUE###') c1
where rang = 1) c
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(c.msisdn))