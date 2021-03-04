insert into TMP.tt_localisation_bdi2
select
msisdn,
imei,
site_name,
commercial_region,
administrative_region,
townname,
a.commercial_offer,
b.contract_type,
b.segmentation,
activation_date
from
(select *
from TMP.tt_localisation_bdi1) a
left join (select *
from DIM.spark_dt_offer_profiles) b
on upper(trim(a.COMMERCIAL_OFFER)) = upper(trim(b.profile_code))