insert into MON.SPARK_FT_MSISDN_IMEI_LOCALISATION_TO_BDI
select
msisdn,
imei,
site_name,
commercial_region,
administrative_region,
townname,
commercial_offer,
contract_type,
segmentation,
current_timestamp() as insert_date,
'###SLICE_VALUE###' as event_date
from (
select a.*,
row_number() over(partition by msisdn order by activation_date desc nulls last) as rn
from TMP.tt_localisation_bdi2 a
) b where rn = 1