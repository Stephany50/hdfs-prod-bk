select  a.guid,a.msisdn, IF(UPPER(b.conformite) like '%OUI%','83ee5b91-7e08-ea11-80c7-005056bc18cc','3ab95a85-7e08-ea11-80c7-005056bc18cc') status
from (select msisdn, guid from cdr.spark_it_kyc_bdi_full where original_file_date = '###SLICE_VALUE###') a
left join (
select msisdn, conforme_art conformite from mon.spark_ft_kyc_bdi_pp where event_date = '###SLICE_VALUE###'
union
select msisdn, est_conforme conformite from mon.spark_ft_kyc_bdi_flotte where event_date = '###SLICE_VALUE###'
) b on a.msisdn = b.msisdn
