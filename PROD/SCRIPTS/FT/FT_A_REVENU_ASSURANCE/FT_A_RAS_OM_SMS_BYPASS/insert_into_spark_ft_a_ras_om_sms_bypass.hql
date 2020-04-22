INSERT INTO AGG.SPARK_FT_A_RAS_OM_SMS_BYPASS
select
served_msisdn,
count(*) NBRE_SMS,
count (distinct other_party) AS NBER_other_party,
served_imei,
SERVED_PARTY_LOCATION,
(count (distinct other_party)/count(*)) AS SPREAD,
CURRENT_TIMESTAMP AS INSERT_DATE,
TO_DATE(TRANSACTION_DATE) EVENT_DATE
from MON.SPARK_FT_MSC_TRANSACTION
where TRANSACTION_DATE = '###SLICE_VALUE###'
and  transaction_direction ='Sortant'
and TRANSACTION_TYPE ='SMS_MO'
and other_party like '6%'
and length(other_party)=9
group by transaction_date,served_msisdn,served_imei,SERVED_PARTY_LOCATION
having count(*)>100 and (count (distinct other_party)/count(*))>=0.8