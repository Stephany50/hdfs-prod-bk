INSERT INTO MON.SPARK_FT_A_CNI_EXPIREES
SELECT 
a.NBR_TOTAL_CNI_EXP,
b.NBR_TOTAL_CNI_NEW_EXP,
c.NBR_TOTAL_CNI_CORRECTED,
a.NBR_TOTAL_CNI_EXP_MAX_THREE_DAYS,
a.NBR_TOTAL_DATE_EXP_INV,
a.CNI_EXP_MORE_THAN_SIX_MONTHS_AGO,
a.CNI_EXP_THREE_MONTHS_AGO,
a.CNI_EXP_LESS_THAN_THREE_MONTHS,
current_timestamp INSERT_DATE,
'###SLICE_VALUE###' EVENT_DATE
from 
(select
sum(case when upper(trim(STATUS))=upper('expired') then 1 else 0 end) NBR_TOTAL_CNI_EXP,
sum (case when upper(trim(periode_expiration))=upper('dans_3_jours_au_plus') then 1 else 0 end) NBR_TOTAL_CNI_EXP_MAX_THREE_DAYS,
sum (case when upper(trim(periode_expiration))=upper('date_vide') then 1 else 0 end) NBR_TOTAL_DATE_EXP_INV,
sum (case when upper(trim(periode_expiration))=upper('6_mois_et_plus') then 1 else 0 end) CNI_EXP_MORE_THAN_SIX_MONTHS_AGO,
sum (case when upper(trim(periode_expiration))=upper('3_à_5_mois') then 1 else 0 end) CNI_EXP_THREE_MONTHS_AGO,
sum (case when upper(trim(periode_expiration))=upper('moins_de_3_mois') then 1 else 0 end) CNI_EXP_LESS_THAN_THREE_MONTHS
from MON.SPARK_FT_CNI_EXPIREES where event_date = '###SLICE_VALUE###'
) a,
(select count(t.msisdn) NBR_TOTAL_CNI_NEW_EXP
from (select msisdn from MON.SPARK_FT_CNI_EXPIREES where event_date = '###SLICE_VALUE###' and upper(trim(STATUS))=upper('expired')) t
left join (select msisdn from MON.SPARK_FT_CNI_EXPIREES 
where event_date= DATE_SUB(to_date('###SLICE_VALUE###'),1) and upper(trim(STATUS))=upper('expired')) y on t.msisdn = y.msisdn
where y.msisdn is null
) b,
(select count(*) NBR_TOTAL_CNI_CORRECTED
from (select msisdn from MON.SPARK_FT_CNI_EXPIREES where event_date = DATE_SUB(to_date('###SLICE_VALUE###'),1) 
and upper(trim(STATUS))=upper('expired')) y
left join (select msisdn,cni_expire from MON.SPARK_FT_KYC_BDI_PP 
where event_date = '###SLICE_VALUE###') t on t.msisdn = y.msisdn
where t.cni_expire='NON'
) c
