INSERT INTO CTI.SVI_APP_ABANDONS
SELECT DISTINCT
DATE_DEBUT_OMS AS JOUR,
DATE_FORMAT(date_debut_oms_nq, 'HH:mm:ss') HEURE,
case when DATE_FORMAT(date_debut_oms_nq, 'mm')<60
then DATE_FORMAT(date_debut_oms_nq, 'HH')||':00'
else DATE_FORMAT(date_debut_oms_nq, 'HH')||':30' end TRANCHE_HEURE,
A.id_appel,
A.MSISDN,
A.service AS numero_appele,
A.SEGMENT_CLIENT AS SEGMENT_CLIENT,
A.TYPE_HANGUP AS TYPE_HANGUP,
A.DATE_DEBUT_OMS AS DATE_DEBUT_OMS,
'NA' as ELEMENT,
'NA' as TYPE_ELEMENT,
CURRENT_TIMESTAMP() INSERT_DATE,
DATE_DEBUT_OMS as EVENT_DATE
from
(
select A.*
FROM
(select *
from CTI.SVI_APPEL
where DATE_DEBUT_OMS='###SLICE_VALUE###'
and SERVICE in ('950','951','96400400','900','955','33410000','9111','8900','8911')
) A
left join
(select distinct id_appel from CTI.SVI_APPEL_SELFCARE WHERE EVENT_DATE='###SLICE_VALUE###')B
on A.id_appel = B.id_appel where B.id_appel is null
)A
left join
(select distinct id_appel from CTI.SVI_APP_TRANSFERE WHERE EVENT_DATE='###SLICE_VALUE###'
)C
on A.id_appel = C.id_appel where C.id_appel is null