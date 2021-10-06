INSERT INTO MON.SPARK_FT_CNI_EXPIREES
SELECT a.msisdn,a.numero_piece,a.type_piece,a.nom,a.prenom,a.date_naissance,a.date_expiration,
(case
when datediff(date_expiration,event_date)>0 and datediff(date_expiration,event_date)<=3 then 'dans_3_jours_au_plus'
when cast(months_between(event_date, date_expiration) as int) >= 6 then '6_mois_et_plus'
when cast(months_between(event_date, date_expiration) as int) >= 3 then '3_à_5_mois'
when cast(months_between(event_date, date_expiration) as int) < 3 then 'moins_de_3_mois'
else 'date_vide'
end) periode_expiration,
(case
when datediff(date_expiration,event_date)>0 and datediff(date_expiration,event_date)<=3 then 'not_expired'
else 'expired' end) status,
current_timestamp INSERT_DATE,
EVENT_DATE
FROM
(select
a1.msisdn,nvl(a2.date_expiration,a1.date_expiration)  date_expiration,
a1.nom, a1.prenom, a1.date_naissance,a1.numero_piece,a1.type_piece,
a1.est_suspendu,nvl(a2.event_date,a1.event_date) event_date
from
(select *
from MON.SPARK_FT_BDI
where event_date = '###SLICE_VALUE###') a1
left join (select *
from MON.SPARK_FT_BDI_CRM_B2C
where event_date = '###SLICE_VALUE###') a2 on trim(a1.msisdn) = trim(a2.msisdn)) a
WHERE trim(est_suspendu) = 'NON'
AND ( not(date_expiration >= event_date) or datediff(date_expiration,event_date)<=3)
AND trim(msisdn) rlike '^\\d+$' and length(FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn))) = 9

--date_expiration is null or date_expiration < event_date