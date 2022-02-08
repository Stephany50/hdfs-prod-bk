INSERT INTO MON.SPARK_FT_CNI_EXPIREES
SELECT a.msisdn,a.numero_piece,a.type_piece,a.nom,a.prenom,a.date_naissance,a.date_expiration,
(case
when datediff(date_expiration,event_date)>0 and datediff(date_expiration,event_date)<=3 then 'dans_3_jours_au_plus'
when cast(months_between(event_date, date_expiration) as int) >= 6 then '6_mois_et_plus'
when cast(months_between(event_date, date_expiration) as int) >= 3 then '3_à_5_mois'
when cast(months_between(event_date, date_expiration) as int) < 3 then 'moins_de_3_mois'
else 'date_vide' end) periode_expiration,
(case
when datediff(date_expiration,event_date)>0 and datediff(date_expiration,event_date)<=3 then 'not_expired'
else 'expired' end) status,
current_timestamp INSERT_DATE,
EVENT_DATE
FROM MON.SPARK_FT_KYC_BDI_PP a
WHERE event_date = '###SLICE_VALUE###' and trim(est_suspendu) = 'NON' and statut_derogation = 'NON' 
and trim(msisdn) rlike '^\\d+$' and length(FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn))) = 9
and (cni_expire='OUI' or (datediff(date_expiration,event_date)<=3))