select
nvl(trim(replace(a.msisdn,';',' ')),'') as msisdn,
nvl(trim(replace(a.nom,';',' ')),'') as nom,
nvl(trim(replace(a.prenom,';',' ')),'') as prenom,
nvl(a.date_naissance,'') as date_naissance,
nvl(b.genre,'INCONNU') as sexe,
(case when trim(a.type_personne)='PP' then pp.conforme_art else flotte.est_conforme end) as conformite,
nvl(a.numero_piece,'') as numero_piece,
nvl(a.date_expiration,'') as date_expiration
from cdr.spark_it_kyc_bdi_full a
left join (select msisdn, est_conforme from MON.SPARK_FT_KYC_BDI_FLOTTE where event_date=DATE_SUB('###SLICE_VALUE###',1)) flotte on a.msisdn=flotte.msisdn
left join (select msisdn, conforme_art from mon.spark_ft_kyc_bdi_pp where event_date=DATE_SUB('###SLICE_VALUE###',1)) pp on a.msisdn=pp.msisdn
left join DIM.SPARK_DT_BASE_IDENTIFICATION b on a.msisdn = b.msisdn
where original_file_date='###SLICE_VALUE###'
and trim(a.msisdn) rlike '^\\d+$'
and length(trim(a.msisdn)) = 9