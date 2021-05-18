insert into AGG.SPARK_FT_A_BDI_B2B
select
type_personne,
sum(case when trim(nom_structure_an) = 'OUI' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end ) as Nom_structure_an,
sum(case when trim(rccm_an) = 'OUI' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as Num_rccm_an,
sum(case when trim(num_piece_rpstant_an) = 'OUI' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as Num_rpstant_legal_an,
sum(case when trim(date_souscription_an) = 'OUI' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as date_souscription_an,
sum(case when trim(adresse_structure_an) = 'OUI' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as adresse_structure_an,
sum(case when trim(num_tel_an) = 'OUI' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as num_telephone_an,
sum(case when trim(nom_prenom_an) = 'OUI' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as nom_prenom_an,
sum(case when trim(numero_piece_an) = 'OUI' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as numero_piece_an,
sum(case when trim(imei_an) = 'OUI' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as imei_an,
sum(case when trim(adresse_an) = 'OUI' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as adresse_an,
sum(case when trim(statut_an) = 'OUI' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as statut_an,
0 as m2m_generique_nb,
0 as m2m_generique_actif,
sum(case when trim(est_conforme) = 'NON' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end ) as nb_ligne_en_anomalie,
sum(case when trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as nb_actifs,
sum(case when not(msisdn is null or trim(msisdn) = '') then 1 else 0 end) as nb_total,
current_timestamp() as insert_date,
'###SLICE_VALUE###' as event_date
from MON.SPARK_FT_BDI_B2B
where event_date = '###SLICE_VALUE###'
group by type_personne