insert into AGG.SPARK_FT_A_BDI_B2B
select recap.*,new_ac.*,'' col1,'' col2,'' col3,'' col4,current_timestamp() as insert_date,
'###SLICE_VALUE###' as event_date from
(select
type_personne,
sum(case when trim(nom_structure_an) = 'OUI' and upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end ) as Nom_structure_an,
sum(case when trim(rccm_an) = 'OUI' and upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as Num_rccm_an,
sum(case when trim(num_piece_rpstant_an) = 'OUI' and upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as Num_rpstant_legal_an,
sum(case when trim(date_souscription_an) = 'OUI' and trim(type_personne)='FLOTTE' and upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as date_souscription_an,
sum(case when trim(adresse_structure_an) = 'OUI' and upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as adresse_structure_an,
sum(case when trim(num_tel_an) = 'OUI' and upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as num_telephone_an,
sum(case when trim(nom_prenom_an) = 'OUI' and trim(type_personne)='FLOTTE' and upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as nom_prenom_an,
sum(case when trim(numero_piece_an) = 'OUI' and trim(type_personne)='FLOTTE' and upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as numero_piece_an,
sum(case when trim(imei_an) = 'OUI'and trim(type_personne)='FLOTTE' and upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as imei_an,
sum(case when trim(adresse_an) = 'OUI'and trim(type_personne)='FLOTTE' and upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as adresse_an,
sum(case when trim(statut_an) = 'OUI' and upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as statut_an,
0 as m2m_generique_nb,
0 as m2m_generique_actif,
sum(case when trim(est_conforme) = 'NON' and upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end ) as nb_ligne_en_anomalie,
sum(case when upper(trim(statut)) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as nb_actifs,
sum(case when not(msisdn is null or trim(msisdn) = '') then 1 else 0 end) as nb_total
from MON.SPARK_FT_KYC_BDI_FLOTTE
where event_date = '###SLICE_VALUE###'
group by type_personne) recap 
LEFT JOIN
(SELECT
    type_personne pers,
    sum(case when trim(nom_structure_an) = 'OUI' and B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end ) as nom_structure_an_new,
    sum(case when trim(rccm_an) = 'OUI' and B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as num_rccm_an_new,
    sum(case when trim(num_piece_rpstant_an) = 'OUI' and B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as num_rpstant_legal_an_new,
    sum(case when trim(date_souscription_an) = 'OUI' and trim(type_personne)='FLOTTE' and B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as date_souscription_an_new,
    sum(case when trim(adresse_structure_an) = 'OUI' and B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as adresse_structure_an_new,
    sum(case when trim(num_tel_an) = 'OUI' and B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as num_telephone_an_new,
    sum(case when trim(nom_prenom_an) = 'OUI' and trim(type_personne)='FLOTTE' and B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as nom_prenom_an_new,
    sum(case when trim(numero_piece_an) = 'OUI' and trim(type_personne)='FLOTTE' and B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as numero_piece_an_new,
    sum(case when trim(imei_an) = 'OUI' and trim(type_personne)='FLOTTE' and B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as imei_an_new,
    sum(case when trim(adresse_an) = 'OUI' and trim(type_personne)='FLOTTE' and B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as adresse_an_new,
    sum(case when trim(statut_an) = 'OUI' and B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as statut_an_new,
    sum(case when trim(A.est_conforme) = 'NON' and B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end ) as nb_ligne_en_anomalie_new,
    sum(case when B.msisdn is null and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as nb_actifs_new,
    sum(case when not(A.msisdn is null or trim(A.msisdn) = '') and B.msisdn is null then 1 else 0 end) as nb_total_new,
    sum(case when upper(A.est_conforme) = 'OUI' and upper(B.est_conforme) = 'NON' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as nbr_corrected_by_day,
    sum(case when upper(A.est_conforme) = 'NON' and upper(B.est_conforme) = 'OUI' and trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF') then 1 else 0 end) as nbr_regression_by_day
FROM (SELECT * FROM MON.SPARK_FT_KYC_BDI_FLOTTE A WHERE EVENT_DATE = '###SLICE_VALUE###') A
LEFT JOIN (SELECT msisdn,est_conforme FROM MON.SPARK_FT_KYC_BDI_FLOTTE A WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)) B ON A.msisdn = B.msisdn
group by type_personne
) new_ac on recap.type_personne = new_ac.pers