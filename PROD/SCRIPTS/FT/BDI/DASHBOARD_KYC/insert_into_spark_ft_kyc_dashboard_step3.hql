--Repartitions des anomalie par critères sur : La base globale et les nouvelles acquisitions.
insert into AGG.SPARK_FT_KYC_DASHBOARD_DETAILS
SELECT *,current_timestamp() AS insert_date,'###SLICE_VALUE###' AS EVENT_DATE FROM
((SELECT 'GLOBAL' type_kpi,type_personne,region,type_piece,R.key,R.value
FROM (SELECT
        (case when A.type_personne in ('MAJEUR','PP') then 'MAJEUR' when A.type_personne in ('MINEUR') then 'MINEUR' else 'AUTRE' end) type_personne,
        (translate(UPPER(nvl(A.region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')) region,
        A.type_piece,
        sum(case when A.date_activation is null then 1 else 0 end) date_activation_an,
        sum(case when A.nom_prenom_absent = 'OUI' or A.nom_prenom_douteux='OUI' then 1 else 0 end) nom_prenom_an,
        sum(case when A.numero_piece_absent = 'OUI' or A.numero_piece_inf_4 = 'OUI' or  A.numero_piece_non_authorise = 'OUI' or  A.numero_piece_egale_msisdn = 'OUI'
        or  A.numero_piece_a_caract_non_auth = 'OUI' or  A.numero_piece_uniquement_lettre = 'OUI' then 1 else 0 end) numero_piece_an,
        sum(case when A.date_expiration_douteuse = 'OUI' or A.date_expiration_absent = 'OUI' then 1 else 0 end) date_expiration_an,
        sum(case when A.type_piece is null or trim(A.type_piece) = '' then 1 else 0 end) type_piece_an,
        sum(case when A.nom_parent_absent = 'OUI' or A.nom_parent_douteux = 'OUI' then 1 else 0 end) nom_parent_an,
        sum(case when A.date_naissance_absent = 'OUI' or A.date_naissance_douteux = 'OUI' then 1 else 0 end) date_naissance_an,
        sum(case when A.numero_piece_tut_absent = 'OUI' or A.numero_piece_tut_inf_4 = 'OUI' or  A.numero_piece_tut_non_auth = 'OUI' or  A.numero_piece_tut_egale_msisdn = 'OUI'
        or  A.numero_piece_tut_carac_non_a = 'OUI' or  A.numero_piece_tut_uniq_lettre = 'OUI' then 1 else 0 end) numero_piece_tut_an,
        sum(case when A.date_naissance_tut_absent = 'OUI' or A.date_naissance_tut_douteux = 'OUI' then 1 else 0 end) date_naissance_tut_an,
        sum(case when A.adresse_absent = 'OUI' or A.adresse_douteuse = 'OUI' then 1 else 0 end) adresse_an,
        sum(case when A.imei is null or trim(A.imei) = '' then 1 else 0 end) imei_an,
        sum(case when A.multi_sim = 'OUI' then 1 else 0 end) multi_sim,
        sum(case when (cast(months_between('###SLICE_VALUE###', A.date_expiration) as int) < 6) then 1 else 0 end) cni_expire
        FROM MON.SPARK_FT_KYC_BDI_PP A WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') and  upper(A.EST_SUSPENDU)<>'OUI' and A.conforme_art='NON'
        GROUP BY (case when A.type_personne in ('MAJEUR','PP') then 'MAJEUR' when A.type_personne in ('MINEUR') then 'MINEUR' else 'AUTRE' end),
        (translate(UPPER(nvl(A.region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')),A.type_piece
) LATERAL VIEW EXPLODE(MAP(
    'date_activation_an',date_activation_an,
    'nom_prenom_an',nom_prenom_an,
    'numero_piece_an',numero_piece_an,
    'date_expiration_an',date_expiration_an,
    'type_piece_an',type_piece_an,
    'nom_parent_an',nom_parent_an,
    'date_naissance_an',date_naissance_an,
    'numero_piece_tut_an',numero_piece_tut_an,
    'adresse_an',adresse_an,
    'multi_sim',multi_sim,
    'cni_expire',cni_expire   
)) R as key, value)
UNION
(SELECT 'NVL_AQC' type_kpi,type_personne,region,type_piece,R.key,R.value
FROM (SELECT
        (case when A.type_personne in ('MAJEUR','PP') then 'MAJEUR' when A.type_personne in ('MINEUR') then 'MINEUR' else 'AUTRE' end) type_personne,
        (translate(UPPER(nvl(A.region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')) region,
        A.type_piece,
        sum(case when A.date_activation is null then 1 else 0 end) date_activation_an,
        sum(case when A.nom_prenom_absent = 'OUI' or A.nom_prenom_douteux='OUI' then 1 else 0 end) nom_prenom_an,
        sum(case when A.numero_piece_absent = 'OUI' or A.numero_piece_inf_4 = 'OUI' or  A.numero_piece_non_authorise = 'OUI' or  A.numero_piece_egale_msisdn = 'OUI'
        or  A.numero_piece_a_caract_non_auth = 'OUI' or  A.numero_piece_uniquement_lettre = 'OUI' then 1 else 0 end) numero_piece_an,
        sum(case when A.date_expiration_douteuse = 'OUI' or A.date_expiration_absent = 'OUI' then 1 else 0 end) date_expiration_an,
        sum(case when A.type_piece is null or trim(A.type_piece) = '' then 1 else 0 end) type_piece_an,
        sum(case when A.nom_parent_absent = 'OUI' or A.nom_parent_douteux = 'OUI' then 1 else 0 end) nom_parent_an,
        sum(case when A.date_naissance_absent = 'OUI' or A.date_naissance_douteux = 'OUI' then 1 else 0 end) date_naissance_an,
        sum(case when A.numero_piece_tut_absent = 'OUI' or A.numero_piece_tut_inf_4 = 'OUI' or  A.numero_piece_tut_non_auth = 'OUI' or  A.numero_piece_tut_egale_msisdn = 'OUI'
        or  A.numero_piece_tut_carac_non_a = 'OUI' or  A.numero_piece_tut_uniq_lettre = 'OUI' then 1 else 0 end) numero_piece_tut_an,
        sum(case when A.date_naissance_tut_absent = 'OUI' or A.date_naissance_tut_douteux = 'OUI' then 1 else 0 end) date_naissance_tut_an,
        sum(case when A.adresse_absent = 'OUI' or A.adresse_douteuse = 'OUI' then 1 else 0 end) adresse_an,
        sum(case when A.imei is null or trim(A.imei) = '' then 1 else 0 end) imei_an,
        sum(case when A.multi_sim = 'OUI' then 1 else 0 end) multi_sim,
        sum(case when (cast(months_between('###SLICE_VALUE###', A.date_expiration) as int) < 6) then 1 else 0 end) cni_expire
        FROM MON.SPARK_FT_KYC_BDI_PP A WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') and  upper(A.EST_SUSPENDU)<>'OUI' and A.conforme_art='NON'
        and TO_DATE(A.date_activation) = TO_DATE('###SLICE_VALUE###')
        GROUP BY (case when A.type_personne in ('MAJEUR','PP') then 'MAJEUR' when A.type_personne in ('MINEUR') then 'MINEUR' else 'AUTRE' end),
        (translate(UPPER(nvl(A.region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')),A.type_piece
) LATERAL VIEW EXPLODE(MAP(
    'date_activation_an',date_activation_an,
    'nom_prenom_an',nom_prenom_an,
    'numero_piece_an',numero_piece_an,
    'date_expiration_an',date_expiration_an,
    'type_piece_an',type_piece_an,
    'nom_parent_an',nom_parent_an,
    'date_naissance_an',date_naissance_an,
    'numero_piece_tut_an',numero_piece_tut_an,
    'adresse_an',adresse_an,
    'multi_sim',multi_sim,
    'cni_expire',cni_expire   
)) R as key, value))