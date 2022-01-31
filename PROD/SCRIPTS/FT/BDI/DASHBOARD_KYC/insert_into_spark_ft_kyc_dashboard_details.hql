insert into AGG.SPARK_FT_KYC_DASHBOARD_DETAILS
SELECT *,current_timestamp() AS insert_date,'###SLICE_VALUE###' AS EVENT_DATE
FROM 
(
  ---repartition de la conformité par region et par type de personne
  (
    SELECT type_personne,('vr_total_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')) key,count(*) value
    FROM MON.SPARK_FT_BDI WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') AND upper(EST_SUSPENDU)<>'OUI'
    GROUP BY type_personne,('vr_total_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue'))
  )
  union
  (--- les lignes conformes par region et par type de personne
    SELECT type_personne,('vr_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')) key,count(*) value
    FROM MON.SPARK_FT_BDI WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') AND upper(EST_SUSPENDU)<>'OUI' AND trim(conforme_art) = 'OUI' 
    GROUP BY type_personne,('vr_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue'))
  )
  union
  (--- repartition des lignes non conformes par region et type de personne.
    SELECT type_personne,('vr_non_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')) key,count(*) value
    FROM MON.SPARK_FT_BDI WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') AND upper(EST_SUSPENDU)<>'OUI' AND trim(conforme_art) = 'NON' 
    GROUP BY type_personne,('vr_non_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue'))
  )
  ---repartion de la non conformité pour les majeurs et mineur par critères selon l'art
  (
    SELECT
    type_personne,
    sum(case when date_activation is null then 1 else 0 end) date_activation_an,
    sum(case when nom_prenom_absent = 'OUI' or nom_prenom_douteux='OUI' then 1 else 0 end) nom_prenom_an,
    sum(case when numero_piece_absent = 'OUI' or numero_piece_inf_4 = 'OUI' or  numero_piece_non_authorise = 'OUI' or  numero_piece_egale_msisdn = 'OUI'
    or  numero_piece_a_caract_non_auth = 'OUI' or  numero_piece_uniquement_lettre = 'OUI' then 1 else 0 end) numero_piece_an,
    sum(case when date_expiration_douteuse = 'OUI' or date_expiration_absent = 'OUI' then 1 else 0 end) date_expiration_an,
    sum(case when type_piece is null or trim(type_piece) = '' then 1 else 0 end) type_piece_an,
    sum(case when nom_parent_absent = 'OUI' or nom_parent_douteux = 'OUI' then 1 else 0 end) nom_parent_an,
    sum(case when date_naissance_absent = 'OUI' or date_naissance_douteux = 'OUI' then 1 else 0 end) date_naissance_an,
    sum(case when numero_piece_tut_absent = 'OUI' or numero_piece_tut_inf_4 = 'OUI' or  numero_piece_tut_non_auth = 'OUI' or  numero_piece_tut_egale_msisdn = 'OUI'
    or  numero_piece_tut_carac_non_a = 'OUI' or  numero_piece_tut_uniq_lettre = 'OUI' then 1 else 0 end) numero_piece_tut_an,
    sum(case when date_naissance_tut_absent = 'OUI' or date_naissance_tut_douteux = 'OUI' then 1 else 0 end) date_naissance_tut_an,
    sum(case when adresse_absent = 'OUI' or adresse_douteuse = 'OUI' then 1 else 0 end) adresse_an,
    sum(case when imei is null or trim(imei) = '' then 1 else 0 end) adresse_an,
    sum(case when multi_sim = 'OUI' then 1 else 0 end) multi_sim,
    sum(case when (cast(months_between('###SLICE_VALUE###', date_expiration) as int) < 6) then 1 else 0 end) cni_expire
    FROM MON.SPARK_FT_BDI WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') AND upper(EST_SUSPENDU)<>'OUI' AND trim(conforme_art) = 'NON' 
    GROUP BY type_personne
  )
  ---Repartition de la non conformité par 

)
