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
  (
    SELECT type_personne,('vr_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')) key,count(*) value
    FROM MON.SPARK_FT_BDI WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') AND upper(EST_SUSPENDU)<>'OUI' AND trim(conforme_art) = 'OUI' 
    GROUP BY type_personne,('vr_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue'))
  )
  union
  (
    SELECT type_personne,('vr_non_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')) key,count(*) value
    FROM MON.SPARK_FT_BDI WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') AND upper(EST_SUSPENDU)<>'OUI' AND trim(conforme_art) = 'OUI' 
    GROUP BY type_personne,('vr_non_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue'))
  )
  ---repartion de la non conformité pour les majeurs et par critères selon l'art
  (
    SELECT
    type_personne,
    sum(case when nom_prenom_absent='OUI' or nom_parent_absent='OUI' or nom_prenom_douteux='OUI' or nom_parent_douteux='OUI' then 1 else 0 end) NBR_TOTAL_NS_RECAP_AN,
    sum(case when cni_representant_legal_an='OUI' then 1 else 0 end) NBR_TOTAL_RL_RECAP_AN,
    sum(case when rccm_an='OUI' then 1 else 0 end) NBR_TOTAL_RCCM_RECAP_AN,
    sum(case when adresse_structure_an='OUI' then 1 else 0 end) NBR_TOTAL_AD_RECAP_AN
    FROM MON.SPARK_FT_BDI WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') AND upper(EST_SUSPENDU)<>'OUI' AND trim(conforme_art) = 'NON' and upper(type_personne) = 'MAJEUR'
    GROUP BY type_personne,('vr_non_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue'))
  )
)
