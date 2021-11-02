insert into AGG.SPARK_FT_KYC_DASHBOARD
SELECT R.key,R.value,current_timestamp() AS insert_date,'###SLICE_VALUE###' AS EVENT_DATE
FROM (
  SELECT A.*,B.*,C.*
  FROM
    (SELECT ----KPI BDI conformité
      count(distinct msisdn) vg_total_actif_pp, ---Total des Personnes Physiques
      sum(case when trim(type_personne) in ('PP','MAJEUR') then 1 else 0 end) vg_total_actif_maj, 
      sum(case when trim(type_personne) = 'MINEUR' then 1 else 0 end) vg_total_actif_min,
      sum(case when trim(conforme_art) = 'OUI' then 1 else 0 end) vg_total_actif_pp_conform,
      sum(case when trim(conforme_art) = 'NON' then 1 else 0 end) vg_total_actif_pp_non_conform,
      sum(case when trim(type_personne) in ('PP','MAJEUR') and trim(conforme_art) = 'OUI' then 1 else 0 end) vg_total_actif_maj_conform,
      sum(case when trim(type_personne) ='MINEUR' and trim(conforme_art)='OUI' then 1 else 0 end) vg_total_actif_min_conform,
      sum(case when trim(type_personne) in ('PP','MAJEUR') and trim(conforme_art) = 'NON' then 1 else 0 end) vg_total_actif_maj_non_conform,
      sum(case when trim(type_personne) ='MINEUR' and trim(conforme_art)='NON' then 1 else 0 end) vg_total_actif_min_non_conform,
      sum(case when trim(statut_derogation) = 'OUI' then 1 else 0 end) vg_total_abonne_derogation,
      sum(case when trim(statut_derogation) = 'OUI' and trim(conforme_art)='NON' then 1 else 0 end) vg_total_abonne_derogation_non_conform,
      sum(case when to_date(date_activation) = TO_DATE('###SLICE_VALUE###') then 1 else 0 end) vg_total_activations_day,
      sum(case when to_date(date_activation) = TO_DATE('###SLICE_VALUE###') and trim(conforme_art)='NON' then 1 else 0 end) vg_total_activations_day_non_conform,
      sum(case when trim(multi_sim) = 'OUI' then 1 else 0 end) vg_total_multisim,
      sum(case when trim(multi_sim) = 'OUI' and trim(conforme_art)='NON' then 1 else 0 end) vg_total_multisim_non_conform,
      sum(case when trim(cni_expire) = 'OUI' then 1 else 0 end) vg_total_cni_expire,
      sum(case when trim(cni_expire) = 'OUI' and trim(conforme_art)='NON' then 1 else 0 end) vg_total_cni_expire_non_conform
      FROM (SELECT * FROM MON.SPARK_FT_BDI WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') AND upper(EST_SUSPENDU)<>'OUI')) A,
      (SELECT --- KPI HLR
       count(distinct msisdn) vhlr_total_abonnes,
       sum(case when trim(statut) = 'ACTIF' then 1 else 0 end) vhlr_total_actif_abonnes,
       sum(case when trim(statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT') then 1 else 0 end) vhlr_total_onewayblock_abonnes,
       sum(case when trim(statut) = 'SUSPENDU' then 1 else 0 end) vhlr_total_twowayblock_actif_abonnes
       FROM (SELECT * FROM MON.SPARK_FT_ABONNE_HLR WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###'))) B,
      (SELECT --- KPI SUR ZSMART
       count(distinct msisdn) vzm_total_abonnes,
       sum(case when trim(statut) = 'Actif' then 1 else 0 end) vzm_total_actif_abonnes,
       sum(case when trim(statut) = 'Suspendu' then 1 else 0 end) vzm_total_suspendu_abonnes
       FROM (SELECT * FROM MON.SPARK_FT_ZSMART_CONF WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###'))) C
) LATERAL VIEW EXPLODE(MAP(
    'vg_total_actif_pp',vg_total_actif_pp,
    'vg_total_actif_maj',vg_total_actif_maj,
    'vg_total_actif_min',vg_total_actif_min,
    'vg_total_actif_pp_conform',vg_total_actif_pp_conform,
    'vg_total_actif_pp_non_conform',vg_total_actif_pp_non_conform,
    'vg_total_actif_maj_conform',vg_total_actif_maj_conform,
    'vg_total_actif_min_conform',vg_total_actif_min_conform,
    'vg_total_actif_maj_non_conform',vg_total_actif_maj_non_conform,
    'vg_total_actif_min_non_conform',vg_total_actif_min_non_conform,
    'vg_total_abonne_derogation',vg_total_abonne_derogation,
    'vg_total_abonne_derogation_non_conform',vg_total_abonne_derogation_non_conform,
    'vg_total_activations_day',vg_total_activations_day,
    'vg_total_activations_day_non_conform',vg_total_activations_day_non_conform,
    'vg_total_multisim',vg_total_multisim,
    'vg_total_multisim_non_conform',vg_total_multisim_non_conform,
    'vg_total_cni_expire',vg_total_cni_expire,
    'vg_total_cni_expire_non_conform',vg_total_cni_expire_non_conform,
    'vhlr_total_abonnes',vhlr_total_abonnes,
    'vhlr_total_actif_abonnes',vhlr_total_actif_abonnes,
    'vhlr_total_onewayblock_abonnes',vhlr_total_onewayblock_abonnes,
    'vhlr_total_twowayblock_actif_abonnes',vhlr_total_twowayblock_actif_abonnes  
)) R as key, value
union
SELECT * ,current_timestamp() AS insert_date,'###SLICE_VALUE###' AS EVENT_DATE FROM
(
  (
    SELECT ('vr_total_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')) key,count(*) value
    FROM MON.SPARK_FT_BDI WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') AND upper(EST_SUSPENDU)<>'OUI'
    GROUP BY ('vr_total_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue'))
  )
  union
  (
    SELECT ('vr_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')) key,count(*) value
    FROM MON.SPARK_FT_BDI WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') AND upper(EST_SUSPENDU)<>'OUI' AND trim(conforme_art) = 'OUI' 
    GROUP BY ('vr_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue'))
  )
  union
  (
    SELECT ('vr_non_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')) key,count(*) value
    FROM MON.SPARK_FT_BDI WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') AND upper(EST_SUSPENDU)<>'OUI' AND trim(conforme_art) = 'OUI' 
    GROUP BY ('vr_non_conform_'||translate(lower(nvl(region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue'))
  )
)
