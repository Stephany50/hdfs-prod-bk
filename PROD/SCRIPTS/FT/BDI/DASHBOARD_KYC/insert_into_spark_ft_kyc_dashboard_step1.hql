--Calcul des KPIs donnant une vue Global sur l'etat de la Base.
insert into AGG.SPARK_FT_A_KYC_DASHBOARD
SELECT type_personne,region,type_piece,R.key,R.value,current_timestamp() AS insert_date,'###SLICE_VALUE###' AS EVENT_DATE
FROM (SELECT
      (case when type_personne in ('MAJEUR','PP') then 'MAJEUR' when type_personne in ('MINEUR') then 'MINEUR' else 'AUTRE' end) type_personne,
      (translate(UPPER(nvl(A.region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')) region,
      type_piece,
      count(distinct A.msisdn) vg_total,
      sum(case when upper(EST_SUSPENDU)<>'OUI' then 1 else 0 end) vg_total_actif,
      sum(case when upper(EST_SUSPENDU)='OUI' then 1 else 0 end) vg_total_suspendu,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and trim(conforme_art) = 'OUI' then 1 else 0 end) vg_total_conform,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and trim(conforme_art) = 'NON' then 1 else 0 end) vg_total_non_conform,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and trim(est_client_vip) = 'OUI' then 1 else 0 end) vg_total_vip,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and trim(est_client_vip) = 'OUI' and trim(conforme_art) = 'OUI' then 1 else 0 end) vg_total_vip_conform,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and trim(est_client_vip) = 'OUI' and trim(conforme_art) = 'NON' then 1 else 0 end) vg_total_vip_non_conform,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and trim(statut_derogation) = 'OUI' then 1 else 0 end) vg_total_derogation,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and to_date(date_activation) = TO_DATE('###SLICE_VALUE###') then 1 else 0 end) vg_total_activations_day,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and to_date(date_activation) = TO_DATE('###SLICE_VALUE###') and trim(conforme_art)='OUI' then 1 else 0 end) vg_total_activations_day_conform,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and to_date(date_activation) = TO_DATE('###SLICE_VALUE###') and trim(conforme_art)='NON' then 1 else 0 end) vg_total_activations_day_non_conform,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and trim(multi_sim) = 'OUI' then 1 else 0 end) vg_total_multisim,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and trim(cni_expire) = 'OUI' then 1 else 0 end) vg_total_cni_expire,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and (cast(months_between('###SLICE_VALUE###', date_expiration) as int) >= 6) then 1 else 0 end) vg_total_cni_expire_6mois,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and trim(est_present_om) = 'OUI' then 1 else 0 end) vg_total_compte_om,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and trim(est_present_om) ='NON' then 1 else 0 end) vg_total_non_compte_om,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and trim(est_present_om) = 'OUI' and trim(conforme_art)='OUI' then 1 else 0 end) vg_total_compte_om_conform,
      sum(case when upper(EST_SUSPENDU)<>'OUI' and trim(est_present_om) = 'OUI' and trim(conforme_art)='NON' then 1 else 0 end) vg_total_compte_om_non_conform,
      sum(case when TO_DATE(date_activation) = TO_DATE('###SLICE_VALUE###') then 1 else 0 end) vg_total_nvl_acquisitions,
      sum(case when TO_DATE(date_activation) = TO_DATE('###SLICE_VALUE###') and upper(EST_SUSPENDU)<>'OUI' then 1 else 0 end) vg_total_nvl_acquisitions_actives,
      sum(case when TO_DATE(date_activation) = TO_DATE('###SLICE_VALUE###') and upper(EST_SUSPENDU)='OUI' then 1 else 0 end) vg_total_nvl_acquisitions_inactives,      
      sum(case when TO_DATE(date_activation) = TO_DATE('###SLICE_VALUE###') and upper(EST_SUSPENDU)<>'OUI' and trim(conforme_art)='OUI' then 1 else 0 end) vg_total_nvl_acquisitions_conformes,
      sum(case when TO_DATE(date_activation) = TO_DATE('###SLICE_VALUE###') and upper(EST_SUSPENDU)<>'OUI' and trim(conforme_art)='NON' then 1 else 0 end) vg_total_nvl_acquisitions_non_conformes,
      --Vue HLR
      count(distinct B.msisdn) vhlr_total_abonnes,
      sum(case when trim(statut_hlr) = 'ACTIF' then 1 else 0 end) vhlr_total_actif_abonnes,
      sum(case when trim(statut_hlr) in ('SUSPENDU_SORTANT') then 1 else 0 end) vhlr_total_suspendu_sortant,
      sum(case when trim(statut_hlr) in ('SUSPENDU_ENTRANT') then 1 else 0 end) vhlr_total_suspendu_entrant,
      sum(case when trim(statut_hlr) = 'SUSPENDU' then 1 else 0 end) vhlr_total_suspendu_sortant_entrant,
      --Vue ZSMART
      count(distinct C.msisdn) vzm_total_abonnes,
      sum(case when trim(statut_zm) = 'Actif' then 1 else 0 end) vzm_total_actif_abonnes,
      sum(case when trim(statut_zm) = 'Suspendu' then 1 else 0 end) vzm_total_suspendu_abonnes
      FROM (SELECT * FROM MON.SPARK_FT_KYC_BDI_PP WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###')) A
      RIGHT JOIN (SELECT msisdn,statut statut_hlr FROM MON.SPARK_FT_ABONNE_HLR WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') group by msisdn,statut) B on fn_format_msisdn_to_9digits(A.msisdn)=fn_format_msisdn_to_9digits(B.msisdn)
      RIGHT JOIN (SELECT msisdn,statut statut_zm FROM MON.SPARK_FT_KYC_ZSMART WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') group by msisdn,statut) C on fn_format_msisdn_to_9digits(A.msisdn)=fn_format_msisdn_to_9digits(C.msisdn)
      GROUP BY (case when type_personne in ('MAJEUR','PP') then 'MAJEUR' when type_personne in ('MINEUR') then 'MINEUR' else 'AUTRE' end),
      (translate(UPPER(nvl(A.region_administrative,'UNKNOWN')), 'áéíóúê', 'aeioue')) ,type_piece
) LATERAL VIEW EXPLODE(MAP(
    'vg_total',vg_total,
    'vg_total_actif',vg_total_actif,
    'vg_total_suspendu',vg_total_suspendu,
    'vg_total_conform',vg_total_conform,
    'vg_total_non_conform',vg_total_non_conform,
    'vg_total_vip',vg_total_vip,
    'vg_total_vip_conform',vg_total_vip_conform,
    'vg_total_vip_non_conform',vg_total_vip_non_conform,
    'vg_total_derogation',vg_total_derogation,
    'vg_total_activations_day',vg_total_activations_day,
    'vg_total_activations_day_conform',vg_total_activations_day_conform,
    'vg_total_activations_day_non_conform',vg_total_activations_day_non_conform,
    'vg_total_multisim',vg_total_multisim,
    'vg_total_cni_expire',vg_total_cni_expire,
    'vg_total_cni_expire_6mois',vg_total_cni_expire_6mois,
    'vg_total_compte_om',vg_total_compte_om,
    'vg_total_non_compte_om',vg_total_non_compte_om,
    'vg_total_compte_om_conform',vg_total_compte_om_conform,
    'vg_total_compte_om_non_conform',vg_total_compte_om_non_conform,
    'vg_total_nvl_acquisitions',vg_total_nvl_acquisitions,
    'vg_total_nvl_acquisitions_actives',vg_total_nvl_acquisitions_actives,
    'vg_total_nvl_acquisitions_inactives',vg_total_nvl_acquisitions_inactives,
    'vg_total_nvl_acquisitions_conformes',vg_total_nvl_acquisitions_conformes,
    'vg_total_nvl_acquisitions_non_conformes',vg_total_nvl_acquisitions_non_conformes,
    'vhlr_total_abonnes',vhlr_total_abonnes,
    'vhlr_total_actif_abonnes',vhlr_total_actif_abonnes,
    'vhlr_total_suspendu_sortant',vhlr_total_suspendu_sortant,
    'vhlr_total_suspendu_entrant',vhlr_total_suspendu_entrant,
    'vhlr_total_suspendu_sortant_entrant',vhlr_total_suspendu_sortant_entrant,
    'vzm_total_abonnes',vzm_total_abonnes,
    'vzm_total_actif_abonnes',vzm_total_actif_abonnes,
    'vzm_total_suspendu_abonnes',vzm_total_suspendu_abonnes
)) R as key, value