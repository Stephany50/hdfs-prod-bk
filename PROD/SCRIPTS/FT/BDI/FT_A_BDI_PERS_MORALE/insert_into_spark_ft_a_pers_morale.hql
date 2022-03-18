INSERT INTO AGG.SPARK_FT_A_BDI_PERS_MORALE
SELECT R.key,R.value,current_timestamp() AS insert_date,'###SLICE_VALUE###' AS EVENT_DATE
FROM (
  SELECT agg.*,new_ac.*
  FROM
    (SELECT ---  * SECTION 1: les kpi qui donnent la vue globale sur l'etat de  la base des personnes morales.
    count(*) NBR_TOTAL_PM,
    sum(case when upper(est_conforme) = 'OUI' then 1 else 0 end) NBR_TOTAL_PM_CONFORM,
    sum(case when upper(est_conforme) = 'NON' then 1 else 0 end) NBR_TOTAL_PM_AN,
    sum(case when raison_sociale_an='OUI' then 1 else 0 end) NBR_TOTAL_NS_AN,
    sum(case when cni_representant_legal_an='OUI' then 1 else 0 end) NBR_TOTAL_RL_AN,
    sum(case when rccm_an='OUI' then 1 else 0 end) NBR_TOTAL_RCCM_AN,
    sum(case when adresse_structure_an='OUI' then 1 else 0 end) NBR_TOTAL_AD_AN
    FROM TMP.TT_KYC_FT_A_PERS_MO_ST1) agg,
    (SELECT --- SECTION 2: les kpi qui donnent la vue globale sur l'etat sur les nouvelles acquisions (les champs se terminant par _new).
    sum(case when b.guid is null then 1 else 0 end) NBR_TOTAL_PM_NEW,
    sum(case when upper(A.est_conforme) = 'OUI' and b.guid is null then 1 else 0 end) NBR_TOTAL_PM_CONFORM_NEW,
    sum(case when upper(A.est_conforme) = 'NON' and b.guid is null then 1 else 0 end) NBR_TOTAL_PM_AN_NEW,
    sum(case when a.raison_sociale_an='OUI' and b.guid is null then 1 else 0 end) NBR_TOTAL_NS_AN_NEW,
    sum(case when a.cni_representant_legal_an='OUI' and b.guid is null then 1 else 0 end) NBR_TOTAL_RL_AN_NEW,
    sum(case when a.rccm_an='OUI' and b.guid is null then 1 else 0 end) NBR_TOTAL_RCCM_AN_NEW,
    sum(case when a.adresse_structure_an='OUI' and b.guid is null then 1 else 0 end) NBR_TOTAL_AD_AN_NEW,
    sum(case when upper(a.est_conforme) = 'OUI' and upper(B.est_conforme) = 'NON' then 1 else 0 end) NBR_CORRECTED_BY_DAY
    FROM (SELECT * FROM TMP.TT_KYC_FT_A_PERS_MO_ST1) A
    LEFT JOIN (SELECT * FROM TMP.TT_KYC_FT_A_PERS_MO_ST2) b ON a.guid = b.guid) new_ac
) LATERAL VIEW EXPLODE(MAP(
    'NBR_TOTAL_PM',NBR_TOTAL_PM,
    'NBR_TOTAL_PM_CONFORM',NBR_TOTAL_PM_CONFORM,
    'NBR_TOTAL_PM_AN',NBR_TOTAL_PM_AN,
    'NBR_TOTAL_NS_AN',NBR_TOTAL_NS_AN,
    'NBR_TOTAL_RL_AN',NBR_TOTAL_RL_AN,
    'NBR_TOTAL_RCCM_AN',NBR_TOTAL_RCCM_AN,
    'NBR_TOTAL_AD_AN',NBR_TOTAL_AD_AN,
    'NBR_TOTAL_PM_NEW',NBR_TOTAL_PM_NEW,
    'NBR_TOTAL_PM_CONFORM_NEW',NBR_TOTAL_PM_CONFORM_NEW,
    'NBR_TOTAL_PM_AN_NEW',NBR_TOTAL_PM_AN_NEW,
    'NBR_TOTAL_NS_AN_NEW',NBR_TOTAL_NS_AN_NEW,
    'NBR_TOTAL_RCCM_AN_NEW',NBR_TOTAL_RCCM_AN_NEW,
    'NBR_TOTAL_RL_AN_NEW',NBR_TOTAL_RL_AN_NEW,
    'NBR_TOTAL_AD_AN_NEW',NBR_TOTAL_AD_AN_NEW,
    'NBR_CORRECTED_BY_DAY',NBR_CORRECTED_BY_DAY
)) R as key, value