INSERT INTO AGG.SPARK_FT_A_BDI_PERS_MORALE
SELECT *
FROM
(SELECT 
NBR_TOTAL_PM,NBR_TOTAL_PM_CONFORM,NBR_TOTAL_PM_AN,NBR_TOTAL_NS_AN,NBR_TOTAL_RL_AN,NBR_TOTAL_RCCM_AN,NBR_TOTAL_AD_AN,
NBR_TOTAL_PM_NEW,NBR_TOTAL_PM_CONFORM_NEW,NBR_TOTAL_PM_AN_NEW,NBR_TOTAL_NS_AN_NEW,NBR_TOTAL_RL_AN_NEW,NBR_TOTAL_RCCM_AN_NEW,NBR_TOTAL_AD_AN_NEW,
0 COMPTE,0 NBR_TOTAL_PM_RECAP_AN,0 NBR_TOTAL_NS_RECAP_AN,0 NBR_TOTAL_RL_RECAP_AN,0 NBR_TOTAL_RCCM_RECAP_AN,0 NBR_TOTAL_AD_RECAP_AN
,current_timestamp INSERT_DATE,'###SLICE_VALUE###' EVENT_DATE
FROM
(SELECT 
    count(*) NBR_TOTAL_PM,
    sum(case when raison_sociale_an='NON' and rccm_an='NON' and cni_representant_legal_an='NON' and adresse_structure_an='NON' then 1 else 0 end) NBR_TOTAL_PM_CONFORM,
    sum(case when not(raison_sociale_an='NON' and rccm_an='NON' and cni_representant_legal_an='NON' and adresse_structure_an='NON') then 1 else 0 end) NBR_TOTAL_PM_AN,
    sum(case when raison_sociale_an='OUI' then 1 else 0 end) NBR_TOTAL_NS_AN,
    sum(case when cni_representant_legal_an='OUI' then 1 else 0 end) NBR_TOTAL_RL_AN,
    sum(case when rccm_an='OUI' then 1 else 0 end) NBR_TOTAL_RCCM_AN,
    sum(case when adresse_structure_an='OUI' then 1 else 0 end) NBR_TOTAL_AD_AN
FROM MON.SPARK_FT_BDI_PERS_MORALE a WHERE event_date = '###SLICE_VALUE###') agg,
(SELECT 
    count(*) NBR_TOTAL_PM_NEW,
    sum(case when raison_sociale_an='NON' and rccm_an='NON' and cni_representant_legal_an='NON' and adresse_structure_an='NON' then 1 else 0 end) NBR_TOTAL_PM_CONFORM_NEW,
    sum(case when not(raison_sociale_an='NON' and rccm_an='NON' and cni_representant_legal_an='NON' and adresse_structure_an='NON') then 1 else 0 end) NBR_TOTAL_PM_AN_NEW,
    sum(case when raison_sociale_an='OUI' then 1 else 0 end) NBR_TOTAL_NS_AN_NEW,
    sum(case when cni_representant_legal_an='OUI' then 1 else 0 end) NBR_TOTAL_RL_AN_NEW,
    sum(case when rccm_an='OUI' then 1 else 0 end) NBR_TOTAL_RCCM_AN_NEW,
    sum(case when adresse_structure_an='OUI' then 1 else 0 end) NBR_TOTAL_AD_AN_NEW
FROM (SELECT * FROM MON.SPARK_FT_BDI_PERS_MORALE A WHERE EVENT_DATE = '###SLICE_VALUE###') A
LEFT JOIN (SELECT compte_client FROM MON.SPARK_FT_BDI_PERS_MORALE A WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)) B ON A.compte_client = B.compte_client
WHERE B.compte_client is null
) new_ac)
UNION
(SELECT
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    (case when COMPTE_CLIENT like '4.%' then 'COMPTE 4.X' when COMPTE_CLIENT like '1.%' then 'COMPTE 1.X' else 'AUTRE' end) COMPTE,
    count(*) NBR_TOTAL_PM_RECAP_AN,
    sum(case when raison_sociale_an='OUI' then 1 else 0 end) NBR_TOTAL_NS_RECAP_AN,
    sum(case when cni_representant_legal_an='OUI' then 1 else 0 end) NBR_TOTAL_RL_RECAP_AN,
    sum(case when rccm_an='OUI' then 1 else 0 end) NBR_TOTAL_RCCM_RECAP_AN,
    sum(case when adresse_structure_an='OUI' then 1 else 0 end) NBR_TOTAL_AD_RECAP_AN,
current_timestamp INSERT_DATE,'###SLICE_VALUE###' EVENT_DATE
FROM MON.SPARK_FT_BDI_PERS_MORALE A
WHERE EVENT_DATE = '###SLICE_VALUE###' and not(raison_sociale_an='NON' and rccm_an='NON' and cni_representant_legal_an='NON' and adresse_structure_an='NON')
GROUP BY (case when COMPTE_CLIENT like '4.%' then 'COMPTE 4.X' when COMPTE_CLIENT like '1.%' then 'COMPTE 1.X' else 'AUTRE' end)
)