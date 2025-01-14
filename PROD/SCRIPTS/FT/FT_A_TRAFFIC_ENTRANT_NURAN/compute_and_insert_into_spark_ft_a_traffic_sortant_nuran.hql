INSERT INTO AGG.SPARK_FT_A_TRAFFIC_SORTANT_NURAN
SELECT
        SITE_NAME, 
        SUM(DUREE_SORTANT) AS DUREE_SORTANT,
        SUM(NBRE_TEL_SORTANT) AS NBRE_TEL_SORTANT, 
        SUM(NBRE_SMS_SORTANT) AS NBRE_SMS_SORTANT,
        SUM(NBRE_TEL_MTN_SORTANT) AS NBRE_TEL_MTN_SORTANT, 
        SUM(NBRE_TEL_CAMTEL_SORTANT) AS NBRE_TEL_CAMTEL_SORTANT, 
        SUM(NBRE_TEL_OCM_SORTANT) AS NBRE_TEL_OCM_SORTANT, 
        SUM(DUREE_TEL_MTN_SORTANT) AS DUREE_TEL_MTN_SORTANT,
        SUM(DUREE_TEL_CAMTEL_SORTANT) AS DUREE_TEL_CAMTEL_SORTANT, 
        SUM(DUREE_TEL_OCM_SORTANT) AS DUREE_TEL_OCM_SORTANT, 
        SUM(NBRE_SMS_MTN_SORTANT) AS NBRE_SMS_MTN_SORTANT, 
        SUM(NBRE_SMS_CAMTEL_SORTANT) AS NBRE_SMS_CAMTEL_SORTANT, 
        SUM(NBRE_SMS_OCM_SORTANT) AS NBRE_SMS_OCM_SORTANT, 
        SUM(NBRE_SMS_ZEBRA_SORTANT) AS NBRE_SMS_ZEBRA_SORTANT, 
        SUM(NBRE_TEL_NEXTTEL_SORTANT) AS NBRE_TEL_NEXTTEL_SORTANT, 
        SUM(DUREE_TEL_NEXTTEL_SORTANT) AS DUREE_TEL_NEXTTEL_SORTANT, 
        SUM(NBRE_SMS_NEXTTEL_SORTANT) AS NBRE_SMS_NEXTTEL_SORTANT,
        CURRENT_TIMESTAMP AS INSERT_DATE,
        '###SLICE_VALUE###' AS EVENT_DATE
FROM MON.SPARK_FT_CELL_TRAFIC_DAYLY a
JOIN (select (case when length(ci) =2 then concat('000',ci)
when length(ci) =3 then concat('00',ci)
when length(ci) =4 then concat('0',ci) else ci end) ci, site_name
from dim.dt_ci_lac_site_nuran) b
ON SUBSTR(MS_LOCATION,-5,5) = b.CI
WHERE a.EVENT_DATE = '###SLICE_VALUE###'
GROUP BY b.SITE_NAME