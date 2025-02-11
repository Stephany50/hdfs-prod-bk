select
    EVENT_DATE,
    SITE_NAME, 
    DUREE_SORTANT DUREE_SORTANT_SECONDS,
    NBRE_TEL_SORTANT NBRE_TEL_SORTANT, 
    NBRE_SMS_SORTANT NBRE_SMS_SORTANT,
    NBRE_TEL_MTN_SORTANT, 
    NBRE_TEL_CAMTEL_SORTANT, 
    NBRE_TEL_OCM_SORTANT,
    DUREE_TEL_MTN_SORTANT DUREE_TEL_MTN_SORTANT_SECONDS,
    DUREE_TEL_CAMTEL_SORTANT DUREE_TEL_CAMTEL_SORTANT_SECONDS,
    DUREE_TEL_OCM_SORTANT DUREE_TEL_OCM_SORTANT_SECONDS,
    NBRE_SMS_MTN_SORTANT, 
    NBRE_SMS_CAMTEL_SORTANT,
    NBRE_SMS_OCM_SORTANT,
    NBRE_SMS_ZEBRA_SORTANT, 
    NBRE_TEL_NEXTTEL_SORTANT,
    DUREE_TEL_NEXTTEL_SORTANT DUREE_TEL_NEXTTEL_SORTANT_SECONDS,
    NBRE_SMS_NEXTTEL_SORTANT,
    INSERT_DATE
from AGG.SPARK_FT_A_TRAFFIC_SORTANT_NURAN
WHERE EVENT_DATE BETWEEN date_sub('###SLICE_VALUE###', 7) and date_sub('###SLICE_VALUE###', 1)
order by event_date