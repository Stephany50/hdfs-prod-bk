select
EVENT_DATE,
SITE_NAME, 
--DUREE_ENTRANT
DUREE_ENTRANT-DUREE_TEL_OCM_ENTRANT DUREE_ENTRANT_SECONDS,
NBRE_TEL_ENTRANT-NBRE_TEL_OCM_ENTRANT NBRE_TEL_ENTRANT, 
NBRE_SMS_ENTRANT-NBRE_SMS_OCM_ENTRANT NBRE_SMS_ENTRANT,
NBRE_TEL_MTN_ENTRANT, 
NBRE_TEL_CAMTEL_ENTRANT, 
--NBRE_TEL_OCM_ENTRANT,
DUREE_TEL_MTN_ENTRANT DUREE_TEL_MTN_ENTRANT_SECONDS,
DUREE_TEL_CAMTEL_ENTRANT DUREE_TEL_CAMTEL_ENTRANT_SECONDS,
--DUREE_TEL_OCM_ENTRANT,
NBRE_SMS_MTN_ENTRANT, 
NBRE_SMS_CAMTEL_ENTRANT,
--NBRE_SMS_OCM_ENTRANT,
NBRE_SMS_ZEBRA_ENTRANT, 
NBRE_TEL_NEXTTEL_ENTRANT,
DUREE_TEL_NEXTTEL_ENTRANT DUREE_TEL_NEXTTEL_ENTRANT_SECONDS,
NBRE_SMS_NEXTTEL_ENTRANT,
INSERT_DATE,
DUREE_TEL_INTERN_ENTRANT DUREE_TEL_INTERN_ENTRANT_SECONDS,
NBRE_SMS_INTERN_ENTRANT
from AGG.SPARK_FT_A_TRAFFIC_ENTRANT_AMN2 
WHERE EVENT_DATE BETWEEN date_sub('###SLICE_VALUE###', 7) and date_sub('###SLICE_VALUE###', 1)
order by event_date