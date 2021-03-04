INSERT INTO AGG.SPARK_FT_A_ROAMING_RETAIL_OUT
SELECT 
    DIRECTION,TARRIF_PLAN, TYPE_ABONNE, 
    SEGMENT_CLIENT, PAYS_VISITE,
    TAP_CODE, OPERATEUR_VISITE, 
    FORFAIT_ROAMING, SUBS_CHANNEL, SERVICE_TYPE,
    SERVICE_DETAIL, DESTINATION,
    SUM(NOMBRE_ACTE), SUM(VOLUME_MINUTE), SUM(VOLUME_DATA), 
    SUM(REVENU_MAIN), '0' NUMBERS_SUBS, COUNT(DISTINCT MSISDN) UNIQUE_ROAMERS,COUNT(MSISDN) ACTIVE_ROAMERS,
    SUM(case when upper(FORFAIT_ROAMING)=upper("OCM System Default") then 0 else 1 end) ROAMERS_UNDER_BUNDLE,
    SUBSTRING(EVENT_DATE,1,7) EVENT_MONTH,
    EVENT_DATE
FROM MON.SPARK_FT_ROAMING_RETAIL_OUT 
WHERE 
    EVENT_DATE='###SLICE_VALUE###'
GROUP BY 
    DIRECTION,TARRIF_PLAN, TYPE_ABONNE, 
    SEGMENT_CLIENT, PAYS_VISITE,
    TAP_CODE, OPERATEUR_VISITE, 
    FORFAIT_ROAMING, SUBS_CHANNEL, SERVICE_TYPE,
    SERVICE_DETAIL, DESTINATION,EVENT_DATE