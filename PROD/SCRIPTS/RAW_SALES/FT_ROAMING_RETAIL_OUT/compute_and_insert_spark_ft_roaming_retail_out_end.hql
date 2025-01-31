INSERT INTO MON.SPARK_FT_ROAMING_RETAIL_OUT
SELECT
    MSISDN,DIRECTION,TARRIF_PLAN,TYPE_ABONNE,
    SEGMENT_CLIENT, PAYS_VISITE,
    TAP_CODE,OPERATEUR_VISITE,
    FORFAIT_ROAMING,
    (CASE
        WHEN R.SUBS_CHANNEL = '32' THEN 'OMNY'
        ELSE R.SUBS_CHANNEL
    END) SUBS_CHANNEL,
    SERVICE_TYPE, 
    SERVICE_DETAIL, DESTINATION,NOMBRE_ACTE,
    VOLUME_MINUTE,VOLUME_DATA,REVENU_MAIN,EVENT_DATE
FROM (SELECT
    MSISDN,DIRECTION,TARRIF_PLAN,TYPE_ABONNE,
    SEGMENT_CLIENT, PAYS_VISITE,
    TAP_CODE,D.operateur OPERATEUR_VISITE,D.code_operateur, 
    FORFAIT_ROAMING,SUBS_CHANNEL, SERVICE_TYPE, 
    SERVICE_DETAIL, DESTINATION,NOMBRE_ACTE,
    VOLUME_MINUTE,VOLUME_DATA,REVENU_MAIN,EVENT_DATE,
    row_number() over(partition by  RANG,MSISDN,DIRECTION,TARRIF_PLAN,TYPE_ABONNE,
    SEGMENT_CLIENT, PAYS_VISITE,
    TAP_CODE,FORFAIT_ROAMING,SUBS_CHANNEL, SERVICE_TYPE, 
    SERVICE_DETAIL, DESTINATION,NOMBRE_ACTE,
    VOLUME_MINUTE,VOLUME_DATA,REVENU_MAIN,EVENT_DATE 
    order by length(upper(trim(D.code_operateur))) DESC nulls last) as RANG_OP
FROM TMP.SPARK_FT_ROAMING_RETAIL_OUT T
LEFT JOIN DIM.SPARK_DT_REF_OPERATEURS D ON substr(T.OPERATEUR_VISITE,1,length(trim(D.code_operateur))) = trim(D.code_operateur)) R
WHERE RANG_OP =1