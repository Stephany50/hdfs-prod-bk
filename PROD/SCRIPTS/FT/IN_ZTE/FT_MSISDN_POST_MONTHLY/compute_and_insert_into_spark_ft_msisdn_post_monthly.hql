INSERT INTO MON.SPARK_FT_MSISDN_POST_MONTHLY PARTITION(MOIS)
SELECT
    (CASE WHEN POST.VOICE_MSISDN IS NOT NULL THEN POST.VOICE_MSISDN ELSE POST.DATA_MSISDN END) MSISDN,
    REF_POST.PAYING_ACCOUNT,
    REF_POST.CLIENT_ACCOUNT,
    REF_POST.PAYING_ACCOUNT_NAME,
    NVL(POST.REVENUE, 0) MAIN_REVENU_TOTAL,
    NVL(POST.PROMO_REVENUE, 0) PROMO_REVENU_TOTAL,
    NVL(POST.REVENUE_ONNET, 0) MAIN_REVENUE_ONNET,
    NVL(POST.REVENUE_OFFNET, 0) MAIN_REVENUE_OFFNET,
    NVL(POST.REVENUE_INT, 0) MAIN_REVENUE_INT,
    NVL(POST.PROMO_REVENUE_ONNET, 0) PROMO_REVENUE_ONNET,
    NVL(POST.PROMO_REVENUE_OFFNET, 0) PROMO_REVENUE_OFFNET,
    NVL(POST.PROMO_REVENUE_INT, 0) PROMO_REVENUE_INT,
    NVL(POST.VOIX_ONNET, 0) MAIN_VOIX_ONNET,
    NVL(POST.VOIX_OFFNET, 0) MAIN_VOIX_OFFNET,
    NVL(POST.VOIX_INT, 0) MAIN_VOIX_INT,
    NVL(POST.VOIX_ROAM, 0) MAIN_VOIX_ROAM,
    NVL(POST.SMS_ONNET, 0) MAIN_SMS_ONNET,
    NVL(POST.SMS_OFFNET, 0) MAIN_SMS_OFFNET,
    NVL(POST.SMS_INT, 0) MAIN_SMS_INT,
    NVL(POST.SMS_ROAM, 0) MAIN_SMS_ROAM,
    NVL(POST.DAT_FIXE, 0) MAIN_DAT_FIXE,
    NVL(POST.PROMO_VOIX_ONNET, 0) PROMO_VOIX_ONNET,
    NVL(POST.PROMO_VOIX_OFFNET, 0) PROMO_VOIX_OFFNET,
    NVL(POST.PROMO_VOIX_INT, 0) PROMO_VOIX_INT,
    NVL(POST.PROMO_VOIX_ROAM, 0) PROMO_VOIX_ROAM,
    NVL(POST.PROMO_SMS_ONNET, 0) PROMO_SMS_ONNET,
    NVL(POST.PROMO_SMS_OFFNET, 0) PROMO_SMS_OFFNET,
    NVL(POST.PROMO_SMS_INT, 0) PROMO_SMS_INT,
    NVL(POST.PROMO_SMS_ROAM, 0) PROMO_SMS_ROAM,
    NVL(POST.PROMO_DAT_FIXE, 0) PROMO_DAT_FIXE,
    NVL(POST.SVA, 0) SVA,
    POST.OFFRE OFFRE,
    NVL(POST.SOU_ONNET/60, 0) MOU_ONNET,
    NVL(POST.SOU_OFFNET/60, 0) MOU_OFFNET,
    NVL(POST.SOU_INTER/60, 0) MOU_INT,
    NVL(POST.BYTES_SENT, 0) VOLUME_DATA_EMIS,
    NVL(POST.BYTES_RECEIVED, 0) VOLUME_DATA_RECU,
    NVL(POST.VOLUME_BUNDLE, 0) VOLUME_BUNDLE,
    NVL(POST.VOLUME_MAIN, 0) VOLUME_MAIN,
    NVL(POST.DATA_COST, 0) REVENU_DATA_MAIN,
    NVL(POST.PROMO_COST, 0) REVENU_DATA_PROMO,
    NVL(POST.NBR_JOUR_ACTIVITE_VOICE, 0) NBR_JOUR_ACTIVITE_VOIX,
    NVL(POST.NBR_JOUR_ACTIVITE_DATA, 0) NBR_JOUR_ACTIVITE_DATA,
    CURRENT_TIMESTAMP INSERT_DATE,
    (CASE WHEN POST.VOICE_MONTH IS NOT NULL THEN POST.VOICE_MONTH ELSE POST.DATA_MONTH END) MOIS
FROM (
         SELECT *
         FROM
             (
                 SELECT

                     CHARGED_PARTY VOICE_MSISDN,
                     SUM(MAIN_RATED_AMOUNT) REVENUE,
                     SUM(PROMO_RATED_AMOUNT) PROMO_REVENUE,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('ONNET','ONNETFREE','OCM_D', 'MVNO') THEN MAIN_RATED_AMOUNT ELSE 0 END) REVENUE_ONNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('MTN','MTN_D', 'NEXTTEL', 'CAM', 'CAM_D') THEN MAIN_RATED_AMOUNT ELSE 0 END) REVENUE_OFFNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('INT') THEN MAIN_RATED_AMOUNT ELSE 0 END) REVENUE_INT,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('ONNET','ONNETFREE','OCM_D', 'MVNO') THEN PROMO_RATED_AMOUNT ELSE 0 END) PROMO_REVENUE_ONNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('MTN','MTN_D', 'NEXTTEL', 'CAM', 'CAM_D') THEN PROMO_RATED_AMOUNT ELSE 0 END) PROMO_REVENUE_OFFNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('INT') THEN PROMO_RATED_AMOUNT ELSE 0 END) PROMO_REVENUE_INT,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('ONNET','ONNETFREE','OCM_D', 'MVNO') AND (SERVICE_CODE LIKE '%TEL%' OR UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' OR UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%') THEN MAIN_RATED_AMOUNT ELSE 0 END) VOIX_ONNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('MTN','MTN_D', 'NEXTTEL', 'CAM', 'CAM_D') AND (SERVICE_CODE LIKE '%TEL%' OR UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' OR UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%') THEN MAIN_RATED_AMOUNT ELSE 0 END) VOIX_OFFNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('INT') AND (SERVICE_CODE LIKE '%TEL%' OR UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' OR UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%') THEN MAIN_RATED_AMOUNT ELSE 0 END) VOIX_INT,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('OCRMG', 'TCRMG') AND (SERVICE_CODE LIKE '%TEL%' OR UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' OR UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%') THEN MAIN_RATED_AMOUNT ELSE 0 END) VOIX_ROAM,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('ONNET','ONNETFREE','OCM_D', 'MVNO') AND UPPER(SERVICE_CODE) IN ('SMS', 'SMSMO','SMSRMG') THEN MAIN_RATED_AMOUNT ELSE 0 END) SMS_ONNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('MTN','MTN_D', 'NEXTTEL', 'CAM', 'CAM_D') AND UPPER(SERVICE_CODE) IN ('SMS', 'SMSMO','SMSRMG') THEN MAIN_RATED_AMOUNT ELSE 0 END) SMS_OFFNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('INT') AND UPPER(SERVICE_CODE) IN ('SMS', 'SMSMO','SMSRMG') THEN MAIN_RATED_AMOUNT ELSE 0 END) SMS_INT,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('OCRMG', 'TCRMG') AND UPPER(SERVICE_CODE) IN ('SMS', 'SMSMO','SMSRMG') THEN MAIN_RATED_AMOUNT ELSE 0 END) SMS_ROAM,
                     SUM(CASE WHEN SERVICE_CODE LIKE '%DAT%' THEN MAIN_RATED_AMOUNT ELSE 0 END) DAT_FIXE,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('ONNET','ONNETFREE','OCM_D', 'MVNO') AND (SERVICE_CODE LIKE '%TEL%' OR UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' OR UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%') THEN PROMO_RATED_AMOUNT ELSE 0 END) PROMO_VOIX_ONNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('MTN','MTN_D', 'NEXTTEL', 'CAM', 'CAM_D') AND (SERVICE_CODE LIKE '%TEL%' OR UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' OR UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%') THEN PROMO_RATED_AMOUNT ELSE 0 END) PROMO_VOIX_OFFNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('INT') AND (SERVICE_CODE LIKE '%TEL%' OR UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' OR UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%') THEN PROMO_RATED_AMOUNT ELSE 0 END) PROMO_VOIX_INT,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('OCRMG', 'TCRMG') AND (SERVICE_CODE LIKE '%TEL%' OR UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' OR UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%') THEN PROMO_RATED_AMOUNT ELSE 0 END) PROMO_VOIX_ROAM,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('ONNET','ONNETFREE','OCM_D', 'MVNO') AND UPPER(SERVICE_CODE) IN ('SMS', 'SMSMO','SMSRMG') THEN PROMO_RATED_AMOUNT ELSE 0 END) PROMO_SMS_ONNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('MTN','MTN_D', 'NEXTTEL', 'CAM', 'CAM_D') AND UPPER(SERVICE_CODE) IN ('SMS', 'SMSMO','SMSRMG') THEN PROMO_RATED_AMOUNT ELSE 0 END) PROMO_SMS_OFFNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('INT') AND UPPER(SERVICE_CODE) IN ('SMS', 'SMSMO','SMSRMG') THEN PROMO_RATED_AMOUNT ELSE 0 END) PROMO_SMS_INT,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('OCRMG', 'TCRMG') AND UPPER(SERVICE_CODE) IN ('SMS', 'SMSMO','SMSRMG') THEN PROMO_RATED_AMOUNT ELSE 0 END) PROMO_SMS_ROAM,
                     SUM(CASE WHEN SERVICE_CODE LIKE '%DAT%' THEN PROMO_RATED_AMOUNT ELSE 0 END) PROMO_DAT_FIXE,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('VAS', 'EMERG') THEN MAIN_RATED_AMOUNT ELSE 0 END) SVA,
                     MAX(COMMERCIAL_PROFILE) OFFRE,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('ONNET','ONNETFREE','OCM_D', 'MVNO') THEN RATED_DURATION ELSE 0 END) SOU_ONNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('MTN','MTN_D', 'NEXTTEL', 'CAM', 'CAM_D') THEN RATED_DURATION ELSE 0 END) SOU_OFFNET,
                     SUM(CASE WHEN CALL_DESTINATION_CODE IN ('INT') THEN RATED_DURATION ELSE 0 END) SOU_INTER,
                     COUNT(DISTINCT TIMESTAMP(TRANSACTION_DATE)) NBR_JOUR_ACTIVITE_VOICE,
                     DATE_FORMAT(TRANSACTION_DATE,'yyyy-MM') VOICE_MONTH
                 FROM MON.SPARK_FT_BILLED_TRANSACTION_POSTPAID
                 WHERE to_date(TRANSACTION_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
                 GROUP BY DATE_FORMAT(TRANSACTION_DATE,'yyyy-MM'), CHARGED_PARTY
             ) VOICE
                 FULL OUTER JOIN (
                 SELECT
                     SERVED_PARTY_MSISDN DATA_MSISDN,
                     SUM(BYTES_SENT) BYTES_SENT,
                     SUM(BYTES_RECEIVED) BYTES_RECEIVED,
                     SUM(BUNDLE_BYTES_USED_VOLUME) VOLUME_BUNDLE,
                     SUM(BYTES_SENT + BYTES_RECEIVED - BUNDLE_BYTES_USED_VOLUME) VOLUME_MAIN,
                     SUM(MAIN_COST) DATA_COST,
                     SUM(PROMO_COST) PROMO_COST,
                     COUNT(DISTINCT SESSION_DATE) NBR_JOUR_ACTIVITE_DATA,
                     DATE_FORMAT(SESSION_DATE, 'yyyy-MM') DATA_MONTH
                 FROM MON.SPARK_FT_CRA_GPRS_POST
                 WHERE to_date(SESSION_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
                 GROUP BY DATE_FORMAT(SESSION_DATE, 'yyyy-MM'), SERVED_PARTY_MSISDN
             ) DAT ON (VOICE.VOICE_MONTH = DAT.DATA_MONTH AND VOICE.VOICE_MSISDN = DAT.DATA_MSISDN)
     ) POST
         LEFT JOIN (SELECT * FROM DIM.DT_REF_MSISDN_POSTPAID WHERE EVENT_MONTH = (SELECT MAX(EVENT_MONTH) FROM DIM.DT_REF_MSISDN_POSTPAID)) REF_POST ON (POST.VOICE_MSISDN = REF_POST.MSISDN OR POST.DATA_MSISDN = REF_POST.MSISDN)
