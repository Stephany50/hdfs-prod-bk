INSERT INTO SPOOL.SPOOL_DATA_VIA_OM
SELECT
    SERVED_PARTY_MSISDN BENEFICIAIRE
    , NOM NOM_BENEFICIAIRE
    , PRENOM PRENOM_BENEFICIAIRE
    , SUBSCRIPTION_SERVICE_DETAILS BUNDLE_NAME
    , PRIX BDLE_COST
    , COUNT(*) NBER_PURCHASE
    , CURRENT_TIMESTAMP() INSERT_DATE
    , '###SLICE_VALUE###' TRANSACTION_DATE
FROM (
    SELECT
        SERVED_PARTY_MSISDN
        , SUBSCRIPTION_SERVICE_DETAILS
    FROM MON.SPARK_FT_SUBSCRIPTION
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND (subscription_channel='32' or (upper(subscription_channel) like '%GOS%SDP%' and upper(SUBSCRIPTION_SERVICE_DETAILS) like '%MY%WAY%DIGITAL%') )
) A
LEFT JOIN
DIM.DT_BASE_IDENTIFICATION B
ON A.SERVED_PARTY_MSISDN=B.MSISDN
LEFT JOIN
DIM.DT_CBM_REF_SOUSCRIPTION_PRICE C
ON UPPER(A.SUBSCRIPTION_SERVICE_DETAILS) = UPPER(C.BDLE_NAME)
GROUP BY SERVED_PARTY_MSISDN
    , NOM
    , PRENOM
    , SUBSCRIPTION_SERVICE_DETAILS
    , PRIX
    --, BENEFIT_RESULT_VALUE_LIST
