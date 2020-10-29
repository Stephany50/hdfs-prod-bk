SELECT 
    msisdn,
    account_formule,
    osp_contract_type,
    first_site_name,
    townname,
    administrative_region,
    categories_site,
    CURRENT_TIMESTAMP() insert_date,
    event_date
FROM 
(
    SELECT
        ACTIVATION_DATE AS EVENT_DATE,
        MSISDN,
        ACCOUNT_FORMULE,
        OSP_CONTRACT_TYPE,
        FIRST_SITE_NAME,
        CATEGORIES_SITE
    FROM MON.SPARK_FT_ACTIVATIONS_SITE_DAY
    WHERE ACTIVATION_DATE = '###SLICE_VALUE###'
        AND (
            CASE
                WHEN NVL(OSP_STATUS, CURRENT_STATUS) = 'ACTIVE' THEN 'ACTIF'
                WHEN NVL(OSP_STATUS, CURRENT_STATUS) = 'a' THEN 'ACTIF'
                ELSE NVL(OSP_STATUS, CURRENT_STATUS)
            END
        ) = 'ACTIF'
        AND FN_NNP_SIMPLE_DESTINATION(MSISDN) = 'OCM'     
) A
LEFT JOIN
(
    SELECT
        SITE_NAME,
        TOWNNAME,
        ADMINISTRATIVE_REGION,
        COMMERCIAL_REGION
    FROM
    (
        SELECT
            SITE_NAME,
            TOWNNAME,
            REGION ADMINISTRATIVE_REGION,
            COMMERCIAL_REGION,
            ROW_NUMBER() OVER (PARTITION BY SITE_NAME ORDER BY TOWNNAME) AS RANG
        FROM DIM.SPARK_DT_GSM_CELL_CODE
    ) B0
    WHERE RANG = 1
) B
ON FIRST_SITE_NAME = SITE_NAME
