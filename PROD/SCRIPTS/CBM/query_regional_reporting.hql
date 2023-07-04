INSERT INTO MON.SPARK_FT_CBM_REGIONAL_REPORTING
SELECT
    CASE WHEN region IS NOT NULL THEN region
        ELSE "z. Non localise"
        END AS region,
    CASE WHEN type_zone2 IS NOT NULL THEN type_zone2
        ELSE "z. Non localise"
        END AS type_zone2,
    CASE WHEN zone_pmo IS NOT NULL THEN zone_pmo
        ELSE "z. Non localise"
        END AS zone_pmo,
    CASE WHEN site_name IS NOT NULL THEN site_name
        ELSE "z. Non localise"
        END AS site_name,
    CASE WHEN townname IS NOT NULL THEN townname
        ELSE "z. Non localise"
        END AS townname,
    CASE WHEN segment_value IS NOT NULL THEN segment_value
        ELSE "z. Gross Add"
        END AS segment_value,
    CASE WHEN tenure IS NOT NULL THEN tenure
        ELSE "z. Gross Add"
        END AS tenure,
    SUM(arpu_data) AS arpu_data,
    SUM(arpu_onet) AS arpu_onet,
    SUM(arpu_ofnet) AS arpu_ofnet,
    SUM(arpu_inter) AS arpu_inter, 
    SUM(PAYG_VOIX) AS PAYG_VOIX,
    SUM(bdles_data) AS bdles_data,
    SUM(bdles_ofnet) AS bdles_ofnet,
    SUM(bdles_onet) AS bdles_onet,
    SUM(bdles_inter) AS bdles_inter,
    SUM(A.volume_data_in) AS volume1,
    SUM(A.mou2) AS mou1,
    SUM(CASE WHEN A.mou2 > 0 THEN 1
            ELSE 0
            END) AS Voice_users_day,
    SUM(CASE WHEN A.volume_data_in > 1 THEN 1
            ELSE 0
            END) AS Data_users_day,
    SUM(CASE WHEN M.mou2 > 0 THEN 1
            ELSE 0
            END) AS Voice_users_mtd,
    SUM(CASE WHEN M.volume_data_in > 1 THEN 1
            ELSE 0
            END) AS Data_users_mtd,
    '###SLICE_VALUE###' AS event_date 
FROM 
(
    SELECT
        msisdn,
        SUM(NVL(bytes_data, 0) / (1024 * 1024)) AS volume_data_in,
        SUM(NVL(mou, 0) / (1*1)) AS mou2
    FROM MON.SPARK_FT_CBM_ARPU_MOU
    WHERE event_date >= DATE_FORMAT('###SLICE_VALUE###','yyyy-MM-01')
    AND event_date <= '###SLICE_VALUE###'
    GROUP BY msisdn
) M
LEFT JOIN
(
    SELECT 
        msisdn,
        arpu_data,
        arpu_onet,
        arpu_ofnet,
        arpu_inter, 
        PAYG_VOIX,
        bdles_data,
        bdles_ofnet,
        bdles_onet,
        bdles_inter,
        NVL(bytes_data, 0) / (1024 * 1024) AS volume_data_in,
        NVL(mou, 0) / (1*1) AS mou2
    FROM MON.SPARK_FT_CBM_ARPU_MOU
    WHERE event_date = '###SLICE_VALUE###'
) A
ON GET_NNP_MSISDN_9DIGITS(A.msisdn) = GET_NNP_MSISDN_9DIGITS(M.msisdn)
LEFT JOIN
(
    SELECT DISTINCT
        msisdn,
        type_zone2,
        region,
        zone_pmo,
        site_name,
        townname
    FROM MON.SPARK_FT_LOCALISATION_DATA_USERS
    WHERE event_date = '###SLICE_VALUE###'
) B
ON GET_NNP_MSISDN_9DIGITS(A.msisdn) = GET_NNP_MSISDN_9DIGITS(B.msisdn)
LEFT JOIN 
(
    SELECT
        msisdn,
        segment_value 
    FROM MON.SPARK_FT_CBM_SEGMENT_VALUE
    WHERE event_date = DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM-01')
) C 
ON GET_NNP_MSISDN_9DIGITS(A.msisdn) = GET_NNP_MSISDN_9DIGITS(C.msisdn)
LEFT JOIN 
(
    SELECT 
        access_key AS msisdn,
        activation_date,
        CASE WHEN DATEDIFF(CURRENT_DATE(), TO_DATE(ACTIVATION_DATE)) > 365 THEN "4. sup.12mois"
            WHEN DATEDIFF(CURRENT_DATE(), TO_DATE(ACTIVATION_DATE)) > 91 THEN "3. inf.12mois"
            WHEN DATEDIFF(CURRENT_DATE(), TO_DATE(ACTIVATION_DATE)) > 30 THEN "2. inf.3mois"
            WHEN DATEDIFF(CURRENT_DATE(), TO_DATE(ACTIVATION_DATE)) >= 0 THEN "1. inf. 1mois"
            ELSE "5. Others"
        END AS tenure
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
    WHERE event_date = DATE_ADD('###SLICE_VALUE###', 1)
) D 
ON GET_NNP_MSISDN_9DIGITS(A.msisdn) = GET_NNP_MSISDN_9DIGITS(D.msisdn)
GROUP BY 
    region,
    type_zone2,
    zone_pmo,
    site_name,
    townname,
    segment_value,
    tenure