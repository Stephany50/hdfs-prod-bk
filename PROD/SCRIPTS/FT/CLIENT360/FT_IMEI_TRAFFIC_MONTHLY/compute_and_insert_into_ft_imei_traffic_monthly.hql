
-- @TRAITEMENT :: RECUPERER LES DONNÉES  ET  INSERER LES DONNÉES AGREGÉES <FT_CONSO_MSISDN_MONTH>
--
INSERT INTO MON.FT_IMEI_TRAFFIC_MONTHLY
SELECT  imei, imsi, msisdn
    , nvl(b.PROFILE_CODE,'') profile_code
    , nvl(b.PROFILE_NAME,'') profile_name
    , NVL (a.LANG, 'FR') language
    , (CASE
        WHEN NVL(a.OSP_STATUS, nvl(a.CURRENT_STATUS,'')) ='ACTIVE' THEN 'ACTIVE'
        WHEN NVL(a.OSP_STATUS, nvl(a.CURRENT_STATUS,'')) ='a' THEN 'ACTIVE'
        WHEN NVL(a.OSP_STATUS, nvl(a.CURRENT_STATUS,'')) ='d' THEN 'DEACT'
        WHEN NVL(a.OSP_STATUS, nvl(a.CURRENT_STATUS,'')) ='s' THEN 'INACTIVE'
        WHEN NVL(a.OSP_STATUS, nvl(a.CURRENT_STATUS,'')) ='DEACTIVATED' THEN 'DEACT'
        WHEN NVL(a.OSP_STATUS, nvl(a.CURRENT_STATUS,'')) ='INACTIVE' THEN 'INACTIVE'
        WHEN NVL(a.OSP_STATUS, nvl(a.CURRENT_STATUS,'')) ='VALID' THEN 'VALID'
        ELSE NVL(a.OSP_STATUS, nvl(a.CURRENT_STATUS,''))
    END ) status
    , date_first_usage
    , date_last_usage
    , total_days_count
    , 'FT_IMEI_ONLINE' SRC_TABLE
    , CURRENT_TIMESTAMP INSERT_DATE
    , nvl(a.ACTIVATION_DATE,null) ACTIVATION_DATE
    ,smonth
FROM MON.FT_CONTRACT_SNAPSHOT a
LEFT JOIN (
    SELECT
        b.PROFILE_CODE,  MAX (TRIM (b.CUSTOMER_TYPE)) CUSTOMER_TYPE, MAX (TRIM (b.OFFER_NAME))  OFFER_NAME
        , MAX (TRIM (b.PROFILE_NAME)) PROFILE_NAME, MAX (TRIM (b.CRM_SEGMENTATION)) CRM_SEGMENTATION, MAX (TRIM (b.CUSTOMER_PROFILE)) CUSTOMER_PROFILE
        , MAX (TRIM (DECILE_TYPE)) DECILE_TYPE
    FROM  DIM.DT_OFFER_PROFILES b
    GROUP BY b.PROFILE_CODE
) b ON UPPER (NVL(a.PROFILE,  substr(a.BSCS_COMM_OFFER, instr(a.BSCS_COMM_OFFER,'|')+1))) = b.PROFILE_CODE
LEFT JOIN (
    SELECT
        date_format('###SLICE_VALUE###' ||  '-01','yyyyMM')  smonth, substr(imei, 1, 14) imei, imsi, msisdn  --M@J:20170710 Restriction au 14 premier caractere de imei G2d.
        , min(sdate) date_first_usage, max(sdate) date_last_usage
        , count(DISTINCT sdate) total_days_count
        , 'FT_IMEI_ONLINE' SRC_TABLE
        , CURRENT_TIMESTAMP INSERT_DATE
    FROM MON.FT_IMEI_ONLINE
    WHERE sdate BETWEEN   '###SLICE_VALUE###' ||  '-01' AND DATE_SUB(ADD_MONTHS ('###SLICE_VALUE###' ||  '-01' , 1) , 1)
    GROUP BY substr(imei, 1, 14), imsi, msisdn
) c ON c.MSISDN = a.ACCESS_KEY
WHERE
 a.EVENT_DATE = ADD_MONTHS ('###SLICE_VALUE###' ||  '-01' , 1)
 AND (CASE
        WHEN nvl(a.OSP_STATUS, a.CURRENT_STATUS)='ACTIVE' THEN 'ACTIVE'
        WHEN nvl(a.OSP_STATUS, a.CURRENT_STATUS)='a' THEN 'ACTIVE'
        WHEN nvl(a.OSP_STATUS, a.CURRENT_STATUS)='d' THEN 'DEACT'
        WHEN nvl(a.OSP_STATUS, a.CURRENT_STATUS)='s' THEN 'INACTIVE'
        WHEN nvl(a.OSP_STATUS, a.CURRENT_STATUS)='DEACTIVATED' THEN 'DEACT'
        WHEN nvl(a.OSP_STATUS, a.CURRENT_STATUS)='INACTIVE' THEN 'INACTIVE'
        WHEN nvl(a.OSP_STATUS, a.CURRENT_STATUS)='VALID' THEN 'VALID'
        ELSE nvl(a.OSP_STATUS, a.CURRENT_STATUS)
    END) <> 'TERMINATED'
    AND msisdn IS NOT NULL and ACCESS_KEY is not null AND NVL(PROFILE_CODE,'') !=''

