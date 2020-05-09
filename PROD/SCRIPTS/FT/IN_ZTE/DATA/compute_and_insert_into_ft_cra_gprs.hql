INSERT INTO TABLE MON.FT_CRA_GPRS PARTITION(SESSION_DATE)
SELECT
    TRANSACTION_ID,
    CALL_TYPE,
    SERVED_PARTY_MSISDN,
    OTHER_PARTY_MSISDN,
    SERVED_PARTY_OFFER,
    SERVED_PARTY_TYPE,
    SESSION_TIME,
    BYTES_SENT,
    BYTES_RECEIVED,
    TOTAL_COST,
    SESSION_DURATION,
    CONTENT_PROVIDER,
    SERVICE_TYPE,
    SERVICE_CODE,
    TRANSACTION_TERMINATION_IND,
    SERVED_PARTY_IMSI,
    SERVED_PARTY_IMEI,
    TOTAL_HITS,
    TOTAL_UNIT,
    TOTAL_COMMODITIES,
    TOTAL_ERRORS,
    SGSN,
    GGSN,
    APN,
    FIRST_URL,
    ERROR_LIST,
    COUNTRY_DESTINATION_LIST,
    UNIT_OF_MEASUREMENT,
    UNITS_USED_IN_BUNDLE,
    ERROR_LIST_IN,
    DOWNLOAD_STATUS,
    OPERATOR_CODE,
    SERVICE_CATEGORY,
    SERVED_PARTY_PRICE_PLAN,
    CHARGED_PARTY_MSISDN,
    ROAMING_INDICATOR,
    LOCATION_MCC,
    LOCATION_MNC,
    LOCATION_LAC,
    LOCATION_CI,
    PDP_ADDRESS,
    MAIN_COST,
    PROMO_COST,
    BUNDLE_BYTES_USED_VOLUME,
    BUNDLE_MMS_USED_VOLUME,
    CHARGE_SUM,
    USED_VOLUME_LIST,
    USED_BALANCE_LIST,
    USED_UNIT_LIST,
    MAIN_REMAINING_CREDIT,
    PROMO_REMAINING_CREDIT,
    BUNDLE_BYTES_REMAINING_VOLUME,
    BUNDLE_MMS_REMAINING_VOLUME,
    REMAINING_VOLUME_LIST,
    TOTAL_OCCURENCE,
    SOURCE_PLATFORM,
    SOURCE_DATA,
    PRECEDING_VOLUME_LIST,
    SDP_GOS_SERV_NAME,
    SDP_GOS_SERV_DETAIL,
    DWH_IT_ENTRY_DATE,
    DWH_FT_ENTRY_DATE,
    ORIGINAL_FILE_NAME,
    GPP_USER_LOCATION_INFO,
    SESSION_DATE
FROM (
    SELECT
        session_id TRANSACTION_ID,
        (CASE call_type
        WHEN 0 THEN 'UNK'
        WHEN 1 THEN 'OUT'
        WHEN 2 THEN 'INC'
        WHEN 3 THEN 'FWD'
        WHEN 18 THEN 'FWD'
        ELSE CAST(call_type AS STRING)
        END) CALL_TYPE,
        TO_DATE(start_time) SESSION_DATE,
        DATE_FORMAT(start_time,'HHmmss') SESSION_TIME,
        SUBSTRING(calling_nbr,-9) SERVED_PARTY_MSISDN,
        NVL(pc1.profile_name, CAST(a.prod_spec_id AS STRING)) SERVED_PARTY_OFFER,
        NVL(pp1.price_plan_name, CAST(a.price_plan_code AS STRING))  SERVED_PARTY_PRICE_PLAN,
        (CASE PREPAY_FLAG
        WHEN 1 THEN 'PREPAID'
        WHEN 2 THEN 'POSTPAID'
        WHEN 3 THEN 'HYBRID'
        ELSE CAST(PREPAY_FLAG AS STRING)
        END ) SERVED_PARTY_TYPE,
        billing_imsi SERVED_PARTY_IMSI,
        calling_imei SERVED_PARTY_IMEI,
        SUBSTRING(called_nbr,-9) OTHER_PARTY_MSISDN ,
        CAST(provider_id AS STRING) CONTENT_PROVIDER,
        NVL(rg.rating_service_type,a.rating_group) SERVICE_TYPE,
        NVL(re.rating_event_name,CAST(re_id AS STRING))  SERVICE_CODE,
        NVL(re.rating_event_service,CAST(re_id AS STRING))  SERVICE_CATEGORY,
        sgsn_address SGSN,
        ggsn_address GGSN,
        called_station_id APN,
        (CASE charge_unit
        WHEN '4' THEN 'DURATION'
        WHEN '74' THEN 'OCCURRENCE'
        WHEN '106' THEN 'QUANTITY'
        WHEN '14' THEN 'ORIGINAL CHARGE'
        WHEN '235' THEN 'UP+DOWN LOAD'
        ELSE charge_unit
        END) UNIT_OF_MEASUREMENT,
        result_code DOWNLOAD_STATUS,
        termination_cause TRANSACTION_TERMINATION_IND,
        FN_GET_OPERATOR_CODE(SUBSTRING(billing_nbr,-9)) OPERATOR_CODE,
        pdp_address PDP_ADDRESS,
        SUBSTRING(billing_nbr,-9) CHARGED_PARTY_MSISDN,
        international_roaming_flag ROAMING_INDICATOR,
        NULL UNITS_USED_IN_BUNDLE,
        NULL TOTAL_HITS,
        NULL TOTAL_COMMODITIES,
        NULL TOTAL_ERRORS,
        NULL FIRST_URL,
        NULL ERROR_LIST,
        NULL COUNTRY_DESTINATION_LIST,
        NULL ERROR_LIST_IN,
        ISMP_PRODUCT_OFFER_ID SDP_GOS_SERV_NAME,
        ISMP_PRODUCT_ID SDP_GOS_SERV_DETAIL,
        COUNT(*) TOTAL_OCCURENCE,
        MAX(mcc) LOCATION_MCC,
        MAX(mnc) LOCATION_MNC,
        MAX(NVL(LPAD(CONV(IF(SUBSTR(CAST(GPP_USER_LOCATION_INFO AS STRING),9,4)="",NULL,SUBSTR(CAST(GPP_USER_LOCATION_INFO AS STRING),9,4)), 16, 10) ,5,0),'ND')) LOCATION_LAC,
        MAX((CASE LENGTH(GPP_USER_LOCATION_INFO)
        WHEN 16 THEN NVL(LPAD(CONV(IF(SUBSTR(CAST(GPP_USER_LOCATION_INFO AS STRING),13,4)="",NULL,SUBSTR(CAST(GPP_USER_LOCATION_INFO AS STRING),13,4)), 16, 10) ,5,0),'ND')
        ELSE NVL(LPAD(CONV(IF(SUBSTR(CAST(GPP_USER_LOCATION_INFO AS STRING),21,4)="",NULL,SUBSTR(CAST(GPP_USER_LOCATION_INFO AS STRING),21,4)), 16, 10) ,5,0),'ND')
        END)) LOCATION_CI,
        MAX(
        (CASE bti1.ACCT_RES_RATING_TYPE
        WHEN 'BD' THEN charge1_corrected
        WHEN 'MB' THEN byte_down
        ELSE 0
        end)
        +
        (CASE bti2.ACCT_RES_RATING_TYPE
        WHEN 'BD' THEN charge2_corrected
        WHEN 'MB' THEN byte_down
        ELSE 0
        end)
        +
        (CASE bti3.ACCT_RES_RATING_TYPE
        WHEN 'BD' THEN charge3_corrected
        WHEN 'MB' THEN byte_down
        ELSE 0
        END)
        +
        (CASE bti4.ACCT_RES_RATING_TYPE
        WHEN 'BD' THEN charge4_corrected
        WHEN 'MB' THEN byte_down
        ELSE 0
        END)
        ) TOTAL_UNIT,
        MAX(byte_up) BYTES_SENT,
        MAX(byte_down) BYTES_RECEIVED,
        CAST(MAX(
        IF(bti1.ACCT_RES_RATING_TYPE in ('PB','MB'), charge1_corrected, 0)
        + IF(bti2.ACCT_RES_RATING_TYPE in ('PB','MB'), charge2_corrected, 0)
        + IF(bti3.ACCT_RES_RATING_TYPE in ('PB','MB'), charge3_corrected, 0)
        + IF(bti4.ACCT_RES_RATING_TYPE in ('PB','MB'), charge4_corrected, 0)
        ) AS DOUBLE)/100 TOTAL_COST ,
        NVL(duration,0) SESSION_DURATION  ,
        CAST(MAX(
        IF(bti1.ACCT_RES_RATING_TYPE in ('MB'), charge1_corrected, 0)
        + IF(bti2.ACCT_RES_RATING_TYPE in ('MB'), charge2_corrected, 0)
        + IF(bti3.ACCT_RES_RATING_TYPE in ('MB'), charge3_corrected, 0)
        + IF(bti4.ACCT_RES_RATING_TYPE in ('MB'), charge4_corrected, 0)
        ) AS DOUBLE)/100 MAIN_COST ,
        CAST(MAX(
        IF(bti1.ACCT_RES_RATING_TYPE in ('PB'), charge1_corrected, 0)
        + IF(bti2.ACCT_RES_RATING_TYPE in ('PB'), charge2_corrected, 0)
        + IF(bti3.ACCT_RES_RATING_TYPE in ('PB'), charge3_corrected, 0)
        + IF(bti4.ACCT_RES_RATING_TYPE in ('PB'), charge4_corrected, 0)
        ) AS DOUBLE)/100 PROMO_COST ,
        MAX(
        IF(bti1.ACCT_RES_RATING_TYPE in ('BD'), charge1_corrected, 0)
        + IF(bti2.ACCT_RES_RATING_TYPE in ('BD'), charge2_corrected, 0)d
        + IF(bti3.ACCT_RES_RATING_TYPE in ('BD'), charge3_corrected, 0)
        + IF(bti4.ACCT_RES_RATING_TYPE in ('BD'), charge4_corrected, 0)
        ) BUNDLE_BYTES_USED_VOLUME ,
        MAX(
        IF(bti1.ACCT_RES_RATING_TYPE in ('BM'), charge1_corrected, 0)
        + IF(bti2.ACCT_RES_RATING_TYPE in ('BM'), charge2_corrected, 0)
        + IF(bti3.ACCT_RES_RATING_TYPE in ('BM'), charge3_corrected, 0)
        + IF(bti4.ACCT_RES_RATING_TYPE in ('BM'), charge4_corrected, 0)
        ) BUNDLE_MMS_USED_VOLUME ,
        MAX(charge1_corrected + charge2_corrected + charge3_corrected + charge4_corrected) CHARGE_SUM,
        MAX(IF(CONCAT_WS('|',NVL(charge1_corrected,''),NVL(charge2_corrected,''),NVL(charge3_corrected,''),NVL(charge4_corrected,''))='',NULL,CONCAT_WS('|',NVL(charge1_corrected,''),NVL(charge2_corrected,''),NVL(charge3_corrected,''),NVL(charge4_corrected,'')))) USED_VOLUME_LIST,
        MAX(CONCAT_WS('|'
        , NVL(bti1.ACCT_RES_NAME,NVL(CAST(a.ACCT_ITEM_TYPE_ID1 AS STRING),''))
        , NVL(bti2.ACCT_RES_NAME,NVL(CAST(a.ACCT_ITEM_TYPE_ID2 AS STRING),''))
        , NVL(bti3.ACCT_RES_NAME,NVL(CAST(a.ACCT_ITEM_TYPE_ID3 AS STRING),''))
        , NVL(bti4.ACCT_RES_NAME,NVL(CAST(a.ACCT_ITEM_TYPE_ID4 AS STRING),''))
        )
        ) USED_BALANCE_LIST,
        MAX(CONCAT_WS('|'
        , NVL(bti1.ACCT_RES_RATING_UNIT,'')
        , NVL(bti2.ACCT_RES_RATING_UNIT,'')
        , NVL(bti3.ACCT_RES_RATING_UNIT,'')
        , NVL(bti4.ACCT_RES_RATING_UNIT,'')
        )
        ) USED_UNIT_LIST,
        CAST(MAX(
        IF(bti1.ACCT_RES_RATING_TYPE = 'MB', -NVL(balance1,0), 0)
        + IF(bti2.ACCT_RES_RATING_TYPE = 'MB', -NVL(balance2,0), 0)
        + IF(bti3.ACCT_RES_RATING_TYPE = 'MB', -NVL(balance3,0), 0)
        + IF(bti4.ACCT_RES_RATING_TYPE = 'MB', -NVL(balance4,0), 0)
        ) AS DOUBLE)/100 MAIN_REMAINING_CREDIT ,
        CAST(MAX(
        IF(bti1.ACCT_RES_RATING_TYPE = 'PB', -NVL(balance1,0), 0)
        + IF(bti2.ACCT_RES_RATING_TYPE = 'PB', -NVL(balance2,0), 0)
        + IF(bti3.ACCT_RES_RATING_TYPE = 'PB', -NVL(balance3,0), 0)
        + IF(bti4.ACCT_RES_RATING_TYPE = 'PB', -NVL(balance4,0), 0)
        ) AS DOUBLE)/100 PROMO_REMAINING_CREDIT ,
        MAX(
        IF(bti1.ACCT_RES_RATING_TYPE = 'BD', -NVL(balance1,0), 0)
        + IF(bti2.ACCT_RES_RATING_TYPE = 'BD', -NVL(balance2,0), 0)
        + IF(bti3.ACCT_RES_RATING_TYPE = 'BD', -NVL(balance3,0), 0)
        + IF(bti4.ACCT_RES_RATING_TYPE = 'BD', -NVL(balance4,0), 0)
        ) BUNDLE_BYTES_REMAINING_VOLUME ,
        MAX(
        IF(bti1.ACCT_RES_RATING_TYPE = 'BM', -NVL(balance1,0), 0)
        + IF(bti2.ACCT_RES_RATING_TYPE = 'BM', -NVL(balance2,0), 0)
        + IF(bti3.ACCT_RES_RATING_TYPE = 'BM', -NVL(balance3,0), 0)
        + IF(bti4.ACCT_RES_RATING_TYPE = 'BM', -NVL(balance4,0), 0)
        ) BUNDLE_MMS_REMAINING_VOLUME ,
        MAX( CONCAT(
        (CASE
        WHEN bti1.ACCT_RES_RATING_TYPE in ('PB','MB') THEN ROUND(CAST(-NVL(balance1,0) AS DOUBLE)/100,2)
        WHEN bti1.ACCT_RES_RATING_TYPE IN ('BD','BM') THEN -NVL(balance1,0)
        ELSE 0 END)
        ,'|', (CASE
        WHEN bti2.ACCT_RES_RATING_TYPE in ('PB','MB') THEN ROUND(CAST(-NVL(balance2,0) AS DOUBLE)/100,2)
        WHEN bti2.ACCT_RES_RATING_TYPE IN ('BD','BM') THEN -NVL(balance2,0)
        ELSE 0 END)
        ,'|', (CASE
        WHEN bti3.ACCT_RES_RATING_TYPE in ('PB','MB') THEN ROUND(CAST (-NVL(balance3,0) AS DOUBLE)/100,2)
        WHEN bti3.ACCT_RES_RATING_TYPE IN ('BD','BM') THEN -NVL(balance3,0)
        ELSE 0 END)
        ,'|', (CASE
        WHEN bti4.ACCT_RES_RATING_TYPE in ('PB','MB') THEN ROUND(CAST(-NVL(balance4,0) AS DOUBLE)/100,2)
        WHEN bti4.ACCT_RES_RATING_TYPE IN ('BD','BM') THEN -NVL(balance4,0)
        ELSE 0 END)
        )
        ) REMAINING_VOLUME_LIST,
        MAX(ORIGINAL_FILE_NAME) ORIGINAL_FILE_NAME,
        MAX(GPP_USER_LOCATION_INFO) GPP_USER_LOCATION_INFO,
        MAX(a.insert_date) DWH_IT_ENTRY_DATE,
        CURRENT_TIMESTAMP DWH_FT_ENTRY_DATE,
        'IN_ZTE' SOURCE_PLATFORM,
        'IT_ZTE_DATA' SOURCE_DATA,
        MAX(IF(CONCAT_WS('|',NVL(CAST(pre_balance1 AS STRING),''),NVL(CAST(pre_balance2 AS STRING),''),NVL(CAST(pre_balance3 AS STRING),''),NVL(CAST(pre_balance4 AS STRING),''))='',NULL,CONCAT_WS('|',NVL(CAST(pre_balance1 AS STRING),''),NVL(CAST(pre_balance2 AS STRING),''),NVL(CAST(pre_balance3 AS STRING),''),NVL(CAST(pre_balance4 AS STRING),'')))) PRECEDING_VOLUME_LIST
    FROM (
        SELECT a.*
        ,IF(charge1 < 0, 0, NVL(charge1,0)) charge1_corrected
        ,IF(charge2 < 0, 0, NVL(charge2,0)) charge2_corrected
        ,IF(charge3 < 0, 0, NVL(charge3,0)) charge3_corrected
        ,IF(charge4 < 0, 0, NVL(charge4,0)) charge4_corrected
        FROM CDR.IT_ZTE_DATA a WHERE a.START_DATE = '###SLICE_VALUE###'
        ) a
        LEFT JOIN DIM.SPARK_DT_RATING_EVENT re ON (a.RE_ID = re.RATING_EVENT_ID)
        LEFT JOIN (SELECT ACCT_RES_ID, ACCT_ITEM_TYPE_ID, UPPER(ACCT_RES_NAME) ACCT_RES_NAME, ACCT_RES_RATING_TYPE, ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM) bti1 ON (a.ACCT_ITEM_TYPE_ID1 = bti1.ACCT_ITEM_TYPE_ID)
        LEFT JOIN (SELECT ACCT_RES_ID, ACCT_ITEM_TYPE_ID, UPPER(ACCT_RES_NAME) ACCT_RES_NAME, ACCT_RES_RATING_TYPE, ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM) bti2 ON (a.ACCT_ITEM_TYPE_ID2 = bti2.ACCT_ITEM_TYPE_ID)
        LEFT JOIN (SELECT ACCT_RES_ID, ACCT_ITEM_TYPE_ID, UPPER(ACCT_RES_NAME) ACCT_RES_NAME, ACCT_RES_RATING_TYPE, ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM) bti3 ON (a.ACCT_ITEM_TYPE_ID3 = bti3.ACCT_ITEM_TYPE_ID)
        LEFT JOIN (SELECT ACCT_RES_ID, ACCT_ITEM_TYPE_ID, UPPER(ACCT_RES_NAME) ACCT_RES_NAME, ACCT_RES_RATING_TYPE, ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM) bti4 ON (a.ACCT_ITEM_TYPE_ID4 = bti4.ACCT_ITEM_TYPE_ID)
        LEFT JOIN (SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME, PRICE_PLAN_CODE FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' ) pp1 ON (a.PRICE_PLAN_CODE = pp1.PRICE_PLAN_CODE)
        LEFT JOIN (SELECT PROFILE_ID, MAX(upper(PROFILE_NAME)) PROFILE_NAME FROM CDR.SPARK_IT_ZTE_PROFILE GROUP BY PROFILE_ID) pc1 ON(a.PROD_SPEC_ID = pc1.PROFILE_ID)
        LEFT JOIN DIM.DT_RATING_SERVICE_GROUP rg ON(a.RATING_GROUP = rg.RATING_GROUP_ID)
    GROUP BY
        session_id
        , (CASE call_type
        WHEN  0 THEN 'UNK'
        WHEN 1 THEN 'OUT'
        WHEN 2 THEN 'INC'
        WHEN 3 THEN 'FWD'
        WHEN 18 THEN 'FWD'
        ELSE CAST(call_type AS STRING)
        END )
        , TO_DATE(start_time)
        , DATE_FORMAT(start_time,'HHmmss')
        , SUBSTRING(calling_nbr,-9)
        , NVL(pc1.profile_name,CAST(a.prod_spec_id AS STRING))
        , NVL(pp1.price_plan_name,CAST(a.price_plan_code AS STRING))
        , (CASE PREPAY_FLAG
        WHEN 1 THEN 'PREPAID'
        WHEN 2 THEN 'POSTPAID'
        WHEN 3 THEN 'HYBRID'
        ELSE CAST(PREPAY_FLAG AS STRING)
        END )
        , billing_imsi
        , calling_imei
        , SUBSTRING(called_nbr,-9)
        , CAST(provider_id AS STRING)
        , NVL(rg.rating_service_type,a.rating_group)
        , NVL(re.rating_event_name,CAST(re_id AS STRING))
        , NVL(re.rating_event_service,CAST(re_id AS STRING))
        , sgsn_address
        , ggsn_address
        , called_station_id
        , (CASE charge_unit
        WHEN '4' THEN 'DURATION'
        WHEN '74' THEN 'OCCURRENCE'
        WHEN '106' THEN 'QUANTITY'
        WHEN '14' THEN 'ORIGINAL CHARGE'
        WHEN '235' THEN 'UP+DOWN LOAD'
        ELSE charge_unit
        END)
        , result_code
        , termination_cause
        , FN_GET_OPERATOR_CODE(SUBSTRING(billing_nbr,-9))
        , pdp_address
        , SUBSTRING(billing_nbr,-9)
        , ISMP_PRODUCT_OFFER_ID
        , ISMP_PRODUCT_ID
        , international_roaming_flag
        , CURRENT_TIMESTAMP
        , 'IN_ZTE'
        , 'IT_ZTE_DATA'
        ,DURATION
        ,CHARGE1
        ,CHARGE2
        ,CHARGE3
        ,CHARGE4
        ,PRE_BALANCE1
        ,BALANCE1
        ,BYTE_DOWN
        ,BYTES
)FT_DATA_RESULT