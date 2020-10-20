INSERT INTO TABLE MON.SPARK_FT_BILLED_TRANSACTION_POSTPAID PARTITION(TRANSACTION_DATE)
SELECT
 (CASE
WHEN LENGTH (SERVED_PARTY) = 8 THEN
(CASE
WHEN (SUBSTR(SERVED_PARTY,1,1) = '9') OR (SUBSTR(SERVED_PARTY,1,2) IN ('55', '56', '57', '58', '59')) THEN CONCAT('6',SERVED_PARTY)
WHEN (SUBSTR(SERVED_PARTY,1,1) = '7') OR (SUBSTR(SERVED_PARTY,1,2) IN ('50', '51', '52', '53', '54','80','81','82','83')) THEN CONCAT('6',SERVED_PARTY)
WHEN SUBSTR(SERVED_PARTY,1,1) = '6' OR SUBSTR(SERVED_PARTY,1,2) = '85' THEN CONCAT('6',SERVED_PARTY)
WHEN SUBSTR(SERVED_PARTY,1,1) IN ('2','3') AND SUBSTR(SERVED_PARTY,1,2)='22' THEN CONCAT('242',SUBSTR(SERVED_PARTY,-6))
WHEN SUBSTR(SERVED_PARTY,1,1) IN ('2','3') AND SUBSTR(SERVED_PARTY,1,2)='33' THEN CONCAT('243',SUBSTR(SERVED_PARTY,-6))
ELSE SERVED_PARTY
 END)
ELSE SERVED_PARTY
 END) SERVED_PARTY,
 (CASE
WHEN LENGTH (OTHER_PARTY) = 8 THEN
(CASE
    WHEN (substr(OTHER_PARTY,1,1) = '9') OR (substr(OTHER_PARTY,1,2) IN ('55', '56', '57', '58', '59')) THEN CONCAT('6',OTHER_PARTY)
    WHEN (substr(OTHER_PARTY,1,1) = '7') OR (substr(OTHER_PARTY,1,2) IN ('50', '51', '52', '53', '54','80','81','82','83')) THEN CONCAT('6',OTHER_PARTY)
    WHEN substr(OTHER_PARTY,1,1) = '6' OR substr(OTHER_PARTY,1,2) = '85' THEN CONCAT('6',OTHER_PARTY)
    WHEN substr(OTHER_PARTY,1,1) in ('2','3') and substr(OTHER_PARTY,1,2)='22' THEN CONCAT('242',substr(OTHER_PARTY,-6))
    WHEN substr(OTHER_PARTY,1,1) in ('2','3') and substr(OTHER_PARTY,1,2)='33' THEN CONCAT('243',substr(OTHER_PARTY,-6))
    ELSE OTHER_PARTY
 END)
ELSE OTHER_PARTY
END) OTHER_PARTY,
 RATED_DURATION,
 CALL_PROCESS_TOTAL_DURATION,
 RATED_VOLUME,
 UNIT_OF_MEASUREMENT,
 SERVICE_CODE,
 TELESERVICE_INDICATOR,
 NETWORK_EVENT_TYPE,
 NETWORK_ELEMENT_ID,
 OTHER_PARTY_ZONE,
 TRIM(CALL_DESTINATION_CODE) CALL_DESTINATION_CODE ,
 BILLING_TERM_INDICATOR,
 NETWORK_TERM_INDICATOR,
 SERVED_PARTY_IMSI,
 RAW_SPECIFIC_CHARGINGINDICATOR,
 RAW_LOAD_LEVEL_INDICATOR,
 FEE_NAME,
 REFILL_TOPUP_PROFILE,
 MAIN_REFILL_AMOUNT,
 BUNDLE_REFILL_AMOUNT,
 RAW_TRANSNUM_USED_FOR_REFILL,
 COMMERCIAL_OFFER,
 COMMERCIAL_PROFILE,
 LOCATION_MCC,
 LOCATION_MNC,
 LOCATION_LAC,
 LOCATION_CI,
 MAIN_RATED_AMOUNT,
 PROMO_RATED_AMOUNT,
 BUNDLE_IDENTIFIER,
 BUNDLE_UNIT,
 BUNDLE_CONSUMED_VOLUME,
 BUNDLE_DISCARDED_VOLUME,
 BUNDLE_REMAINING_VOLUME,
 BUNDLE_REFILL_VOLUME,
 RATED_AMOUNT_IN_BUNDLE,
 MAIN_REMAINING_CREDIT,
 PROMO_REMAINING_CREDIT,
 SPECIFIC_TARIFF_INDICATOR,
 LOCATION_NUMBER,
 MAIN_DISCARDED_CREDIT,
 PROMO_DISCARDED_CREDIT,
 SMS_DISCARDED_VOLUME,
 SMS_USED_VOLUME,
 RAW_TARIFF_PLAN,
 RAW_EVENT_COST,
 RAW_REFILL_MEANS,
 RAW_CALL_TYPE,
 CALL_DESTINATION_TYPE,
 ROAMING_INDICATOR,
 OPERATOR_CODE,
 YZDISCOUNT,
 LOCATION_LAC_DECIMAL,
 LOCATION_CI_DECIMAL,
 CHARGE_SUM,
 BYZ_RATED_AMOUNT,
 SMS_USED_VOLUME AS BUNDLE_SMS_USED_VOLUME,
 BUNDLE_TIME_USED_VOLUME,
 UNKNOWN_USED_VOLUME,
 BUNDLE_SMS_REMAINING_VOLUME,
 BUNDLE_TIME_REMAINING_VOLUME,
 UPLOAD_VOLUME,
 DOWNLOAD_VOLUME,
 IDENTIFIER_LIST,
 UNIT_OF_MEASUREMENT_LIST,
 RATED_VOLUME_LIST,
 REMAINING_VOLUME_LIST,
 TRANSACTION_TYPE,
 SERVED_PARTY_IMEI,
 CHARGED_PARTY,
 TRANSACTION_TERM_INDICATOR,
 UNKNOWN_VOLUME_LIST,
 VOLUME_LIST,
 ORIGINAL_COMMERCIAL_PROFILE,
 TRANSACTION_TIME,
 ORIGINAL_FILE_NAME,
 SOURCE_PLATEFORM,
 SOURCE_DATA,
 INSERT_DATE,
 VLR_NUMBER
 TRANSACTION_DATE
FROM
(
SELECT

    (CASE call_type
        WHEN 0 THEN 'UNK'
        WHEN 1 THEN 'OUT'
        WHEN 2 THEN 'INC'
        WHEN 3 THEN 'FWD'
        ELSE CAST(call_type AS STRING)
    END)Transaction_Type
    , DATE_FORMAT(start_time,'HHmmss') Transaction_Time
    ,VLR_NUMBER
    , Start_Date Transaction_Date
    , calling_nbr_formatted Served_Party
    , billing_imsi Served_Party_Imsi
    , calling_imei Served_Party_Imei
    , NVL(pp1.PRICE_PLAN_NAME,NVL(pp2.PRICE_PLAN_NAME,NVL(pp3.PRICE_PLAN_NAME,NVL(pp4.PRICE_PLAN_NAME,CAST(price_plan_id4 AS STRING))))) Commercial_Offer
    , NVL(pp5.PRICE_PLAN_NAME,CAST(a.price_plan_code AS STRING)) RAW_TARIFF_PLAN
    , NVL(pc1.PROFILE_NAME,CAST(a.PROD_SPEC_ID AS STRING)) Commercial_Profile
    , NVL(pc1.PROFILE_NAME,CAST(a.PROD_SPEC_ID AS STRING)) Original_Commercial_profile
    , billing_nbr_formatted Charged_Party
    , called_nbr_formatted Other_Party
    , (CASE
        WHEN rating_event_name like '%URGENT%' AND called_nbr_formatted LIKE '69%' THEN 'ONNET'
        WHEN rating_event_name like '%URGENT%' AND called_nbr_formatted NOT LIKE '69%' THEN 'OFFNET'
        ELSE NVL(rating_event_zone,CAST(re_id AS STRING))
      END ) Call_destination_type
    , IF (RATING_EVENT_OPERATOR ='OFFNET',
        (CASE
            WHEN LENGTH (NEW_CALLED_NBR) = 8 THEN
            (CASE
                WHEN (substr(NEW_CALLED_NBR,1,1) = '9') OR (substr(NEW_CALLED_NBR,1,2) IN ('55', '56', '57', '58', '59')) THEN 'ONNET'
                WHEN (substr(NEW_CALLED_NBR,1,1) = '7') OR (substr(NEW_CALLED_NBR,1,2) IN ('50', '51', '52', '53', '54','80','81','82','83')) THEN 'MTN'
                WHEN substr(NEW_CALLED_NBR,1,1) = '6' OR substr(NEW_CALLED_NBR,1,2) = '85' THEN 'NEXTTEL'
                WHEN substr(NEW_CALLED_NBR,1,1) in ('2','3') THEN 'CAM'
            ELSE 'INT'
            END)
            WHEN LENGTH (NEW_CALLED_NBR) = 9 THEN
            (CASE
                WHEN (substr(NEW_CALLED_NBR,1,2) = '69') OR (substr(NEW_CALLED_NBR,1,3) IN ('655', '656', '657', '658', '659')) THEN 'ONNET'
                WHEN (substr(NEW_CALLED_NBR,1,2) = '67') OR (substr(NEW_CALLED_NBR,1,3) IN ('650', '651', '652', '653', '654','680','681','682','683')) THEN 'MTN'
                WHEN substr(NEW_CALLED_NBR,1,2) = '66' or substr(NEW_CALLED_NBR,1,3) = '685' THEN 'NEXTTEL'
                WHEN substr(NEW_CALLED_NBR,1,3) in ('243','242','222','233') THEN 'CAM'
                WHEN substr(NEW_CALLED_NBR,1,2) = '62' THEN 'CAMTEL_MOB'
                ELSE 'INT'
            END)
            WHEN LENGTH (NEW_CALLED_NBR) = 13 AND substr(NEW_CALLED_NBR,1,3)= '160' THEN
            (CASE
                WHEN (substr(NEW_CALLED_NBR,1,4) = '1602') THEN 'ONNET'
                WHEN (substr(NEW_CALLED_NBR,1,4) = '1601') THEN 'MTN'
                WHEN (substr(NEW_CALLED_NBR,1,4) = '1603') THEN 'NEXTTEL'
                ELSE 'INT'
            END)
            WHEN LENGTH (NEW_CALLED_NBR) > 9 THEN 'INT'
            ELSE 'VAS'
        END),
         NVL(RATING_EVENT_OPERATOR,CAST(RE_ID AS STRING))
        ) Call_Destination_Code
    , international_roaming_flag Roaming_indicator
    , (CASE WHEN provider_id = 0 OR provider_id = 2 THEN 'OCM'
        WHEN provider_id = 1 THEN 'SET'
        WHEN provider_id IS NULL AND billing_nbr IS NULL THEN 'OCM'
        WHEN provider_id IS NULL AND billing_nbr_formatted NOT LIKE '692%' THEN 'OCM'
        WHEN provider_id IS NULL AND billing_nbr_formatted LIKE '692%' THEN 'SET'
        ELSE CAST(provider_id AS STRING)
      END) operator_code
    , (CASE WHEN rating_event_name like 'VOICE%' THEN trim(SUBSTRING(rating_event_name,6))
        WHEN rating_event_name like 'SMS%' THEN trim(SUBSTRING(rating_event_name,4))
        WHEN rating_event_name like 'DATA%' THEN trim(SUBSTRING(rating_event_name,5))
        WHEN rating_event_name like 'FAX%' THEN trim(SUBSTRING(rating_event_name,4))
        ELSE NVL(rating_event_name,CAST(re_id AS STRING))
      END) Other_Party_Zone
    , NVL(rating_event_service,CAST(re_id AS STRING)) Service_Code
    , NVL(rating_event_specific_tarif,CAST(re_id AS STRING)) Specific_tariff_indicator
    , NVL(yzdiscount,0) yzdiscount
    , result_code Transaction_Term_Indicator
    , CAST(result_code AS STRING) BILLING_TERM_INDICATOR
    , MAX(SUBSTRING(LAC_a,1,3)) location_mcc
    , MAX(SUBSTRING(LAC_a,4,2)) location_mnc
    , MAX(SUBSTRING(LAC_a,6,4)) location_lac
    , MAX(cell_a) location_ci
    , MAX(NVL(LPAD(CONV(IF(SUBSTR(CAST(LAC_a AS STRING),6,4)="",NULL,SUBSTR(CAST(LAC_a AS STRING),6,4)), 16, 10) ,5,0),'ND')) location_lac_decimal
    , MAX(NVL(LPAD(CONV(IF(CAST(cell_a AS STRING)="",NULL,CAST(cell_a AS STRING)), 16, 10) ,5,0),'ND')) location_ci_decimal
    , MAX(NVL(charge1,0) + NVL(charge2,0) + NVL(charge3,0) + NVL(charge4,0)) charge_sum
    , MAX(NVL(duration,0)) Call_process_total_duration
    , MAX(CASE WHEN (NVL(charge1,0) + NVL(charge2,0) + NVL(charge3,0) + NVL(charge4,0)) > 0 THEN NVL(duration,0) else 0 end) Rated_Duration
    , CAST(MAX(
    IF(bti1.ACCT_RES_RATING_TYPE in ('MB'), NVL(charge1,0), 0)
    + IF(bti2.ACCT_RES_RATING_TYPE in ('MB'), NVL(charge2,0), 0)
    + IF(bti3.ACCT_RES_RATING_TYPE in ('MB'), NVL(charge3,0), 0)
    + IF(bti4.ACCT_RES_RATING_TYPE in ('MB'), NVL(charge4,0), 0)
    ) AS DOUBLE)/100 Main_Rated_Amount
    , CAST(MAX(
    IF(bti1.ACCT_RES_RATING_TYPE in ('PB'), NVL(charge1,0), 0)
    + IF(bti2.ACCT_RES_RATING_TYPE in ('PB'), NVL(charge2,0), 0)
    + IF(bti3.ACCT_RES_RATING_TYPE in ('PB'), NVL(charge3,0), 0)
    + IF(bti4.ACCT_RES_RATING_TYPE in ('PB'), NVL(charge4,0), 0)
    ) AS DOUBLE)/100 Promo_Rated_Amount
    , CAST(MAX(
    IF(bti1.ACCT_RES_RATING_TYPE in ('PB','MB'), NVL(charge1,0), 0)
    + IF(bti2.ACCT_RES_RATING_TYPE in ('PB','MB'), NVL(charge2,0), 0)
    + IF(bti3.ACCT_RES_RATING_TYPE in ('PB','MB'), NVL(charge3,0), 0)
    + IF(bti4.ACCT_RES_RATING_TYPE in ('PB','MB'), NVL(charge4,0), 0)
    ) AS DOUBLE)/100 Rated_Amount
    , MAX(NVL(byzcharge1,0) + NVL(byzcharge2,0) + NVL(byzcharge3,0) + NVL(byzcharge4,0)) byz_rated_amount
    , CAST(MAX(
    IF(bti1.ACCT_RES_RATING_TYPE in ('PB'), NVL(charge1,0), 0)
    + IF(bti2.ACCT_RES_RATING_TYPE in ('PB'), NVL(charge2,0), 0)
    + IF(bti3.ACCT_RES_RATING_TYPE in ('PB'), NVL(charge3,0), 0)
    + IF(bti4.ACCT_RES_RATING_TYPE in ('PB'), NVL(charge4,0), 0)
    ) AS DOUBLE)/100 Rated_Amount_in_Bundle
    , MAX(
    IF(bti1.ACCT_RES_RATING_TYPE in ('BS'), NVL(charge1,0), 0)
    + IF(bti2.ACCT_RES_RATING_TYPE in ('BS'), NVL(charge2,0), 0)
    + IF(bti3.ACCT_RES_RATING_TYPE in ('BS'), NVL(charge3,0), 0)
    + IF(bti4.ACCT_RES_RATING_TYPE in ('BS'), NVL(charge4,0), 0)
    ) sms_used_volume --- USE THIS FIELD IN THE BIG SELECT AS bundle_sms_used_volume ie a new column--
    , MAX(
    IF(bti1.ACCT_RES_RATING_TYPE in ('BV'), NVL(charge1,0), 0)
    + IF(bti2.ACCT_RES_RATING_TYPE in ('BV'), NVL(charge2,0), 0)
    + IF(bti3.ACCT_RES_RATING_TYPE in ('BV'), NVL(charge3,0), 0)
    + IF(bti4.ACCT_RES_RATING_TYPE in ('BV'), NVL(charge4,0), 0)
    ) bundle_time_used_volume
    , MAX(
    IF(NVL(bti1.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB'), NVL(charge1,0), 0)
    + IF(NVL(bti2.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB'), NVL(charge2,0), 0)
    + IF(NVL(bti3.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB'), NVL(charge3,0), 0)
    + IF(NVL(bti4.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB'), NVL(charge4,0), 0)
    ) Unknown_used_volume
    , CAST(MAX(
    IF(bti1.ACCT_RES_RATING_TYPE in ('MB'), -NVL(balance1,0), 0)
    + IF(bti2.ACCT_RES_RATING_TYPE in ('MB'), -NVL(balance2,0), 0)
    + IF(bti3.ACCT_RES_RATING_TYPE in ('MB'), -NVL(balance3,0), 0)
    + IF(bti4.ACCT_RES_RATING_TYPE in ('MB'), -NVL(balance4,0), 0)
    ) AS DOUBLE)/100 Main_remaining_credit
    , CAST(MAX(
    IF(bti1.ACCT_RES_RATING_TYPE in ('PB'), -NVL(balance1,0), 0)
    + IF(bti2.ACCT_RES_RATING_TYPE in ('PB'), -NVL(balance2,0), 0)
    + IF(bti3.ACCT_RES_RATING_TYPE in ('PB'), -NVL(balance3,0), 0)
    + IF(bti4.ACCT_RES_RATING_TYPE in ('PB'), -NVL(balance4,0), 0)
    ) AS DOUBLE)/100 Promo_remaining_credit
    , MAX(
    IF(bti1.ACCT_RES_RATING_TYPE in ('BS'), -NVL(balance1,0), 0)
    + IF(bti2.ACCT_RES_RATING_TYPE in ('BS'), -NVL(balance2,0), 0)
    + IF(bti3.ACCT_RES_RATING_TYPE in ('BS'), -NVL(balance3,0), 0)
    + IF(bti4.ACCT_RES_RATING_TYPE in ('BS'), -NVL(balance4,0), 0)
    ) bundle_sms_remaining_volume
    , MAX(
    IF(bti1.ACCT_RES_RATING_TYPE in ('BV'), -NVL(balance1,0), 0)
    + IF(bti2.ACCT_RES_RATING_TYPE in ('BV'), -NVL(balance2,0), 0)
    + IF(bti3.ACCT_RES_RATING_TYPE in ('BV'), -NVL(balance3,0), 0)
    + IF(bti4.ACCT_RES_RATING_TYPE in ('BV'), -NVL(balance4,0), 0)
    ) bundle_time_remaining_volume
    , MAX(CAST(byte_up AS DOUBLE)/1024) upload_volume
    , MAX((CAST(byte_down AS DOUBLE)/1024)) download_volume
    , MAX( CONCAT_WS('|',
    NVL(NVL(bti1.ACCT_RES_NAME,CAST(a.ACCT_ITEM_TYPE_ID1 AS STRING)), '')
    ,NVL(NVL(bti2.ACCT_RES_NAME,CAST(a.ACCT_ITEM_TYPE_ID2 AS STRING)), '')
    ,NVL(NVL(bti3.ACCT_RES_NAME,CAST(a.ACCT_ITEM_TYPE_ID3 AS STRING)), '')
    ,NVL(NVL(bti4.ACCT_RES_NAME,CAST(a.ACCT_ITEM_TYPE_ID4 AS STRING)), '')
    )
    ) Identifier_list
    , MAX( CONCAT_WS('|',
    CAST(NVL(charge1,0) AS STRING)
    ,CAST(NVL(charge2,0) AS STRING)
    ,CAST(NVL(charge3,0) AS STRING)
    ,CAST(NVL(charge4,0) AS STRING)
    )
    ) Volume_List
    , MAX( CONCAT_WS('|',
    NVL(bti1.ACCT_RES_RATING_UNIT,'')
    ,NVL(bti2.ACCT_RES_RATING_UNIT,'')
    ,NVL(bti3.ACCT_RES_RATING_UNIT,'')
    ,NVL(bti4.ACCT_RES_RATING_UNIT,'')
    )
    ) Unit_Of_Measurement_List
    , MAX( CONCAT_WS('|',
    CAST((CASE
    WHEN bti1.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(NVL(charge1,0) AS DOUBLE)/100
    WHEN bti1.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN NVL(charge1,0)
    ELSE 0 END) AS STRING)
    ,CAST((CASE
    WHEN bti2.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(NVL(charge2,0) AS DOUBLE)/100
    WHEN bti2.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN NVL(charge2,0)
    ELSE 0 END) AS STRING)
    ,CAST((CASE
    WHEN bti3.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(NVL(charge3,0) AS DOUBLE)/100
    WHEN bti3.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN NVL(charge3,0)
    ELSE 0 END) AS STRING)
    ,CAST((CASE
    WHEN bti4.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(NVL(charge4,0) AS DOUBLE)/100
    WHEN bti4.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN NVL(charge4,0)
    ELSE 0 END) AS STRING)
    )
    ) Rated_Volume_list
    , MAX( CONCAT_WS('|',
    CAST(IF(NVL(bti1.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB') , NVL(charge1,0),0) AS STRING)
    ,CAST(IF(NVL(bti2.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB') , NVL(charge2,0),0) AS STRING)
    ,CAST(IF(NVL(bti3.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB') , NVL(charge3,0),0) AS STRING)
    ,CAST(IF(NVL(bti4.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB') , NVL(charge4,0),0) AS STRING)
    )
    ) Unknown_Volume_List
    , MAX( CONCAT_WS('|',
    CAST((CASE
    WHEN bti1.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(-NVL(balance1,0) AS DOUBLE)/100
    WHEN bti1.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN -NVL(balance1,0)
    ELSE 0 END) AS STRING)
    ,CAST((CASE
    WHEN bti2.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(-NVL(balance2,0) AS DOUBLE)/100
    WHEN bti2.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN -NVL(balance2,0)
    ELSE 0 END) AS STRING)
    ,CAST((CASE
    WHEN bti3.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(-NVL(balance3,0) AS DOUBLE)/100
    WHEN bti3.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN -NVL(balance3,0)
    ELSE 0 END) AS STRING)
    ,CAST((CASE
    WHEN bti4.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(-NVL(balance4,0) AS DOUBLE)/100
    WHEN bti4.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN -NVL(balance4,0)
    ELSE 0 END) AS STRING)
    )
    ) Remaining_Volume_list
    , NULL LOCATION_NUMBER
    , NULL MAIN_DISCARDED_CREDIT
    , NULL PROMO_DISCARDED_CREDIT
    , NULL SMS_DISCARDED_VOLUME
    , NULL NETWORK_TERM_INDICATOR
    , NULL RAW_SPECIFIC_CHARGINGINDICATOR
    , NULL RAW_LOAD_LEVEL_INDICATOR
    , NULL FEE_NAME
    , NULL REFILL_TOPUP_PROFILE
    , NULL MAIN_REFILL_AMOUNT
    , NULL BUNDLE_REFILL_AMOUNT
    , NULL RAW_TRANSNUM_USED_FOR_REFILL
    , NULL BUNDLE_IDENTIFIER
    , NULL BUNDLE_UNIT
    , NULL BUNDLE_CONSUMED_VOLUME
    , NULL BUNDLE_DISCARDED_VOLUME
    , NULL BUNDLE_REMAINING_VOLUME
    , NULL BUNDLE_REFILL_VOLUME
    , NULL RATED_VOLUME
    , NULL UNIT_OF_MEASUREMENT
    , NULL TELESERVICE_INDICATOR
    , NULL NETWORK_EVENT_TYPE
    , NULL NETWORK_ELEMENT_ID
    , NULL RAW_EVENT_COST
    , NULL RAW_REFILL_MEANS
    , NULL RAW_CALL_TYPE
    , 'IN_ZTE' Source_Plateform
    , 'IT_ZTE_VOICE_SMS_POST' Source_Data
    , MAX(ORIGINAL_FILE_DATE) Original_File_Date
    , MAX(ORIGINAL_FILE_NAME) Original_File_Name
    , CURRENT_TIMESTAMP Insert_Date
    FROM (
        SELECT *,
        REGEXP_EXTRACT(REGEXP_REPLACE(REGEXP_REPLACE(CALLED_NBR, '(^\\+|^00)', ''),'^237',''), '([A-Za-z]*)([0-9]+)(.*)', 2) NEW_CALLED_NBR,
        (CASE
            WHEN calling_nbr IN ('44534952454D494E4445', '534D5350415243') THEN '99999999'
            WHEN LENGTH(NVL(REGEXP_EXTRACT(calling_nbr,'[0-9]+', 0),calling_nbr)) = 9
            AND SUBSTR(NVL(REGEXP_EXTRACT(calling_nbr,'[0-9]+', 0),calling_nbr), 1, 1) NOT IN ('0', '2')
            THEN NVL(REGEXP_EXTRACT(calling_nbr,'[0-9]+', 0),calling_nbr)
            WHEN SUBSTR(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(calling_nbr,'[0-9]+', 0),calling_nbr),"^0+(?!$)",""), 1, 3) = '237'
            AND LENGTH(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(calling_nbr,'[0-9]+', 0),calling_nbr),"^0+(?!$)","")) > 3
            THEN SUBSTR(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(calling_nbr,'[0-9]+', 0),calling_nbr),"^0+(?!$)",""), 4)
            ELSE IF(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(calling_nbr,'[0-9]+', 0),calling_nbr),"^0+(?!$)","") ="0", NULL, REGEXP_REPLACE(NVL(REGEXP_EXTRACT(calling_nbr,'[0-9]+', 0),calling_nbr),"^0+(?!$)",""))
        END) calling_nbr_formatted,
        (CASE
            WHEN billing_nbr IN ('44534952454D494E4445', '534D5350415243') THEN '99999999'
            WHEN LENGTH(NVL(REGEXP_EXTRACT(billing_nbr,'[0-9]+', 0),billing_nbr)) = 9
            AND SUBSTR(NVL(REGEXP_EXTRACT(billing_nbr,'[0-9]+', 0),billing_nbr), 1, 1) NOT IN ('0', '2')
            THEN NVL(REGEXP_EXTRACT(billing_nbr,'[0-9]+', 0),billing_nbr)
            WHEN SUBSTR(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(billing_nbr,'[0-9]+', 0),billing_nbr),"^0+(?!$)",""), 1, 3) = '237'
            AND LENGTH(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(billing_nbr,'[0-9]+', 0),billing_nbr),"^0+(?!$)","")) > 3
            THEN SUBSTR(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(billing_nbr,'[0-9]+', 0),billing_nbr),"^0+(?!$)",""), 4)
            ELSE IF(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(billing_nbr,'[0-9]+', 0),billing_nbr),"^0+(?!$)","") ="0", NULL, REGEXP_REPLACE(NVL(REGEXP_EXTRACT(billing_nbr,'[0-9]+', 0),billing_nbr),"^0+(?!$)",""))
        END) billing_nbr_formatted,
        (CASE
            WHEN called_nbr IN ('44534952454D494E4445', '534D5350415243') THEN '99999999'
            WHEN LENGTH(NVL(REGEXP_EXTRACT(called_nbr,'[0-9]+', 0),called_nbr)) = 9
            AND SUBSTR(NVL(REGEXP_EXTRACT(called_nbr,'[0-9]+', 0),called_nbr), 1, 1) NOT IN ('0', '2')
            THEN NVL(REGEXP_EXTRACT(called_nbr,'[0-9]+', 0),called_nbr)
            WHEN SUBSTR(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(called_nbr,'[0-9]+', 0),called_nbr),"^0+(?!$)",""), 1, 3) = '237'
            AND LENGTH(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(called_nbr,'[0-9]+', 0),called_nbr),"^0+(?!$)","")) > 3
            THEN SUBSTR(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(called_nbr,'[0-9]+', 0),called_nbr),"^0+(?!$)",""), 4)
            ELSE IF(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(called_nbr,'[0-9]+', 0),called_nbr),"^0+(?!$)","") ="0", NULL, REGEXP_REPLACE(NVL(REGEXP_EXTRACT(called_nbr,'[0-9]+', 0),called_nbr),"^0+(?!$)",""))
        END) called_nbr_formatted
        FROM CDR.SPARK_IT_ZTE_VOICE_SMS_POST
    ) a
    LEFT JOIN DIM.DT_RATING_EVENT b ON (a.RE_ID = b.RATING_EVENT_ID)
    LEFT JOIN (SELECT ACCT_RES_ID, ACCT_ITEM_TYPE_ID, UPPER(ACCT_RES_NAME) ACCT_RES_NAME, ACCT_RES_RATING_TYPE, ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM) bti1 ON (a.ACCT_ITEM_TYPE_ID1 = bti1.ACCT_ITEM_TYPE_ID)
    LEFT JOIN (SELECT ACCT_RES_ID, ACCT_ITEM_TYPE_ID, UPPER(ACCT_RES_NAME) ACCT_RES_NAME, ACCT_RES_RATING_TYPE, ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM) bti2 ON (a.ACCT_ITEM_TYPE_ID2 = bti2.ACCT_ITEM_TYPE_ID)
    LEFT JOIN(SELECT ACCT_RES_ID, ACCT_ITEM_TYPE_ID, UPPER(ACCT_RES_NAME) ACCT_RES_NAME, ACCT_RES_RATING_TYPE, ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM) bti3 ON (a.ACCT_ITEM_TYPE_ID3 = bti3.ACCT_ITEM_TYPE_ID)
    LEFT JOIN (SELECT ACCT_RES_ID, ACCT_ITEM_TYPE_ID, UPPER(ACCT_RES_NAME) ACCT_RES_NAME, ACCT_RES_RATING_TYPE, ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM) bti4 ON (a.ACCT_ITEM_TYPE_ID4 = bti4.ACCT_ITEM_TYPE_ID)
    LEFT JOIN (SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME, PRICE_PLAN_CODE FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' ) pp1 ON ( a.PRICE_PLAN_ID1 = pp1.PRICE_PLAN_ID)
    LEFT JOIN (SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME, PRICE_PLAN_CODE FROM cdr.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' ) pp2 ON ( a.PRICE_PLAN_ID2 = pp2.PRICE_PLAN_ID)
    LEFT JOIN (SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME, PRICE_PLAN_CODE FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' ) pp3 ON ( a.PRICE_PLAN_ID3 = pp3.PRICE_PLAN_ID)
    LEFT JOIN (SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME, PRICE_PLAN_CODE FROM cdr.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' ) pp4 ON ( a.PRICE_PLAN_ID4 = pp4.PRICE_PLAN_ID)
    LEFT JOIN (SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME, PRICE_PLAN_CODE FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' ) pp5 ON ( a.PRICE_PLAN_CODE = pp5.PRICE_PLAN_CODE)
    LEFT JOIN (SELECT PROFILE_ID, MAX(upper(PROFILE_NAME)) PROFILE_NAME FROM CDR.SPARK_IT_ZTE_PROFILE GROUP BY PROFILE_ID) pc1 ON (a.PROD_SPEC_ID = pc1.PROFILE_ID)
    WHERE a.START_DATE = '###SLICE_VALUE###'
    GROUP BY
        (CASE call_type
            WHEN 0 THEN 'UNK'
            WHEN 1 THEN 'OUT'
            WHEN 2 THEN 'INC'
            WHEN 3 THEN 'FWD'
            ELSE CAST(call_type AS STRING)
        END)
        , start_time
        , Start_Date
        , calling_nbr_formatted
        , billing_imsi
        , calling_imei
        , NVL(pp1.PRICE_PLAN_NAME,NVL(pp2.PRICE_PLAN_NAME,NVL(pp3.PRICE_PLAN_NAME,NVL(pp4.PRICE_PLAN_NAME,CAST(price_plan_id4 AS STRING)))))
        , NVL(pp5.PRICE_PLAN_NAME,CAST(a.price_plan_code AS STRING))
        , NVL(pc1.PROFILE_NAME,CAST(a.PROD_SPEC_ID AS STRING))
        , NVL(pc1.PROFILE_NAME,CAST(a.PROD_SPEC_ID AS STRING))
        , billing_nbr_formatted
        , called_nbr_formatted
        , (CASE
             WHEN rating_event_name like '%URGENT%' AND called_nbr_formatted LIKE '69%' THEN 'ONNET'
             WHEN rating_event_name like '%URGENT%' AND called_nbr_formatted NOT LIKE '69%' THEN 'OFFNET'
             ELSE NVL(rating_event_zone,CAST(re_id AS STRING))
         END )
        , IF (RATING_EVENT_OPERATOR ='OFFNET',
             (CASE
                 WHEN LENGTH (NEW_CALLED_NBR) = 8 THEN
                 (CASE
                     WHEN (substr(NEW_CALLED_NBR,1,1) = '9') OR (substr(NEW_CALLED_NBR,1,2) IN ('55', '56', '57', '58', '59')) THEN 'ONNET'
                     WHEN (substr(NEW_CALLED_NBR,1,1) = '7') OR (substr(NEW_CALLED_NBR,1,2) IN ('50', '51', '52', '53', '54','80','81','82','83')) THEN 'MTN'
                     WHEN substr(NEW_CALLED_NBR,1,1) = '6' OR substr(NEW_CALLED_NBR,1,2) = '85' THEN 'NEXTTEL'
                     WHEN substr(NEW_CALLED_NBR,1,1) in ('2','3') THEN 'CAM'
                     ELSE 'INT'
                 END)
                 WHEN LENGTH (NEW_CALLED_NBR) = 9 THEN
                 (CASE
                     WHEN (substr(NEW_CALLED_NBR,1,2) = '69') OR (substr(NEW_CALLED_NBR,1,3) IN ('655', '656', '657', '658', '659')) THEN 'ONNET'
                     WHEN (substr(NEW_CALLED_NBR,1,2) = '67') OR (substr(NEW_CALLED_NBR,1,3) IN ('650', '651', '652', '653', '654','680','681','682','683')) THEN 'MTN'
                     WHEN substr(NEW_CALLED_NBR,1,2) = '66' or substr(NEW_CALLED_NBR,1,3) = '685' THEN 'NEXTTEL'
                     WHEN substr(NEW_CALLED_NBR,1,3) in ('243','242','222','233') THEN 'CAM'
                     WHEN substr(NEW_CALLED_NBR,1,2) = '62' THEN 'CAMTEL_MOB'
                     ELSE 'INT'
                 END)
                 WHEN LENGTH (NEW_CALLED_NBR) = 13 AND substr(NEW_CALLED_NBR,1,3)= '160' THEN
                 (CASE
                     WHEN (substr(NEW_CALLED_NBR,1,4) = '1602') THEN 'ONNET'
                     WHEN (substr(NEW_CALLED_NBR,1,4) = '1601') THEN 'MTN'
                     WHEN (substr(NEW_CALLED_NBR,1,4) = '1603') THEN 'NEXTTEL'
                     ELSE 'INT'
                 END)
                 WHEN LENGTH (NEW_CALLED_NBR) > 9 THEN 'INT'
                 ELSE 'VAS'
             END),
             NVL(RATING_EVENT_OPERATOR,CAST(RE_ID AS STRING))
        )
        , international_roaming_flag
        ,(CASE WHEN provider_id = 0 OR provider_id = 2 THEN 'OCM'
            WHEN provider_id = 1 THEN 'SET'
            WHEN provider_id IS NULL AND billing_nbr IS NULL THEN 'OCM'
            WHEN provider_id IS NULL AND billing_nbr_formatted NOT LIKE '692%' THEN 'OCM'
            WHEN provider_id IS NULL AND billing_nbr_formatted LIKE '692%' THEN 'SET'
            ELSE CAST(provider_id AS STRING)
        END)
        ,(CASE WHEN rating_event_name like 'VOICE%' THEN trim(SUBSTRING(rating_event_name,6))
             WHEN rating_event_name like 'SMS%' THEN trim(SUBSTRING(rating_event_name,4))
             WHEN rating_event_name like 'DATA%' THEN trim(SUBSTRING(rating_event_name,5))
             WHEN rating_event_name like 'FAX%' THEN trim(SUBSTRING(rating_event_name,4))
             ELSE NVL(rating_event_name,CAST(re_id AS STRING))
         END)
        , NVL(rating_event_service,CAST(re_id AS STRING))
        , NVL(rating_event_specific_tarif,CAST(re_id AS STRING))
        , NVL(yzdiscount,0)
        , result_code
        , CAST(result_code AS STRING)
        , 'IN_ZTE'
        , 'IT_ZTE_VOICE_SMS_POST'
        , CURRENT_TIMESTAMP
        ,VLR_NUMBER

 )
 RESULT_FT
