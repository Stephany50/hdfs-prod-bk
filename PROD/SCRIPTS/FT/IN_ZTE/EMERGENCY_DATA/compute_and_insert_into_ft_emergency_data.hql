INSERT INTO MON.FT_EMERGENCY_DATA PARTITION(TRANSACTION_DATE)
SELECT
    MSISDN,
    TRANSACTION_TIME,
    AMOUNT,
    TRANSACTION_TYPE,
    BYTES_OBTAINED,
    CONTACT_CHANNEL,
    ORIGINAL_FILE_DATE,
    ORIGINAL_FILE_NAME,
    CURRENT_TIMESTAMP INSERT_DATE,
    SOURCE_INSERT_DATE,
    OFFER_PROFILE_CODE,
    (CASE
        WHEN OPERATOR_CODE_TRANSFORMED='OCM' THEN
            (CASE
                WHEN LENGTH (MSISDN_OPERATOR_CODE_TRANSFORMED) = 9 AND SUBSTR(MSISDN_OPERATOR_CODE_TRANSFORMED,1,3) = '692' THEN 'SET'
                WHEN LENGTH (MSISDN_OPERATOR_CODE_TRANSFORMED) = 8 AND SUBSTR(MSISDN_OPERATOR_CODE_TRANSFORMED,1,2) = '92' THEN 'SET'
                ELSE 'OCM'
             END)
        ELSE 'UNKNOWN_OPERATOR'
     END) OPERATOR_CODE,
     TRANSACTION_DATE
 FROM (
    SELECT
        (CASE
            WHEN LENGTH(MSISDN_TRANSFORMED) = 9
            AND  SUBSTR(MSISDN_TRANSFORMED, 1, 1) NOT IN ('0', '2')
                THEN MSISDN_TRANSFORMED
            WHEN SUBSTR(REGEXP_REPLACE(MSISDN_TRANSFORMED,"^0+(?!$)",""), 1, 3) = '237'
            AND LENGTH(REGEXP_REPLACE(MSISDN_TRANSFORMED,"^0+(?!$)","")) > 3
                THEN SUBSTR(REGEXP_REPLACE(MSISDN_TRANSFORMED,"^0+(?!$)",""), 4)
            ELSE IF(REGEXP_REPLACE(MSISDN_TRANSFORMED,"^0+(?!$)","") ="0", NULL, REGEXP_REPLACE(MSISDN_TRANSFORMED,"^0+(?!$)",""))
        END) MSISDN,
        TRANSACTION_DATE,
        AMOUNT,
        TRANSACTION_TYPE,
        FEE BYTES_OBTAINED,
        CONTACT_CHANNEL,
        ORIGINAL_FILE_DATE,
        ORIGINAL_FILE_NAME,
        INSERT_DATE SOURCE_INSERT_DATE,
        OFFER_PROFILE_CODE,
        (CASE
            WHEN LENGTH (MSISDN_OPERATOR_CODE_TRANSFORMED) = 8 THEN
                (CASE
                    WHEN (substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,1) = '9') OR (substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,2) IN ('55', '56', '57', '58', '59')) THEN 'OCM'
                    WHEN (substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,1) = '7') OR (substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,2) IN ('50', '51', '52', '53', '54','80','81','82','83'))  THEN 'MTN'
                    WHEN substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,1) = '6' OR substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,2) = '85'  THEN 'VIETTEL'
                    WHEN substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,1) in ('2','3') THEN 'CAMTEL'
                    ELSE 'INTERNATIONAL_CMR'
                 END)
            WHEN LENGTH (MSISDN_OPERATOR_CODE_TRANSFORMED) = 9 THEN
                (CASE
                    WHEN (substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,2) = '69') OR (substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,3) IN ('655', '656', '657', '658', '659')) THEN 'OCM'
                    WHEN (substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,2) = '67') OR (substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,3) IN ('650', '651', '652', '653', '654','680','681','682','683')) THEN 'MTN'
                    WHEN substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,2) = '66' or substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,3) = '685'  THEN 'VIETTEL'
                    WHEN substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,3) in ('243','242','222','233') THEN 'CAMTEL'
                    WHEN substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,2) = '62' THEN 'CAMTEL_MOB'
                    ELSE 'INTERNATIONAL_CMR'
                 END)
            WHEN LENGTH (MSISDN_OPERATOR_CODE_TRANSFORMED) = 13 AND substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,3)= '160' THEN
                (CASE
                    WHEN (substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,4) = '1602') THEN 'OCM'
                    WHEN (substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,4) = '1601') THEN 'MTN'
                    WHEN (substr(MSISDN_OPERATOR_CODE_TRANSFORMED,1,4) = '1603')  THEN 'VIETTEL'
                    ELSE 'INTERNATIONAL_CMR'
                 END)
            WHEN LENGTH (MSISDN_OPERATOR_CODE_TRANSFORMED) > 9 THEN 'INTERNATIONAL'
            ELSE 'OCM_SHORT'
        END) OPERATOR_CODE_TRANSFORMED,
        MSISDN_OPERATOR_CODE_TRANSFORMED,
        TRANSACTION_TIME
    FROM
     (
        SELECT
        (CASE
            WHEN a.MSISDN IN ('44534952454D494E4445', '534D5350415243')
                THEN '99999999'
            WHEN LENGTH (a.MSISDN) > 8
                THEN IF(REGEXP_EXTRACT(a.MSISDN,'[0-9]{9,}', 0) = '', a.MSISDN, REGEXP_EXTRACT(a.MSISDN,'[0-9]{9,}', 0))
            ELSE NVL(REGEXP_EXTRACT(a.MSISDN,'[0-9]+', 0),a.MSISDN)
        END) MSISDN_TRANSFORMED,
        a.TRANSACTION_DATE TRANSACTION_DATE,
        a.AMOUNT/100 AMOUNT,
        a.TRANSACTION_TYPE TRANSACTION_TYPE,
        a.FEE FEE,
        a.CONTACT_CHANNEL CONTACT_CHANNEL,
        max(NVL(b.PROFILE, b.OSP_CUSTOMER_FORMULE)) OFFER_PROFILE_CODE,
        max(CASE
                WHEN MSISDN IN ('44534952454D494E4445', '534D5350415243')
                    THEN '99999999'
                WHEN LENGTH(NVL(REGEXP_EXTRACT(MSISDN,'[0-9]+', 0),MSISDN)) = 9
                AND  SUBSTR(NVL(REGEXP_EXTRACT(MSISDN,'[0-9]+', 0),MSISDN), 1, 1) NOT IN ('0', '2')
                    THEN NVL(REGEXP_EXTRACT(MSISDN,'[0-9]+', 0),MSISDN)
                WHEN SUBSTR(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN,'[0-9]+', 0),MSISDN),"^0+(?!$)",""), 1, 3) = '237'
                AND LENGTH(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN,'[0-9]+', 0),MSISDN),"^0+(?!$)","")) > 3
                    THEN SUBSTR(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN,'[0-9]+', 0),MSISDN),"^0+(?!$)",""), 4)
                ELSE IF(REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN,'[0-9]+', 0),MSISDN),"^0+(?!$)","") ="0", NULL, REGEXP_REPLACE(NVL(REGEXP_EXTRACT(MSISDN,'[0-9]+', 0),MSISDN),"^0+(?!$)",""))
            END) MSISDN_OPERATOR_CODE_TRANSFORMED,
        max(a.ORIGINAL_FILE_DATE) ORIGINAL_FILE_DATE,
        max(a.ORIGINAL_FILE_NAME) ORIGINAL_FILE_NAME,
        max(a.INSERT_DATE) INSERT_DATE,
        MAX(TRANSACTION_TIME) TRANSACTION_TIME
        FROM CDR.IT_ZTE_EMERGENCY_DATA a
        LEFT JOIN (SELECT * FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE in (select max(event_date) FROM MON.SPARK_FT_CONTRACT_SNAPSHOT where event_date>date_sub('###SLICE_VALUE###',20)) AND OSP_STATUS <> 'TERMINATED') b ON IF(regexp_replace( a.MSISDN,"^(237)+(?!$)","")='237',NULL,regexp_replace( a.MSISDN,"^(237)+(?!$)",""))= b.ACCESS_KEY
        WHERE TRANSACTION_DATE='###SLICE_VALUE###'
        GROUP BY
            MSISDN,
            TRANSACTION_DATE,
            TRANSACTION_TIME,
            AMOUNT,
            TRANSACTION_TYPE,
            FEE,
            CONTACT_CHANNEL
    )T1
)T2;

