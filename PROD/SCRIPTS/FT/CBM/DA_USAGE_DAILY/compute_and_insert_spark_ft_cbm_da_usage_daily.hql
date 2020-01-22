-- alimentation en données VOIX-SMS
INSERT  INTO MON.SPARK_FT_CBM_DA_USAGE_DAILY
SELECT
    MSISDN MSISDN
    , DA_NAME DA_ID
    , DA_UNIT
    , DA_TYPE
    , (CASE WHEN USAGE = 'NVX_SMS' THEN 'SMS'
        WHEN USAGE = 'VOI_VOX' THEN 'VOICE'
        ELSE USAGE
      END )  ACTIVITY_TYPE
    , ROUND(SUM(CASE
            WHEN DA_UNIT = 'QM' THEN CAST(POSITIVE_CHARGE_AMOUNT AS DECIMAL(17,2))/100
            ELSE POSITIVE_CHARGE_AMOUNT
      END),0) USED_AMT
    , COMMERCIAL_PROFILE SERVICE_CLASS
    , 'FT_VOICE_SMS_DA_USAGE_DAILY' SOURCE_TABLE
    , CURRENT_TIMESTAMP INSERT_DATE
    ,TRANSACTION_DATE PERIOD
FROM MON.FT_VOICE_SMS_DA_USAGE_DAILY
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
  AND DA_NAME IS NOT NULL
  AND DA_NAME <> '0'
GROUP BY TRANSACTION_DATE
    , MSISDN
    , DA_NAME
    , DA_UNIT
    , DA_TYPE
    , (CASE
          WHEN USAGE = 'NVX_SMS' THEN 'SMS'
          WHEN USAGE = 'VOI_VOX' THEN 'VOICE'
          ELSE USAGE
      END)
    , COMMERCIAL_PROFILE
UNION ALL

SELECT 
  MSISDN MSISDN
  , DA_NAME DA_ID
  , DA_UNIT
  , DA_TYPE
  , 'DATA' ACTIVITY_TYPE
  , ROUND(SUM(CASE
          WHEN DA_UNIT = 'QM' THEN CAST(POSITIVE_CHARGE_AMOUNT AS DECIMAL(17,2))/100
          ELSE POSITIVE_CHARGE_AMOUNT
    END),0) USED_AMT
  , COMMERCIAL_PROFILE SERVICE_CLASS
  , 'FT_DATA_DA_USAGE_DAILY' SOURCE_TABLE
  , CURRENT_TIMESTAMP INSERT_DATE
  , SESSION_DATE PERIOD
FROM MON.SPARK_FT_DATA_DA_USAGE_DAILY
WHERE SESSION_DATE = '###SLICE_VALUE###'
  AND DA_NAME IS NOT NULL
  AND DA_NAME <> '0'
GROUP BY SESSION_DATE
    , MSISDN
    , DA_NAME
    , DA_UNIT
    , DA_TYPE
    , 'DATA'
    , COMMERCIAL_PROFILE
