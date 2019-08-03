-- mise à part valorisation sur 1er sous-compte
INSERT  INTO MON.TT_DATA_DA_USAGE_DAILY
SELECT
    MSISDN,
    DA_NAME,
    MAX(NVL(ACCT_RES_RATING_UNIT, DA_UNIT)) DA_UNIT,
    MAX(NVL(ACCT_RES_RATING_TYPE, DA_TYPE)) DA_TYPE,
    SUM (CASE WHEN CHARGE >=0 THEN CHARGE ELSE 0 END) POSITIVE_CHARGE_AMOUNT,
    SUM (CASE WHEN CHARGE < 0 THEN CHARGE ELSE 0 END) NEGATIVE_CHARGE_AMOUNT,
    OPERATOR_CODE,
    CONTRACT_TYPE,
    COMMERCIAL_PROFILE,
    TARIFF_PLAN,
    TYPE_OF_MEASUREMENT,
    SERVICE_TYPE,
    SERVICE_ZONE,
    SDP_GOS_SERVICE,
    'FT_CRA_GPRS' SOURCE_TABLE,
    CURRENT_TIMESTAMP INSERT_DATE
    ,SESSION_DATE
FROM (
  SELECT
      CASE
        WHEN index1=0 THEN 'rating1'
        WHEN index1=1 THEN 'rating2'
        WHEN index1=2 THEN 'rating3'
        ELSE 'rating4'
      END rating_context
      , SESSION_DATE
      , null da_type
      , SERVED_PARTY_MSISDN msisdn
      , (CASE
            WHEN OPERATOR_CODE IS NULL THEN IF (SERVED_PARTY_MSISDN IS NULL,'OCM',FN_GET_OPERATOR_CODE(SERVED_PARTY_MSISDN))
            ELSE OPERATOR_CODE END
        ) operator_code
      , USED_BALANCE_LIST
      , Identif da_name
      , USED_VOLUME_LIST
      , CAST(charge AS BIGINT) charge
      , USED_UNIT_LIST
      , if(unit='',NULL,unit) da_unit
      , if(index1='',NULL,index1) index1
      , SERVED_PARTY_TYPE  contract_type
      , SERVED_PARTY_OFFER commercial_profile
      , SERVED_PARTY_PRICE_PLAN Tariff_Plan
      , UNIT_OF_MEASUREMENT type_of_measurement
      , SERVICE_TYPE
      , SERVICE_CODE SERVICE_ZONE
      , SDP_GOS_SERV_NAME SDP_GOS_SERVICE
  FROM (SELECT * FROM MON.FT_CRA_GPRS WHERE SESSION_DATE ='###SLICE_VALUE###') a
  LATERAL VIEW POSEXPLODE(SPLIT(nvl(USED_BALANCE_LIST,''), '\\|')) tmp1 AS index1, Identif
  LATERAL VIEW POSEXPLODE(SPLIT(nvl(USED_VOLUME_LIST,''), '\\|')) tmp2 AS index2, charge
  LATERAL VIEW POSEXPLODE(SPLIT(nvl(USED_UNIT_LIST,''), '\\|')) tmp3 AS index3, unit
  WHERE index1=index2 and index2=index3 AND length(Identif)  <>0
)a
LEFT JOIN (
  SELECT
    UPPER(ACCT_RES_NAME) ACCT_RES_NAME
    , max(ACCT_RES_RATING_TYPE) ACCT_RES_RATING_TYPE
    , max(ACCT_RES_RATING_UNIT) ACCT_RES_RATING_UNIT
  FROM DIM.DT_BALANCE_TYPE_ITEM
  GROUP BY UPPER(ACCT_RES_NAME)
) b ON a.DA_NAME = b.ACCT_RES_NAME
GROUP BY
    SESSION_DATE,
    MSISDN,
    DA_NAME,
    OPERATOR_CODE,
    CONTRACT_TYPE,
    COMMERCIAL_PROFILE,
    TARIFF_PLAN,
    TYPE_OF_MEASUREMENT,
    SERVICE_TYPE,
    SERVICE_ZONE,
    SDP_GOS_SERVICE
