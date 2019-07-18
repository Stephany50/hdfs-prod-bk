add jar hdfs:///PROD/UDF/hive-udf-1.0.jar;
CREATE TEMPORARY FUNCTION FN_GET_OPERATOR_CODE as 'cm.orangbigdata.udf.GetOperatorCode';


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
      'rating1' rating_context
      , msisdn
      , identif1 da_name
      , unit1 da_unit
      , null da_type
      , CAST(charge1 AS INT) charge
      , operator_code
      , contract_type
      , commercial_profile
      , Tariff_Plan
      , Type_of_measurement
      , service_type
      , service_zone
      , sdp_gos_service
      , 'FT_CRA_GPRS' source_table
      , session_date
  FROM
  (
      select  SESSION_DATE
          , SERVED_PARTY_MSISDN msisdn
          , (CASE WHEN OPERATOR_CODE IS NULL THEN IF (SERVED_PARTY_MSISDN IS NULL,'OCM',FN_GET_OPERATOR_CODE(SERVED_PARTY_MSISDN))
                          ELSE OPERATOR_CODE END
                     ) operator_code
          , USED_BALANCE_LIST
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[0] Identif1
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[1] Identif2
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[2] Identif3
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[3] Identif4
          , USED_VOLUME_LIST
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[0] charge1
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[1] charge2
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[2] charge3
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[3] charge4
          , USED_UNIT_LIST
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[0] unit1
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[1] unit2
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[2] unit3
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[3] unit4
          , SERVED_PARTY_TYPE  contract_type
          , SERVED_PARTY_OFFER commercial_profile
          , SERVED_PARTY_PRICE_PLAN Tariff_Plan
          , UNIT_OF_MEASUREMENT type_of_measurement
          , SERVICE_TYPE
          , SERVICE_CODE SERVICE_ZONE
          , SDP_GOS_SERV_NAME SDP_GOS_SERVICE
      FROM MON.FT_CRA_GPRS a
      WHERE SESSION_DATE ='2019-06-19'
  )T
  WHERE length(Identif1) <> 0
      --AND mauvais_ticket = 'N'
  UNION ALL
  -- mise à part valorisation sur 2e sous-compte
  --INSERT /*APPEND*/ INTO MON.TT_DATA_DA_USAGE_DAILY
  SELECT
      'rating2' rating_context
      , msisdn
      , identif2 da_name
      , unit2 da_unit
      , null da_type
      , CAST(charge2 AS INT) charge
      , operator_code
      , contract_type
      , commercial_profile
      , Tariff_Plan
      , Type_of_measurement
      , service_type
      , service_zone
      , sdp_gos_service
      , 'FT_CRA_GPRS' source_table
      , session_date
  FROM
  (
      select
          SESSION_DATE
          , SERVED_PARTY_MSISDN msisdn
          , (CASE WHEN OPERATOR_CODE IS NULL THEN IF (SERVED_PARTY_MSISDN IS NULL,'OCM',FN_GET_OPERATOR_CODE(SERVED_PARTY_MSISDN))
                          ELSE OPERATOR_CODE END
                     ) operator_code
          , USED_BALANCE_LIST
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[0] Identif1
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[1] Identif2
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[2] Identif3
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[3] Identif4
          , USED_VOLUME_LIST
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[0] charge1
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[1] charge2
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[2] charge3
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[3] charge4
          , USED_UNIT_LIST
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[0] unit1
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[1] unit2
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[2] unit3
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[3] unit4
          , SERVED_PARTY_TYPE  contract_type
          , SERVED_PARTY_OFFER commercial_profile
          , SERVED_PARTY_PRICE_PLAN Tariff_Plan
          , UNIT_OF_MEASUREMENT type_of_measurement
          , SERVICE_TYPE
          , SERVICE_CODE SERVICE_ZONE
          , SDP_GOS_SERV_NAME SDP_GOS_SERVICE
      FROM MON.FT_CRA_GPRS a
      WHERE SESSION_DATE = '2019-06-19'
  )T
  WHERE length(Identif2) <> 0
      --AND mauvais_ticket = 'N'

  UNION ALL
  -- mise à part valorisation sur 3e sous-compte
  --INSERT /*APPEND*/ INTO MON.TT_DATA_DA_USAGE_DAILY
  SELECT
      'rating3' rating_context
      , msisdn
      , identif3 da_name
      , unit3 da_unit
      , null da_type
      , CAST(charge3 AS INT) charge
      , operator_code
      , contract_type
      , commercial_profile
      , Tariff_Plan
      , Type_of_measurement
      , service_type
      , service_zone
      , sdp_gos_service
      , 'FT_CRA_GPRS' source_table
      , session_date
  FROM
  (
      select
          SESSION_DATE
          , SERVED_PARTY_MSISDN msisdn
          , (CASE WHEN OPERATOR_CODE IS NULL THEN IF (SERVED_PARTY_MSISDN IS NULL,'OCM',FN_GET_OPERATOR_CODE(SERVED_PARTY_MSISDN))
                          ELSE OPERATOR_CODE END
                     ) operator_code
          , USED_BALANCE_LIST
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[0] Identif1
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[1] Identif2
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[2] Identif3
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[3] Identif4
          , USED_VOLUME_LIST
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[0] charge1
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[1] charge2
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[2] charge3
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[3] charge4
          , USED_UNIT_LIST
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[0] unit1
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[1] unit2
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[2] unit3
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[3] unit4
          , SERVED_PARTY_TYPE  contract_type
          , SERVED_PARTY_OFFER commercial_profile
          , SERVED_PARTY_PRICE_PLAN Tariff_Plan
          , UNIT_OF_MEASUREMENT type_of_measurement
          , SERVICE_TYPE
          , SERVICE_CODE SERVICE_ZONE
          , SDP_GOS_SERV_NAME SDP_GOS_SERVICE
      FROM MON.FT_CRA_GPRS a
      WHERE SESSION_DATE ='2019-06-19'
  )T
  WHERE length(Identif3) <> 0
      --AND mauvais_ticket = 'N'

  UNION ALL
  -- mise à part valorisation sur 4e sous-compte
  --INSERT /*APPEND*/ INTO MON.TT_DATA_DA_USAGE_DAILY
  SELECT
      'rating4' rating_context
      , msisdn
      , identif4 da_name
      , unit4 da_unit
      , null da_type
      , CAST(charge4 AS INT) charge
      , operator_code
      , contract_type
      , commercial_profile
      , Tariff_Plan
      , Type_of_measurement
      , service_type
      , service_zone
      , sdp_gos_service
      , 'FT_CRA_GPRS' source_table
      , session_date
  FROM
  (
      select
          SESSION_DATE
          , SERVED_PARTY_MSISDN msisdn
          , (CASE WHEN OPERATOR_CODE IS NULL THEN IF(SERVED_PARTY_MSISDN IS NULL,'OCM',FN_GET_OPERATOR_CODE(SERVED_PARTY_MSISDN))
                          ELSE OPERATOR_CODE END
                     ) operator_code
          , USED_BALANCE_LIST
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[0] Identif1
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[1] Identif2
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[2] Identif3
          , SPLIT(nvl(USED_BALANCE_LIST,''),'\\|')[3] Identif4
          , USED_VOLUME_LIST
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[0] charge1
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[1] charge2
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[2] charge3
          , SPLIT(nvl(USED_VOLUME_LIST,''),'\\|')[3] charge4
          , USED_UNIT_LIST
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[0] unit1
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[1] unit2
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[2] unit3
          , SPLIT(nvl(USED_UNIT_LIST,''),'\\|')[3] unit4
          , SERVED_PARTY_TYPE  contract_type
          , SERVED_PARTY_OFFER commercial_profile
          , SERVED_PARTY_PRICE_PLAN Tariff_Plan
          , UNIT_OF_MEASUREMENT type_of_measurement
          , SERVICE_TYPE
          , SERVICE_CODE SERVICE_ZONE
          , SDP_GOS_SERV_NAME SDP_GOS_SERVICE
      FROM MON.FT_CRA_GPRS a
      WHERE SESSION_DATE = '2019-06-19'
  )T
  WHERE length(Identif4) <> 0
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


-- update de Dedicated Account non mappés avec le référentiel
MERGE INTO MON.TT_DATA_DA_USAGE_DAILY a
USING DIM.DT_BALANCE_TYPE_ITEM  b
ON (CASE
    WHEN (SELECT UPPER(ACCT_RES_NAME) FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_ITEM_TYPE_ID = DA_NAME) IS NULL THEN DA_NAME
    ELSE (SELECT UPPER(ACCT_RES_NAME) FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_ITEM_TYPE_ID = DA_NAME)
END )
,(CASE
    WHEN (SELECT UPPER(ACCT_RES_RATING_UNIT) FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_ITEM_TYPE_ID = DA_NAME) IS NULL THEN DA_UNIT
    ELSE (SELECT UPPER(ACCT_RES_RATING_UNIT) FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_ITEM_TYPE_ID = DA_NAME)
END )
, (CASE
      WHEN (SELECT UPPER(ACCT_RES_RATING_TYPE) FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_ITEM_TYPE_ID = DA_NAME) IS NULL THEN DA_TYPE
      ELSE (SELECT UPPER(ACCT_RES_RATING_TYPE) FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_ITEM_TYPE_ID = DA_NAME)
  END )

SET DA_NAME = (CASE
                  WHEN (SELECT UPPER(ACCT_RES_NAME) FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_ITEM_TYPE_ID = DA_NAME) IS NULL THEN DA_NAME
                  ELSE (SELECT UPPER(ACCT_RES_NAME) FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_ITEM_TYPE_ID = DA_NAME)
              END )
  , DA_UNIT = (CASE
                  WHEN (SELECT UPPER(ACCT_RES_RATING_UNIT) FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_ITEM_TYPE_ID = DA_NAME) IS NULL THEN DA_UNIT
                  ELSE (SELECT UPPER(ACCT_RES_RATING_UNIT) FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_ITEM_TYPE_ID = DA_NAME)
              END )
  , DA_TYPE = (CASE
                  WHEN (SELECT UPPER(ACCT_RES_RATING_TYPE) FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_ITEM_TYPE_ID = DA_NAME) IS NULL THEN DA_TYPE
                  ELSE (SELECT UPPER(ACCT_RES_RATING_TYPE) FROM DIM.DT_BALANCE_TYPE_ITEM WHERE ACCT_ITEM_TYPE_ID = DA_NAME)
              END )
WHERE DA_NAME IN(
  SELECT DISTINCT UPPER(a.DA_NAME) DA_NAME FROM MON.TT_DATA_DA_USAGE_DAILY a
  LEFT JOIN (SELECT DISTINCT UPPER(ACCT_RES_NAME) DA_NAME FROM DIM.DT_BALANCE_TYPE_ITEM )b
  WHERE  a.DA_NAME IS NOT NULL AND b.DA_NAME IS NULL
)

-- update de Service Zone non mappés avec le référentiel
UPDATE FT_DATA_DA_USAGE_DAILY e
SET SERVICE_ZONE = (CASE WHEN (SELECT UPPER(RATING_EVENT_NAME) FROM DIM.DT_RATING_EVENT WHERE RATING_EVENT_ID = SERVICE_ZONE) IS NULL THEN SERVICE_ZONE
                                                  ELSE (SELECT UPPER(RATING_EVENT_NAME) FROM DIM.DT_RATING_EVENT WHERE RATING_EVENT_ID = SERVICE_ZONE)
                                                  END )
WHERE SERVICE_ZONE IN
  (SELECT DISTINCT UPPER(SERVICE_ZONE) FROM FT_DATA_DA_USAGE_DAILY WHERE SESSION_DATE = TO_DATE(s_slice_value,'yyyymmdd') AND SERVICE_ZONE IS NOT NULL
  MINUS
  SELECT DISTINCT UPPER(RATING_EVENT_NAME) FROM DIM.DT_RATING_EVENT )
AND SESSION_DATE = TO_DATE(s_slice_value,'yyyymmdd')
;
COMMIT;

-- update de Service Type non mappés avec le référentiel
MERGE INTO  TT_DATA_DA_USAGE_DAILY e
SET SERVICE_TYPE = (
CASE
  WHEN (SELECT UPPER(RATING_SERVICE_TYPE) FROM DIM.DT_RATING_SERVICE_GROUP WHERE RATING_GROUP_ID = SERVICE_TYPE) IS NULL THEN SERVICE_TYPE
  ELSE (SELECT UPPER(RATING_SERVICE_TYPE) FROM DIM.DT_RATING_SERVICE_GROUP WHERE RATING_GROUP_ID = SERVICE_TYPE)
  END )
WHERE SERVICE_TYPE IN
  (SELECT DISTINCT UPPER(SERVICE_TYPE) FROM FT_DATA_DA_USAGE_DAILY WHERE SESSION_DATE = TO_DATE(s_slice_value,'yyyymmdd') AND SERVICE_TYPE IS NOT NULL
  MINUS
  SELECT DISTINCT UPPER(RATING_SERVICE_TYPE) FROM DIM.DT_RATING_SERVICE_GROUP )
AND SESSION_DATE = TO_DATE(s_slice_value,'yyyymmdd')
