INSERT INTO MON.TT_VOICE_SMS_DA_USAGE_DAILY
SELECT
    MSISDN,
    DA_NAME,
    MAX(NVL(ACCT_RES_RATING_UNIT, DA_UNIT)) DA_UNIT,
    MAX(NVL(ACCT_RES_RATING_TYPE, DA_TYPE)) DA_TYPE,
    SUM (CASE WHEN CHARGE >=0 THEN CHARGE ELSE 0 END) POSITIVE_CHARGE_AMOUNT,
    SUM (CASE WHEN CHARGE < 0 THEN CHARGE ELSE 0 END) NEGATIVE_CHARGE_AMOUNT,
    OPERATOR_CODE,
    COMMERCIAL_PROFILE,
    TARIFF_PLAN,
    OTHER_PARTY_ZONE,
    DESTINATION,
    USAGE,
    'FT_BILLED_TRANSACTION_PREPAID' SOURCE_TABLE,
     CURRENT_TIMESTAMP INSERT_DATE,
    TRANSACTION_DATE
FROM (

  select
      CASE
        WHEN index1=0 THEN 'rating1'
        WHEN index1=1 THEN 'rating2'
        WHEN index1=2 THEN 'rating3'
        ELSE 'rating4'
      END rating_context
      , transaction_date
      , null da_type
      , CHARGED_PARTY msisdn
      , (CASE
            WHEN OPERATOR_CODE IS NULL THEN IF (CHARGED_PARTY IS NULL,'OCM',FN_GET_OPERATOR_CODE(CHARGED_PARTY))
            ELSE OPERATOR_CODE END
        ) operator_code
      , Identifier_list
      , identif da_name
      , Volume_List
      , CAST(charge AS BIGINT) charge
      , Unit_Of_Measurement_List
      , if(unit='',NULL,unit) da_unit
      , commercial_profile
      , RAW_TARIFF_PLAN Tariff_Plan
      , Other_Party_Zone
      , (CASE
            WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
            WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
            WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
            WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
            WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
            WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
            WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
            WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
            WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
            WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
            WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
            WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
            ELSE 'AUT'
        END ) Usage
      , (CASE
            WHEN a.Call_Destination_Code IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
            WHEN a.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
            WHEN a.Call_Destination_Code IN ('NEXTTEL') THEN 'OUT_NAT_MOB_NEX'
            WHEN a.Call_Destination_Code IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
            WHEN a.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
            WHEN a.Call_Destination_Code = 'VAS' THEN 'OUT_SVA'
            WHEN a.Call_Destination_Code = 'EMERG' THEN 'OUT_CCSVA'
            WHEN a.Call_Destination_Code = 'OCRMG' THEN 'OUT_ROAM_MO'
            WHEN a.Call_Destination_Code = 'INT' THEN 'OUT_INT'
            WHEN a.Call_Destination_Code = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
            WHEN a.Call_Destination_Code = 'TCRMG' THEN 'OUT_ROAM_MT'
            ELSE Call_Destination_Code
        END ) Destination
  from (SELECT * FROM MON.FT_BILLED_TRANSACTION_PREPAID where TRANSACTION_DATE ='###SLICE_VALUE###' ) a
  LATERAL VIEW POSEXPLODE(SPLIT(nvl(Identifier_list,''), '\\|')) tmp1 AS index1, Identif
  LATERAL VIEW POSEXPLODE(SPLIT(nvl(Volume_List,''), '\\|')) tmp2 AS index2, charge
  LATERAL VIEW POSEXPLODE(SPLIT(nvl(Unit_Of_Measurement_List,''), '\\|')) tmp3 AS index3, unit
  WHERE index1=index2 and index2=index3 AND length(Identif)  <>0
) a
LEFT JOIN (
  SELECT
    UPPER(ACCT_RES_NAME) ACCT_RES_NAME
    , max(ACCT_RES_RATING_TYPE) ACCT_RES_RATING_TYPE
    , max(ACCT_RES_RATING_UNIT) ACCT_RES_RATING_UNIT
  FROM DIM.DT_BALANCE_TYPE_ITEM
  GROUP BY UPPER(ACCT_RES_NAME)
) b ON  a.DA_NAME = b.ACCT_RES_NAME
GROUP BY TRANSACTION_DATE,
    MSISDN,
    DA_NAME,
    OPERATOR_CODE,
    COMMERCIAL_PROFILE,
    TARIFF_PLAN,
    OTHER_PARTY_ZONE,
    DESTINATION,
    USAGE
