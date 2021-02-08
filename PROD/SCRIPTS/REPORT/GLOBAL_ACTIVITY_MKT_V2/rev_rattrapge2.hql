SELECT
   UPPER(PRICE_PLAN_NAME) COMMERCIAL_OFFER_CODE
   ,'OM_DATA' TRANSACTION_TYPE
   ,'MAIN' SUB_ACCOUNT
   ,'+' TRANSACTION_SIGN
   , 'ZTE' SOURCE_PLATFORM
   ,'FT_A_SUBSCRIPTION'  SOURCE_DATA
   , 'IN_TRAFFIC' SERVED_SERVICE
   , 'NVX_OM_DATA' SERVICE_CODE
   , 'DEST_ND' DESTINATION_CODE
   , NULL SERVED_LOCATION
   ,'HIT' MEASUREMENT_UNIT
   , SUM (SUBS_EVENT_RATED_COUNT) RATED_COUNT
   , SUM (SUBS_EVENT_RATED_COUNT) RATED_VOLUME
   , SUM (TOTAL_AMOUNT) TAXED_AMOUNT
   , SUM (TOTAL_AMOUNT-TOTAL_AMOUNT*0.1925) UNTAXED_AMOUNT
   , CURRENT_TIMESTAMP INSERT_DATE
   ,'REVENUE' TRAFFIC_MEAN
   , OPERATOR_CODE OPERATOR_CODE
   , NULL LOCATION_CI
   , DATECODE TRANSACTION_DATE
FROM
(
    SELECT  TRANSACTION_DATE AS DATECODE,
        SUBS_BENEFIT_NAME AS BENEFIT_NAME,
        COMMERCIAL_OFFER AS PRICE_PLAN_NAME,
        SUBS_EVENT_RATED_COUNT,
        SUBS_AMOUNT AS TOTAL_AMOUNT,
        SUBS_CHANNEL,
        SUBS_SERVICE,
        OPERATOR_CODE
    FROM AGG.SPARK_FT_A_SUBSCRIPTION
    WHERE TRANSACTION_DATE ='2021-01-01' and subs_channel='32' and upper(trim(subs_benefit_name))='RP DATA SHAPE_5120K'
)T
WHERE
    TOTAL_AMOUNT>0 --ne prendre que les évènements facturés
     AND NVL(UPPER(BENEFIT_NAME),'ND') not like 'PREPAID INDIVIDUAL FORFAIT%'--eliminer les souscriptions au forfait500,1000 et 5000
GROUP BY
     DATECODE
    ,UPPER(PRICE_PLAN_NAME)
    ,'OM_DATA'
    ,'MAIN'
    ,'+'
    , 'ZTE'
    ,'FT_A_SUBSCRIPTION'
    , 'IN_TRAFFIC'
    , 'NVX_OM_DATA'
    , 'DEST_ND'
    , NULL
    ,'HIT'
    ,'REVENUE'
    , OPERATOR_CODE







SELECT
    'OM_DATA' DESTINATION_CODE
    , COMMERCIAL_OFFER PROFILE_CODE
    , 'NVX_OM_DATA' SERVICE_CODE
    ,'REVENUE' KPI
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,OPERATOR_CODE
    ,SUM(SUBS_AMOUNT) TOTAL_AMOUNT
    ,SUM(SUBS_AMOUNT*data)  RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,REGION_ID
    ,TRANSACTION_DATE
    ,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
    ,'FT_A_SUBSCRIPTION' SOURCE_TABLE
FROM AGG.SPARK_FT_A_SUBSCRIPTION  ud
left join  (
    select event,
        (nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
        (nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
        (case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
        (case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur,
        nvl(DATA_BUNDLE,0) data
        from dim.dt_services
) events on upper(trim(ud.SUBS_BENEFIT_NAME)) = upper(trim(events.EVENT))
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (ud.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region)), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '2021-01-01'
    AND  upper(trim(subs_benefit_name))!='RP DATA SHAPE_5120K' and  subs_benefit_name  is not null and  subs_channel ='32' and data=1
GROUP BY TRANSACTION_DATE
    ,COMMERCIAL_OFFER
    ,OPERATOR_CODE
    ,REGION_ID