    -- Revenu GPRS PAYGO MAIN
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_CELL
SELECT
     TRANSACTION_TYPE DESTINATION_CODE
    , COMMERCIAL_OFFER_CODE PROFILE_CODE
    , SERVICE_CODE
    , KPI
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,'FT_CREDIT_TRANSFER' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(TAXED_AMOUNT) TOTAL_AMOUNT
    ,SUM(TAXED_AMOUNT) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,'2020-04-29' TRANSACTION_DATE
FROM(
    SELECT
        COMMERCIAL_OFFER COMMERCIAL_OFFER_CODE
         , sender_msisdn
         , 'P2P_FEES' TRANSACTION_TYPE
         , 'NVX_P2P' SERVICE_CODE
         , 'REVENUE' KPI
         , SUM (TRANSFER_FEES) TAXED_AMOUNT
         , SENDER_OPERATOR_CODE OPERATOR_CODE
         , REFILL_DATE TRANSACTION_DATE
    FROM  MON.SPARK_FT_CREDIT_TRANSFER
    WHERE REFILL_DATE = '2020-04-29' AND TERMINATION_IND = '000'
    GROUP BY
        REFILL_DATE
           , COMMERCIAL_OFFER
           , SENDER_OPERATOR_CODE
           , sender_msisdn
    UNION
    SELECT
        COMMERCIAL_OFFER COMMERCIAL_OFFER_CODE
         , sender_msisdn
         , 'P2P' TRANSACTION_TYPE
         , 'NVX_P2P_TRANS' SERVICE_CODE
         , 'AIRTIME' KPI
         , SUM (TRANSFER_AMT) TAXED_AMOUNT
         , SENDER_OPERATOR_CODE OPERATOR_CODE
         , REFILL_DATE TRANSACTION_DATE
    FROM  MON.SPARK_FT_CREDIT_TRANSFER A

    WHERE REFILL_DATE = '2020-04-29' AND TERMINATION_IND = '000'
    GROUP BY
        REFILL_DATE
           , COMMERCIAL_OFFER
           , SENDER_OPERATOR_CODE
           , sender_msisdn
) A
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-04-29'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-04-29'
    group by a.msisdn
) site on  site.msisdn =GET_NNP_MSISDN_9DIGITS(A.sender_msisdn)
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
   TRANSACTION_TYPE
    , COMMERCIAL_OFFER_CODE
    , SERVICE_CODE
    , KPI
    , OPERATOR_CODE
    , REGION_ID