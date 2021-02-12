INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
SELECT
     'PARC_OM_30Jrs' DESTINATION_CODE
     ,NULL  PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PARC_OM_30Jrs' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , NULL OPERATOR_CODE
     , count (distinct a.msisdn) TOTAL_AMOUNT
     , count (distinct a.msisdn)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , to_date('###SLICE_VALUE###') TRANSACTION_DATE
     ,'COMPUTE_KPI_OM_PARC' JOB_NAME
     , 'IT_OMNY_TRANSACTIONS' SOURCE_TABLE
FROM (
    SELECT
        DISTINCT  "###SLICE_VALUE###" EVENT_DATE, SENDER_MSISDN MSISDN
    FROM cdr.spark_IT_OMNY_TRANSACTIONS2
    WHERE TRANSFER_DATETIME BETWEEN date_sub("###SLICE_VALUE###", 29 )  AND "###SLICE_VALUE###"
        AND TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'MERCHPAY', 'BILLPAY', 'P2P', 'P2PNONREG','ENT2REG','RC') and SENDER_CATEGORY_CODE='SUBS'
    UNION
    SELECT
        DISTINCT "###SLICE_VALUE###" EVENT_DATE, RECEIVER_MSISDN MSISDN
    FROM cdr.spark_IT_OMNY_TRANSACTIONS2
    WHERE TRANSFER_DATETIME BETWEEN date_sub("###SLICE_VALUE###", 29 )  AND "###SLICE_VALUE###"
        AND TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'MERCHPAY', 'BILLPAY', 'P2P', 'P2PNONREG','ENT2REG','RC') AND RECEIVER_CATEGORY_CODE=='SUBS'
) A
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='###SLICE_VALUE###'
    ) b on a.msisdn = b.msisdn
    where a.event_date='###SLICE_VALUE###'
    group by a.msisdn
) site on  site.msisdn =a.MSISDN
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
REGION_ID
UNION ALL
SELECT
     'PARC_OM_7Jrs' DESTINATION_CODE
     ,NULL  PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PARC_OM_7Jrs' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , NULL OPERATOR_CODE
     , count (distinct a.msisdn) TOTAL_AMOUNT
     , count (distinct a.msisdn)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , to_date('###SLICE_VALUE###') TRANSACTION_DATE
     ,'COMPUTE_KPI_OM_PARC' JOB_NAME
     , 'IT_OMNY_TRANSACTIONS' SOURCE_TABLE
FROM (
    SELECT
        DISTINCT  "###SLICE_VALUE###" EVENT_DATE, SENDER_MSISDN MSISDN
    FROM cdr.spark_IT_OMNY_TRANSACTIONS2
    WHERE TRANSFER_DATETIME BETWEEN date_sub("###SLICE_VALUE###", 6 )  AND "###SLICE_VALUE###"
        AND TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'MERCHPAY', 'BILLPAY', 'P2P', 'P2PNONREG','ENT2REG','RC') and SENDER_CATEGORY_CODE='SUBS'
    UNION
    SELECT
        DISTINCT "###SLICE_VALUE###" EVENT_DATE, RECEIVER_MSISDN MSISDN
    FROM cdr.spark_IT_OMNY_TRANSACTIONS2
    WHERE TRANSFER_DATETIME BETWEEN date_sub("###SLICE_VALUE###", 6 )  AND "###SLICE_VALUE###"
        AND TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'MERCHPAY', 'BILLPAY', 'P2P', 'P2PNONREG','ENT2REG','RC') AND RECEIVER_CATEGORY_CODE=='SUBS'
) A
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='###SLICE_VALUE###'
    ) b on a.msisdn = b.msisdn
    where a.event_date='###SLICE_VALUE###'
    group by a.msisdn
) site on  site.msisdn =a.MSISDN
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
REGION_ID

union ALL

SELECT
     'PARC_OM_DAILY' DESTINATION_CODE
     , NULL  PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PARC_OM_DAILY' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , NULL OPERATOR_CODE
     , count (distinct a.msisdn) TOTAL_AMOUNT
     , count (distinct a.msisdn)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , to_date('###SLICE_VALUE###') TRANSACTION_DATE
     , 'COMPUTE_KPI_OM_PARC' JOB_NAME
     , 'IT_OMNY_TRANSACTIONS' SOURCE_TABLE
FROM (
    SELECT
        DISTINCT SENDER_MSISDN MSISDN
    FROM cdr.spark_IT_OMNY_TRANSACTIONS2
    WHERE TRANSFER_DATETIME = "###SLICE_VALUE###"
        AND TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'MERCHPAY', 'BILLPAY', 'P2P', 'P2PNONREG','ENT2REG','RC') and SENDER_CATEGORY_CODE='SUBS'
    UNION
    SELECT
        DISTINCT RECEIVER_MSISDN MSISDN
    FROM cdr.spark_IT_OMNY_TRANSACTIONS2
    WHERE TRANSFER_DATETIME = "###SLICE_VALUE###"
        AND TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'MERCHPAY', 'BILLPAY', 'P2P', 'P2PNONREG','ENT2REG','RC') AND RECEIVER_CATEGORY_CODE=='SUBS'
) A
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='###SLICE_VALUE###'
    ) b on a.msisdn = b.msisdn
    where a.event_date='###SLICE_VALUE###'
    group by a.msisdn
) site on  site.msisdn = a.MSISDN
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
REGION_ID

union all

SELECT
     'PARC_OM_MTD' DESTINATION_CODE
     , NULL  PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PARC_OM_MTD' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , NULL OPERATOR_CODE
     , count (distinct a.msisdn) TOTAL_AMOUNT
     , count (distinct a.msisdn)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , to_date('###SLICE_VALUE###') TRANSACTION_DATE
     ,'COMPUTE_KPI_OM_PARC' JOB_NAME
     , 'IT_OMNY_TRANSACTIONS' SOURCE_TABLE
FROM (
    SELECT
        DISTINCT  "###SLICE_VALUE###" EVENT_DATE, SENDER_MSISDN MSISDN
    FROM cdr.spark_IT_OMNY_TRANSACTIONS2
    WHERE TRANSFER_DATETIME BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
        AND TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'MERCHPAY', 'BILLPAY', 'P2P', 'P2PNONREG','ENT2REG','RC') and SENDER_CATEGORY_CODE='SUBS'
    UNION
    SELECT
        DISTINCT "###SLICE_VALUE###" EVENT_DATE, RECEIVER_MSISDN MSISDN
    FROM cdr.spark_IT_OMNY_TRANSACTIONS2
    WHERE TRANSFER_DATETIME BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
        AND TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'MERCHPAY', 'BILLPAY', 'P2P', 'P2PNONREG','ENT2REG','RC') AND RECEIVER_CATEGORY_CODE=='SUBS'
) A
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='###SLICE_VALUE###'
    ) b on a.msisdn = b.msisdn
    where a.event_date='###SLICE_VALUE###'
    group by a.msisdn
) site on  site.msisdn =a.MSISDN
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
REGION_ID
