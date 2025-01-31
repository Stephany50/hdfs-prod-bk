INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW

SELECT
     'PDV_OM_ACTIF_30Jrs' DESTINATION_CODE
     , null PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PDV_OM_ACTIF_30Jrs' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , null OPERATOR_CODE
     , COUNT (DISTINCT A.MSISDN) TOTAL_AMOUNT
     , COUNT (DISTINCT A.MSISDN) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     ,'COMPUTE_KPI_OM_PDV' JOB_NAME
     , 'IT_OMNY_TRANSACTIONS' SOURCE_TABLE
     , to_date('###SLICE_VALUE###') TRANSACTION_DATE
FROM 
(
    SELECT
        DISTINCT SENDER_MSISDN MSISDN
    FROM CDR.spark_IT_OMNY_TRANSACTIONS
    WHERE TRANSFER_DATETIME BETWEEN date_sub('###SLICE_VALUE###',29) AND '###SLICE_VALUE###' AND
        TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('CASHOUT','CASHIN', 'P2P', 'COUTBYCODE','INVC2C') AND SENDER_MSISDN NOT IN (SELECT MSISDN FROM backup_dwh.MSISDN_B2W UNION ALL
        SELECT MSISDN FROM backup_dwh.MSISDN_VISA UNION ALL SELECT MSISDN FROM backup_dwh.MSISDN_GIMAC) AND SENDER_CATEGORY_CODE<>'SUBS'
    UNION
    SELECT
        DISTINCT RECEIVER_MSISDN MSISDN
    FROM CDR.spark_IT_OMNY_TRANSACTIONS
    WHERE TRANSFER_DATETIME BETWEEN date_sub('###SLICE_VALUE###',29) AND '###SLICE_VALUE###' AND
        TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('CASHOUT','CASHIN', 'P2P', 'COUTBYCODE','INVC2C') AND RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM backup_dwh.MSISDN_B2W UNION ALL
        SELECT MSISDN FROM backup_dwh.MSISDN_VISA UNION ALL SELECT MSISDN FROM backup_dwh.MSISDN_GIMAC) AND RECEIVER_CATEGORY_CODE<>'SUBS'
 )A
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