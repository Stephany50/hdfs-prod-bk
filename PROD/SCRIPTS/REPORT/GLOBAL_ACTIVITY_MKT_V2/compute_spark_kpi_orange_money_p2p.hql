INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG


----------- Stock total client OM
SELECT
     'VALEUR_P2P_OM' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'VALEUR_P2P_OM' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , C.OPERATOR_CODE OPERATOR_CODE
     , SUM(CASE WHEN UPPER(RECEIVER_GRADE_NAME) NOT LIKE '%INFORMEL%' THEN TRANSACTION_AMOUNT ELSE 0 END) TOTAL_AMOUNT
     , SUM(CASE WHEN UPPER(RECEIVER_GRADE_NAME) NOT LIKE '%INFORMEL%' THEN TRANSACTION_AMOUNT ELSE 0 END)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , ORIGINAL_FILE_DATE TRANSACTION_DATE
     ,'COMPUTE_KPI_OM_P2P' JOB_NAME
     , 'IT_OMNY_TRANSACTIONS' SOURCE_TABLE
FROM (

    SELECT *
    FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
    WHERE
        TRANSFER_DATETIME ='2021-02-03' AND
        TRANSFER_STATUS='TS' AND SUBSTR(TRANSFER_ID, 1, 2) IN ('PP') AND
        SENDER_CATEGORY_CODE = 'SUBS' AND
        RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM BACKUP_DWH.OM_TEST_MSISDNS) AND
        SENDER_MSISDN NOT IN (SELECT MSISDN FROM BACKUP_DWH.OM_TEST_MSISDNS)

) B2
LEFT JOIN ( SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
    WHERE EVENT_DATE in (select max(event_date) from MON.SPARK_FT_CONTRACT_SNAPSHOT where event_date>=date_sub('2021-02-03',6) )
    GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON nvl(C.ACCESS_KEY,'ND') = nvl(B2.SENDER_MSISDN,'ND')
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2021-02-03'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2021-02-03'
    group by a.msisdn
) site on  site.msisdn =B2.SENDER_MSISDN
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
    ORIGINAL_FILE_DATE,
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID

UNION ALL



----------- Stock total client OM
SELECT
     'VOLUME_P2P_OM' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'VOLUME_P2P_OM' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , C.OPERATOR_CODE OPERATOR_CODE
     , COUNT(CASE WHEN UPPER(RECEIVER_GRADE_NAME) NOT LIKE '%INFORMEL%' THEN 1 ELSE NULL END) TOTAL_AMOUNT
     , COUNT(CASE WHEN UPPER(RECEIVER_GRADE_NAME) NOT LIKE '%INFORMEL%' THEN 1 ELSE NULL END)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , ORIGINAL_FILE_DATE TRANSACTION_DATE
     ,'COMPUTE_KPI_OM_P2P' JOB_NAME
     , 'IT_OMNY_TRANSACTIONS' SOURCE_TABLE
FROM (

    SELECT *
    FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
    WHERE
        TRANSFER_DATETIME ='2021-02-03' AND
        TRANSFER_STATUS='TS' AND SUBSTR(TRANSFER_ID, 1, 2) IN ('PP') AND
        SENDER_CATEGORY_CODE = 'SUBS' AND
        RECEIVER_MSISDN NOT IN (SELECT MSISDN FROM BACKUP_DWH.OM_TEST_MSISDNS) AND
        SENDER_MSISDN NOT IN (SELECT MSISDN FROM BACKUP_DWH.OM_TEST_MSISDNS)

) B2
LEFT JOIN ( SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
    WHERE EVENT_DATE in (select max(event_date) from MON.SPARK_FT_CONTRACT_SNAPSHOT where event_date>=date_sub('2021-02-03',6) )
    GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON nvl(C.ACCESS_KEY,'ND') = nvl(B2.SENDER_MSISDN,'ND')
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2021-02-03'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2021-02-03'
    group by a.msisdn
) site on  site.msisdn =B2.SENDER_MSISDN
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
    ORIGINAL_FILE_DATE,
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID
