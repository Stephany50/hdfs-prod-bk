INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
---------- Revenue Orange Money

SELECT
     'REVENUE_OM' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'REVENUE_OM' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_MARKETING' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , SUM(nvl(REVENU,0)) TOTAL_AMOUNT
     , SUM(nvl(REVENU,0)) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , JOUR TRANSACTION_DATE
FROM MON.SPARK_DATAMART_OM_MARKETING2 B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
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
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE JOUR ='###SLICE_VALUE###' --and STYLE  IN ('RECHARGE','TOP_UP')
GROUP BY
    JOUR,
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID

UNION ALL

------ cash in OM


SELECT
     'CASH_IN_OM' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'CASH_IN_OM' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_MARKETING' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , SUM(nvl(val,0)) TOTAL_AMOUNT
     , SUM(nvl(val,0)) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , JOUR TRANSACTION_DATE
FROM MON.SPARK_DATAMART_OM_MARKETING2 B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
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
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE JOUR ='###SLICE_VALUE###' and service_type  IN ('CASHIN')
GROUP BY
    JOUR,
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID

UNION ALL

------ cash out OM


SELECT
     'CASH_OUT_OM' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'CASH_OUT_OM' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_MARKETING' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , SUM(nvl(val,0)) TOTAL_AMOUNT
     , SUM(nvl(val,0)) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , JOUR TRANSACTION_DATE
FROM MON.SPARK_DATAMART_OM_MARKETING2 B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
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
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE JOUR ='###SLICE_VALUE###' and service_type  IN ('CASHOUT')
GROUP BY
    JOUR,
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID


UNION ALL

------ cash MERCH


SELECT
     'MERCH_PAY_OM' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'MERCH_PAY_OM' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_MARKETING' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , SUM(nvl(val,0)) TOTAL_AMOUNT
     , SUM(nvl(val,0)) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , JOUR TRANSACTION_DATE
FROM MON.SPARK_DATAMART_OM_MARKETING2 B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
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
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE JOUR ='###SLICE_VALUE###' and service_type  IN ('MERCHPAY')
GROUP BY
    JOUR,
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID


UNION ALL

------ cash BILLPAY


SELECT
     'BILL_PAY_OM' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'BILL_PAY_OM' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_MARKETING' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , SUM(nvl(val,0)) TOTAL_AMOUNT
     , SUM(nvl(val,0)) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , JOUR TRANSACTION_DATE
FROM MON.SPARK_DATAMART_OM_MARKETING2 B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
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
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE JOUR ='###SLICE_VALUE###' and service_type  IN ('BILLPAY')
GROUP BY
    JOUR,
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID


UNION ALL
----------- Stock total client OM
SELECT
     'BALANCE_OM' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'BALANCE_OM' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'IT_OM_ALL_BALANCE' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , SUM(nvl(BALANCE,0)) TOTAL_AMOUNT
     , SUM(nvl(BALANCE,0)) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , ORIGINAL_FILE_DATE TRANSACTION_DATE
FROM CDR.SPARK_IT_OM_ALL_BALANCE B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.account_id
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
) site on  site.msisdn =B2.account_id
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE ORIGINAL_FILE_DATE ='###SLICE_VALUE###' AND USER_CATEGORY IN ('Subscriber')
GROUP BY
    ORIGINAL_FILE_DATE,
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID


UNION ALL

------------- -  Recharges (OM)

SELECT
     'REFILL_OM' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'REFILL_OM' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_MARKETING' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , SUM(nvl(VAL,0)) TOTAL_AMOUNT
     , SUM(nvl(VAL,0)) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , JOUR TRANSACTION_DATE
FROM MON.SPARK_DATAMART_OM_MARKETING2 B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
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
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE JOUR ='###SLICE_VALUE###' and STYLE  IN ('RECHARGE','TOP_UP')
GROUP BY
    JOUR,
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID

UNION ALL
-------- Users(30Jrs)


SELECT
     'PARC_OM_30Jrs' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PARC_OM_30Jrs' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_MARKETING' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , COUNT (DISTINCT b2.MSISDN) TOTAL_AMOUNT
     , COUNT (DISTINCT b2.MSISDN) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , to_date('###SLICE_VALUE###') TRANSACTION_DATE
FROM (select distinct msisdn from MON.SPARK_DATAMART_OM_MARKETING2 WHERE JOUR BETWEEN date_sub('###SLICE_VALUE###',30) and '###SLICE_VALUE###' AND STYLE NOT LIKE ('REC%') )B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
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
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID


UNION ALL
-------- Users(90Jrs)


SELECT
     'PARC_OM_90Jrs' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PARC_OM_90Jrs' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_MARKETING' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , COUNT (DISTINCT b2.MSISDN) TOTAL_AMOUNT
     , COUNT (DISTINCT b2.MSISDN) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , to_date('###SLICE_VALUE###') TRANSACTION_DATE
FROM (select distinct msisdn from MON.SPARK_DATAMART_OM_MARKETING2 WHERE JOUR BETWEEN date_sub('###SLICE_VALUE###',90) and '###SLICE_VALUE###' AND STYLE NOT LIKE ('REC%') )B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
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
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID

UNION ALL


---Nombre de Pos OM actif (MONDV_OM.DATAMART_OM_DISTRIB)


SELECT
     'PDV_OM_ACTIF_30Jrs' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PDV_OM_ACTIF_30Jrs' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_DISTRIB' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , COUNT (DISTINCT b2.MSISDN) TOTAL_AMOUNT
     , COUNT (DISTINCT b2.MSISDN) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , to_date('###SLICE_VALUE###') TRANSACTION_DATE
FROM (select DOD.* FROM
    MON.SPARK_DATAMART_OM_DISTRIB DOD
LEFT JOIN MON.SPARK_REF_OM_PRODUCTS2 RE
            ON DOD.MSISDN=RE.MSISDN
WHERE JOUR BETWEEN date_sub('###SLICE_VALUE###',30) and '###SLICE_VALUE###' AND RE.REF_DATE in (select max(ref_date) ref_date from MON.SPARK_REF_OM_PRODUCTS2 where ref_date<='###SLICE_VALUE###' )  AND PRODUCT_LINE='DISTRIBUTION'AND SERVICE_TYPE NOT LIKE 'P2P%'
)B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
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
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID