INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG


SELECT
     'PDV_OM_ACTIF_30Jrs' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PDV_OM_ACTIF_30Jrs' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , C.OPERATOR_CODE OPERATOR_CODE
     , COUNT (DISTINCT b2.MSISDN) TOTAL_AMOUNT
     , COUNT (DISTINCT b2.MSISDN) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , to_date('###SLICE_VALUE###') TRANSACTION_DATE
     ,'COMPUTE_KPI_OM_PDV' JOB_NAME
     , 'DATAMART_OM_DISTRIB' SOURCE_TABLE
FROM (select DOD.* FROM
    MON.SPARK_DATAMART_OM_DISTRIB DOD
LEFT JOIN MON.SPARK_REF_OM_PRODUCTS2 RE
            ON DOD.MSISDN=RE.MSISDN
WHERE JOUR BETWEEN date_sub('###SLICE_VALUE###',30) and '###SLICE_VALUE###' AND RE.REF_DATE in (select max(ref_date) ref_date from MON.SPARK_REF_OM_PRODUCTS2 where ref_date<='###SLICE_VALUE###' )  AND PRODUCT_LINE='DISTRIBUTION'AND SERVICE_TYPE NOT LIKE 'P2P%'
)B2
LEFT JOIN ( SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
    WHERE EVENT_DATE in (select max(event_date) from MON.SPARK_FT_CONTRACT_SNAPSHOT where event_date>=date_sub('###SLICE_VALUE###',6) )
    GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON nvl(C.ACCESS_KEY,'ND') = nvl(B2.msisdn,'ND')
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
