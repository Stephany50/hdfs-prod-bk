 SELECT SEGMENTATION,sum(rated_amount) valeur
         FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
         LEFT JOIN DIM.DT_OFFER_PROFILES b ON  upper(a.PROFILE_CODE) =UPPER(b.PROFILE_CODE)
         where transaction_date ='2021-02-01'   and KPI='PARC' and DESTINATION_CODE = 'USER_30DAYS_GROUP' AND SEGMENTATION in ('Staff','B2B','B2C')
 group by SEGMENTATION


SELECT sum(rated_amount) valeur
 FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT a
 where transaction_date ='2021-02-03'  and DESTINATION_CODE = 'USER_30DAYS_GROUP'
 
 
    select
        NULL region_administrative,
        NULL region_commerciale,
        'Subscriber overview' category,
        'Tx users (30jrs) en %' KPI ,
        'Tx users (30jrs) en %' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        max(concat(a.source_table,'&',b.source_table)) source_table,
        'MOY' cummulable,
        max(a.valeur/nvl(b.valeur,1))*100 valeur
    FROM (
        select sum(rated_amount) valeur,max(source_table) source_table
        from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
        where transaction_date ='2021-02-01'   and KPI='PARC' and DESTINATION_CODE = 'USER_30DAYS_GROUP'
    )a
    left join (
        SELECT sum(rated_amount) valeur,max(source_table) source_table
        FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
        where transaction_date ='2021-02-01'   and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP'
    )b

select  profile2, sum(TOTAL_AMOUNT) from (
select

    sum(EFFECTIF) TOTAL_AMOUNT,
     (CASE
            WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
                1
            ELSE 0
          END) profile2
from MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY  a
where event_date =DATE_ADD('2021-02-01',1) and a.operator_code <> 'SET'
group by
    EVENT_DATE,
(CASE
        WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
            1
        ELSE 0
      END))d group by profile2
              

select
    'USER_GROUP' DESTINATION_CODE,
    PROFILE PROFILE_CODE,
    'UNKNOWN'  SERVICE_CODE,
    'PARC' KPI,
    'UNKNOWN' SUB_ACCOUNT,
    'HIT' MEASUREMENT_UNIT,
    NVL(prof.OPERATOR_CODE,'OCM')OPERATOR_CODE ,
    sum(EFFECTIF) TOTAL_AMOUNT,
    sum(EFFECTIF) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE,
    REGION_ID,
    DATE_SUB(event_date,1) TRANSACTION_DATE
    ,'COMPUTE_KPI_CUSTOMER_BASE' JOB_NAME
    ,'FT_GROUP_SUBSCRIBER_SUMMARY' SOURCE_TABLE
from MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY  a
LEFT JOIN DIM.DT_OFFER_PROFILES prof on upper(a.PROFILE) = prof.PROFILE_CODE
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (a.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region)), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
where event_date =DATE_ADD('2021-02-01',1) and a.operator_code <> 'SET'
 AND (CASE
        WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
            1
        ELSE 0
      END) = 0
group by
    EVENT_DATE,
    profile,
    NVL(prof.OPERATOR_CODE,'OCM'),
    region_id
    
select
    'USER_GROSS_ADD_SUBSCRIPTION' DESTINATION_CODE,
    COMMERCIAL_OFFER PROFILE_CODE,
    'UNKNOWN'  SERVICE_CODE,
    'PARC' KPI,
    'UNKNOWN' SUB_ACCOUNT,
    'HIT' MEASUREMENT_UNIT,
    NVL(OPERATOR_CODE,'OCM')OPERATOR_CODE ,
    count(distinct a.msisdn) TOTAL_AMOUNT,
    count(distinct a.msisdn) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE,
    REGION_ID,
    TRANSACTION_DATE
    ,'COMPUTE_KPI_CUSTOMER_BASE' JOB_NAME
    ,'FT_SUBSCRIPTION' SOURCE_TABLE
from (
    select
        TRANSACTION_DATE,
        SERVED_PARTY_MSISDN MSISDN,
        max(OPERATOR_CODE) OPERATOR_CODE,
        max(COMMERCIAL_OFFER) COMMERCIAL_OFFER
    from MON.SPARK_FT_SUBSCRIPTION
    where transaction_date='2021-02-01' and subscription_service like '%PPS%'
    group by TRANSACTION_DATE,SERVED_PARTY_MSISDN
) a
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2021-02-01'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2021-02-01'
    group by a.msisdn
) site on  site.msisdn =a.MSISDN
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
COMMERCIAL_OFFER,NVL(OPERATOR_CODE,'OCM'),REGION_ID,TRANSACTION_DATE


 select
        c.administrative_region region_administrative,
        c.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Net adds' KPI ,
        'Net adds' axe_vue_transversale ,
        null axe_revenu,
        'NET ADDS' axe_subscriber,
        source_table,
        'SUM' cummulable,
        sum(parcj1-parcj0) valeur
    from (
        select region_id,cast(sum(rated_amount) as bigint) parcj0,max(source_table) source_table from  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG where transaction_date= date_sub('2021-01-01',1)   and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP' group by region_id
    )a
    left join  (
        select region_id,cast(sum(rated_amount) as bigint) parcj1 from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG where transaction_date= '2021-01-01'    and KPI='PARC' and DESTINATION_CODE = 'USER_GROUP' group by region_id
    )b on a.region_id=b.region_id
    left join dim.spark_dt_regions_mkt_v2 c on a.region_id = c.region_id
    group by
    c.administrative_region ,
    c.commercial_region,
    source_table
