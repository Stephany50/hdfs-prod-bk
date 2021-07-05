INSERT INTO AGG.SPARK_FT_A_REPORTING_360
SELECT
    ADMINISTRATIVE_REGION,
    COMMERCIAL_REGION,
    (
        case
            when KPI_NAME = 'DATA_VIA_OM' then 'REVENUE PER STREAM'
            else 'REFILL'
        end
    ) KPI_GROUP_NAME,
    KPI_NAME,
    KPI_VALUE VALUE,
    current_timestamp() INSERT_DATE,
    '###SLICE_VALUE###' PROCESSING_DATE
from
(
    select
        case
            WHEN KPI = 'VALEUR_AIRTIME' THEN 'EVD_TOTAL'
            WHEN KPI = 'REFILL_SELF_TOP' THEN 'ORANGE_MONEY_REFILL'
            WHEN KPI = 'DATA_VIA_OM' THEN 'DATA_VIA_OM'
        end KPI_NAME,
        sum(TOTAL_AMOUNT) KPI_VALUE,
        REGION_ID
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
    where transaction_date = '###SLICE_VALUE###'
        and KPI in ('VALEUR_AIRTIME', 'REFILL_SELF_TOP', 'DATA_VIA_OM')
    group by region_id,
        case
            WHEN KPI = 'VALEUR_AIRTIME' THEN 'EVD_TOTAL'
            WHEN KPI = 'REFILL_SELF_TOP' THEN 'ORANGE_MONEY_REFILL'
            WHEN KPI = 'DATA_VIA_OM' THEN 'DATA_VIA_OM'
        end
    
    union all

    select
        'SELF_TOP_UP' KPI_NAME,
        sum(transaction_amount) KPI_VALUE,
        cast(r.region_id as string) REGION_ID
    FROM  (
        SELECT * FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
        WHERE transfer_datetime='###SLICE_VALUE###' AND 
            service_type='RC' and 
            transfer_status='TS' and 
            OTHER_MSISDN IS NOT NULL AND 
            GET_NNP_MSISDN_9DIGITS(OTHER_MSISDN)=GET_NNP_MSISDN_9DIGITS(SENDER_MSISDN) AND 
            SENDER_CATEGORY_CODE='SUBS'
    ) a
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
    ) site on GET_NNP_MSISDN_9DIGITS(a.sender_msisdn) = GET_NNP_MSISDN_9DIGITS(site.msisdn)
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
    group by r.region_id

) A
LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B
ON A.REGION_ID = B.REGION_ID