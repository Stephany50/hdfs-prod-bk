INSERT INTO AGG.SPARK_FT_A_REPORTING_360
SELECT 
NVL(ADMINISTRATIVE_REGION, 'INCONNU') ADMINISTRATIVE_REGION,
NVL(COMMERCIAL_REGION, 'INCONNU') COMMERCIAL_REGION,
'REVENUE PER STREAM' KPI_GROUP_NAME,
REPLACE(REPLACE(REPLACE(REPLACE(UPPER(KPI_NAME), 'Ã','E'), 'Ê', 'E'), 'É',  'E'), 'È', 'E') KPI_NAME, 
VALUE,
CURRENT_TIMESTAMP INSERT_DATE,
'###SLICE_VALUE###' PROCESSING_DATE  
FROM (

    --GOS SVA ; rachat de validité ; sos credit fees ; trafic crbt ; orange célébrité ; Orange signature.
    select
        b.administrative_region ADMINISTRATIVE_REGION,
        b.commercial_region COMMERCIAL_REGION,
        concat(usage_description, ' REALISE') kpi_name,
        sum (
        CASE
            WHEN upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*coefficient 
            ELSE rated_amount
        END ) value
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    left join dim.dt_usages  on service_code = usage_code
    left join DIM.SPARK_DT_COEFF_VAS on substr(transaction_date, 6, 2) = month_period
    where transaction_date ='###SLICE_VALUE###' and KPI= 'REVENUE' AND sub_account='MAIN' AND upper(SERVICE_CODE) IN ('NVX_GPRS_SVA', 'NVX_SOS','NVX_VEXT','NVX_RBT','NVX_CEL', 'NVX_SIG')
    group by
        b.administrative_region ,
        b.commercial_region,
        concat(usage_description, ' REALISE')

    UNION ALL

    -- TOTAL VAS REALISE = GOS SVA ; rachat de validité ; sos credit fees ; trafic crbt ; orange célébrité ; Orange signature.
    select 
        ADMINISTRATIVE_REGION,
        COMMERCIAL_REGION,
        'TOTAL VAS REALISE' KPI_NAME,
        SUM(VALUE) VALUE
    from
    (
        --GOS SVA ; rachat de validité ; sos credit fees ; trafic crbt ; orange célébrité ; Orange signature.
        select
            b.administrative_region ADMINISTRATIVE_REGION,
            b.commercial_region COMMERCIAL_REGION,
            sum (
            CASE
                WHEN upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*coefficient
                ELSE rated_amount 
            END ) value
        from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
        left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
        left join dim.dt_usages  on service_code = usage_code
        left join DIM.SPARK_DT_COEFF_VAS on substr(transaction_date, 6, 2) = month_period
        where transaction_date ='###SLICE_VALUE###' and KPI= 'REVENUE' AND sub_account='MAIN' AND upper(SERVICE_CODE) IN ('NVX_GPRS_SVA', 'NVX_SOS','NVX_VEXT','NVX_RBT','NVX_CEL', 'NVX_SIG')
        group by
            b.administrative_region ,
            b.commercial_region

        union all
        -- Modify Fnf Number
        SELECT
            r.ADMINISTRATIVE_REGION,
            r.COMMERCIAL_REGION,
            SUM(subs_amount) VALUE
        FROM 
        (
            select *
            from AGG.SPARK_FT_A_SUBSCRIPTION  
            where transaction_date='###SLICE_VALUE###' and SUBS_SERVICE = 'Modify FnF Number'
        ) ud
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
                max(if(administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',administrative_region)) administrative_region
            from MON.VW_SDT_CI_INFO_NEW
            group by site_name
        ) c on upper(trim(b.site_name))=upper(trim(c.site_name))
        LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 r ON TRIM(upper(nvl(c.administrative_region, 'INCONNU'))) = upper(r.ADMINISTRATIVE_REGION)
        WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
        GROUP BY
            r.ADMINISTRATIVE_REGION,
            r.COMMERCIAL_REGION
    ) A
    group by 
        ADMINISTRATIVE_REGION,
        COMMERCIAL_REGION

) T