INSERT INTO AGG.SPARK_FT_A_REPORTING_360
SELECT
    ADMINISTRATIVE_REGION,
    COMMERCIAL_REGION,
    (
        case
            when KPI_NAME in ('DATA_USERS_MTD', 'DATA_USERS_DAILY_IN', 'DATA_USERS_30Jrs_IN') then 'USERS'
            else 'SUBSCRIBERS'
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
            WHEN DESTINATION_CODE = 'USER_ART' THEN 'PARC_ART'
            WHEN DESTINATION_CODE = 'USER_GROUP' THEN 'PARC_GROUP'
            WHEN DESTINATION_CODE = 'PARC_OM_DAILY' THEN 'PARC_OM_DAILY'
            WHEN DESTINATION_CODE = 'PARC_OM_MTD' THEN 'PARC_OM_MTD'
            WHEN DESTINATION_CODE = 'PARC_OM_30Jrs' THEN 'PARC_OM_30Jrs'
            WHEN DESTINATION_CODE = 'OTARIE_DATA_USERS_DAILY_1Mo' THEN 'DATA_USERS_DAILY'
            WHEN DESTINATION_CODE = 'OTARIE_DATA_USERS_MTD_1Mo' THEN 'DATA_USERS_MTD'
            WHEN DESTINATION_CODE = 'OTARIE_DATA_USERS_30_DAYS_1Mo' THEN 'DATA_USERS_30Jrs'
            WHEN DESTINATION_CODE = 'UNIQUE_DATA_USERS_1Mo' THEN 'DATA_USERS_DAILY_IN'
            WHEN DESTINATION_CODE = 'UNIQUE_DATA_USERS_MTD_1Mo' THEN 'DATA_USERS_MTD_IN'
            WHEN DESTINATION_CODE = 'UNIQUE_DATA_USERS_1Mo_30Jrs' THEN 'DATA_USERS_30Jrs_IN'
            WHEN DESTINATION_CODE = 'USER_DAILY_ACTIVE' THEN 'DAILY_BASE'
            WHEN DESTINATION_CODE = 'USER_30DAYS_GROUP' THEN 'CHARGED_BASE'
            WHEN DESTINATION_CODE = 'USER_GROSS_ADD_SUBSCRIPTION' THEN 'GROSS_ADD'
        end KPI_NAME,
        sum(TOTAL_AMOUNT) KPI_VALUE,
        REGION_ID
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
    where transaction_date = '###SLICE_VALUE###'
        and DESTINATION_CODE in ('UNIQUE_DATA_USERS_1Mo', 'UNIQUE_DATA_USERS_MTD_1Mo', 'UNIQUE_DATA_USERS_1Mo_30Jrs', 'USER_GROSS_ADD_SUBSCRIPTION', 'USER_ART', 'USER_GROUP', 'PARC_OM_DAILY', 'PARC_OM_MTD', 'PARC_OM_30Jrs', 'OTARIE_DATA_USERS_30_DAYS_1Mo', 'OTARIE_DATA_USERS_MTD_1Mo', 'OTARIE_DATA_USERS_7_DAYS_1Mo','OTARIE_DATA_USERS_DAILY_1Mo', 'USER_DAILY_ACTIVE', 'USER_30DAYS_GROUP')
    group by region_id,
        case
            WHEN DESTINATION_CODE = 'USER_ART' THEN 'PARC_ART'
            WHEN DESTINATION_CODE = 'USER_GROUP' THEN 'PARC_GROUP'
            WHEN DESTINATION_CODE = 'PARC_OM_DAILY' THEN 'PARC_OM_DAILY'
            WHEN DESTINATION_CODE = 'PARC_OM_MTD' THEN 'PARC_OM_MTD'
            WHEN DESTINATION_CODE = 'PARC_OM_30Jrs' THEN 'PARC_OM_30Jrs'
            WHEN DESTINATION_CODE = 'OTARIE_DATA_USERS_DAILY_1Mo' THEN 'DATA_USERS_DAILY'
            WHEN DESTINATION_CODE = 'OTARIE_DATA_USERS_MTD_1Mo' THEN 'DATA_USERS_MTD'
            WHEN DESTINATION_CODE = 'OTARIE_DATA_USERS_30_DAYS_1Mo' THEN 'DATA_USERS_30Jrs'
            WHEN DESTINATION_CODE = 'UNIQUE_DATA_USERS_1Mo' THEN 'DATA_USERS_DAILY_IN'
            WHEN DESTINATION_CODE = 'UNIQUE_DATA_USERS_MTD_1Mo' THEN 'DATA_USERS_MTD_IN'
            WHEN DESTINATION_CODE = 'UNIQUE_DATA_USERS_1Mo_30Jrs' THEN 'DATA_USERS_30Jrs_IN'
            WHEN DESTINATION_CODE = 'USER_DAILY_ACTIVE' THEN 'DAILY_BASE'
            WHEN DESTINATION_CODE = 'USER_30DAYS_GROUP' THEN 'CHARGED_BASE'
            WHEN DESTINATION_CODE = 'USER_GROSS_ADD_SUBSCRIPTION' THEN 'GROSS_ADD'
        end

    union

    select
        'GROSS_ADDS_MTD' KPI_NAME,
        SUM(value) KPI_VALUE,
        REGION_ID
    from
    (
        select
            transaction_date,
            region_id,
            sum(TOTAL_AMOUNT) value
        from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
        where transaction_date BETWEEN SUBSTRING('###SLICE_VALUE###', 1, 7)||'-01'  AND '###SLICE_VALUE###'
        and DESTINATION_CODE = 'USER_GROSS_ADD_SUBSCRIPTION'
        group by 
            region_id,
            transaction_date
    ) A
    group by region_id

    union
    select
        'CHARGED_BASE_LIGHT' KPI_NAME,
        count(C0.msisdn) kpi_value,
        cast(region_id as string) region_id
    from
    (
        select
            C01.msisdn,
            C01.location_ci location_ci
        from
        (
            select
                T0.msisdn msisdn,
                location_ci
            from
            (
                select
                *
                from MON.SPARK_FT_ACCOUNT_ACTIVITY a
                where event_date = date_add('###SLICE_VALUE###', 1)
                and (OG_CALL >= DATE_SUB(date_add('###SLICE_VALUE###', 1), 31) or least(IC_CALL_4,IC_CALL_3,IC_CALL_2,IC_CALL_1) >= DATE_SUB(date_add('###SLICE_VALUE###', 1), 31))
            ) T0
            left join (
                select 
                    msisdn
                from
                (
                    select
                        msisdn,
                        sum(BDLE_COST*(NVL(COEFF_DATA, 0) + NVL(COEFF_ROAMING_DATA, 0))/100) revenu_data
                    from
                    (
                        select
                            msisdn,
                            bdle_cost,
                            BDLE_NAME
                        from mon.spark_ft_cbm_bundle_subs_daily
                        where period between DATE_SUB('###SLICE_VALUE###', 30) and '###SLICE_VALUE###' 
                    ) C020
                    LEFT JOIN DIM.DT_CBM_REF_SOUSCRIPTION_PRICE C021
                    ON UPPER(TRIM(C020.BDLE_NAME)) = UPPER(TRIM(C021.BDLE_NAME))
                    group by msisdn
                ) T1
                where nvl(revenu_data, 0) = 0
            ) T11 on T0.msisdn = T11.msisdn 
            where T11.msisdn is null
        ) C01	
    ) C0
    left join (
        select
            ci location_ci,
            max(site_name) site_name
        from dim.spark_dt_gsm_cell_code
        group by ci
    ) C1 on cast (C0.location_ci as int) = cast(C1.location_ci as int)
    left join (
        select
            site_name,
            max(administrative_region) administrative_region
        from MON.VW_SDT_CI_INFO_NEW
        group by site_name
    ) C2 on upper(trim(C1.site_name))=upper(trim(C2.site_name))
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 C3
    ON TRIM(COALESCE(upper(if(C2.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD', C2.administrative_region)), 'INCONNU')) = upper(C3.ADMINISTRATIVE_REGION)
    group by region_id
) A
LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B
ON A.REGION_ID = B.REGION_ID
