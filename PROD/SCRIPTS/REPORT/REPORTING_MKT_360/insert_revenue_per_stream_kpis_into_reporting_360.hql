INSERT INTO AGG.SPARK_FT_A_REPORTING_360
SELECT 
NVL(ADMINISTRATIVE_REGION, 'INCONNU') ADMINISTRATIVE_REGION,
NVL(COMMERCIAL_REGION, 'INCONNU') COMMERCIAL_REGION,
(
    case
        when KPI_NAME IN ('SMS USERS MTD', 'VOICE USERS MTD') then 'USERS'
        when KPI_NAME IN ('DATA TRAFFIC', 'VOICE TRAFFIC', 'SMS TRAFFIC') then 'TRAFFIC'
        when KPI_NAME IN 
            (
                'SMS USERS', 'SMS USERS_7_DAYS', 'SMS USERS_30_DAYS',
                'VOICE USERS', 'VOICE USERS_7_DAYS', 'VOICE USERS_30_DAYS'
            ) then 'SUBSCRIBERS'
        else 'REVENUE PER STREAM'
    end
) KPI_GROUP_NAME,
REPLACE(REPLACE(REPLACE(REPLACE(UPPER(KPI_NAME), 'Ã','E'), 'Ê', 'E'), 'É',  'E'), 'È', 'E') KPI_NAME, 
VALUE,
CURRENT_TIMESTAMP INSERT_DATE,
'###SLICE_VALUE###' PROCESSING_DATE  
FROM (
    -- TOTAL VOICE 
    SELECT
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'TOTAL VOICE' KPI_NAME,
        SUM(
            CASE WHEN SOURCE_TABLE = 'FT_SUBS_RETAIL_ZEBRA' THEN (70/100)*RATED_AMOUNT
            ELSE RATED_AMOUNT END
        ) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND SUB_ACCOUNT='MAIN' AND 
        (
            SUBSTRING(DESTINATION_CODE,1,13)='REVENUE_VOICE' OR 
            SOURCE_TABLE IN ('FT_SUBS_RETAIL_ZEBRA','FT_CREDIT_TRANSFER','FT_CONTRACT_SNAPSHOT') OR 
            DESTINATION_CODE IN ('UNKNOWN_BUN') OR 
            (source_table ='IT_ZTE_ADJUSTMENT' and service_code='NVX_PAR')
        )         
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION

    UNION ALL
    -- VOICE PYG 
    SELECT 
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'VOICE PYG' KPI_NAME,
        SUM(RATED_AMOUNT) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND 
        SUB_ACCOUNT='MAIN' AND 
        SOURCE_TABLE='FT_GSM_TRAFFIC_REVENUE_DAILY' AND 
        DESTINATION_CODE LIKE '%REVENUE_VOICE%'
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION

    UNION ALL
    -- VOICE PYG INTERNATIONAL et VOICE PYG NATIONAL
    SELECT 
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        (
            case 
                when DESTINATION_CODE='REVENUE_VOICE_INTERNATIONAL' then 'PYG_VOIX_INTERNATIONAL'
                else 'PYG_VOIX_NATIONAL'
            end
        ) KPI_NAME,
        SUM(RATED_AMOUNT) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND 
        SUB_ACCOUNT='MAIN' AND 
        SOURCE_TABLE='FT_GSM_TRAFFIC_REVENUE_DAILY' AND 
        DESTINATION_CODE LIKE '%REVENUE_VOICE%'
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION,
        DESTINATION_CODE


    UNION ALL
    -- VOICE BUNDLES 
    SELECT
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'VOICE BUNDLES' KPI_NAME,
        SUM(
            CASE WHEN SOURCE_TABLE = 'FT_SUBS_RETAIL_ZEBRA' THEN (70/100)*RATED_AMOUNT
            ELSE RATED_AMOUNT END
        ) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND 
        SUB_ACCOUNT='MAIN' AND 
        DESTINATION_CODE='REVENUE_VOICE_BUNDLE'
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION,
        DESTINATION_CODE
    
    UNION ALL
    --- VOICE COMBO AND VOICE PURE
    SELECT
            r.ADMINISTRATIVE_REGION,
            r.COMMERCIAL_REGION,
            case when coef_voix<1 then 'VOICE COMBO' else 'VOICE PURE' end  KPI_NAME,
            SUM(SUBS_AMOUNT*coef_voix) VALUE
        FROM AGG.SPARK_FT_A_SUBSCRIPTION   ud
    left join  (
        select event,
            (nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
            (nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
            (case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
            (case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur,
            nvl(DATA_BUNDLE,0) data
            from dim.dt_services 
    ) events on upper(trim(ud.SUBS_BENEFIT_NAME)) = upper(trim(events.EVENT))
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
            r.COMMERCIAL_REGION,
            case when coef_voix<1 then 'VOICE COMBO' else 'VOICE PURE' end


    UNION ALL
    -- TOTAL DATA 
    SELECT
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'TOTAL DATA' KPI_NAME,
        SUM(CASE WHEN SOURCE_TABLE ='FT_SUBS_RETAIL_ZEBRA' THEN RATED_AMOUNT*30/100 ELSE RATED_AMOUNT END) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND 
        SUB_ACCOUNT='MAIN' AND 
        (
            DESTINATION_CODE='REVENUE_DATA_BUNDLE' or 
            DESTINATION_CODE='OM_DATA' or 
            (DESTINATION_CODE='REVENUE_DATA_PAYGO' and service_code<>'NVX_GPRS_SVA') or 
            source_table in ('FT_EMERGENCY_DATA','FT_DATA_TRANSFER','FT_SUBS_RETAIL_ZEBRA')
        )
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION

    UNION ALL
    -- DATA BUNDLES 
    SELECT
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'DATA BUNDLES' KPI_NAME,
        SUM(RATED_AMOUNT) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND SUB_ACCOUNT='MAIN' AND 
        DESTINATION_CODE in ('REVENUE_DATA_BUNDLE', 'OM_DATA') 
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION

    UNION ALL
    -- DATA COMBO 
    SELECT
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'DATA COMBO' KPI_NAME,
        SUM(RATED_AMOUNT) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE_COMBO_DATA' AND SUB_ACCOUNT='MAIN' AND 
        DESTINATION_CODE='COMBO_DATA'
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION

    UNION ALL
    -- DATA ROAMING 
    SELECT
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'DATA ROAMING' KPI_NAME,
        SUM(RATED_AMOUNT) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND 
        SUB_ACCOUNT='MAIN' AND
        SERVICE_CODE='NVX_GPRS_ROAMING'
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION

    UNION ALL
    -- TOTAL SMS 
    SELECT
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'TOTAL SMS' KPI_NAME,
        SUM(RATED_AMOUNT) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND 
        SUB_ACCOUNT='MAIN' AND 
        SUBSTRING(DESTINATION_CODE,1,11)='REVENUE_SMS'
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION

    UNION ALL
    -- SMS PYG 
    SELECT 
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'SMS PYG' KPI_NAME,
        SUM(RATED_AMOUNT) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND 
        SUB_ACCOUNT='MAIN' AND 
        SOURCE_TABLE='FT_GSM_TRAFFIC_REVENUE_DAILY' AND 
        DESTINATION_CODE LIKE '%REVENUE_SMS%'
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION
    
    UNION ALL
    -- SMS PYG INTERNATIONAL ET NATIONAL
    SELECT 
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        (
            case 
                when DESTINATION_CODE='REVENUE_SMS_INTERNATIONAL' then 'PYG_SMS_INTERNATIONAL'
                else 'PYG_SMS_NATIONAL'
            end
        ) KPI_NAME,
        SUM(RATED_AMOUNT) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND 
        SUB_ACCOUNT='MAIN' AND 
        SOURCE_TABLE='FT_GSM_TRAFFIC_REVENUE_DAILY' AND 
        DESTINATION_CODE LIKE '%REVENUE_SMS%'
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION,
        DESTINATION_CODE

    UNION ALL
    -- SMS BUNDLES 
    SELECT
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'SMS BUNDLES' KPI_NAME,
        SUM(RATED_AMOUNT) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND 
        SUB_ACCOUNT='MAIN' AND 
        DESTINATION_CODE='REVENUE_SMS_BUNDLE'
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION
    
    UNION ALL
    --- SMS COMBO AND SMS PURE
    SELECT
            r.ADMINISTRATIVE_REGION,
            r.COMMERCIAL_REGION,
            case when coef_sms<1 then 'SMS COMBO' else 'SMS PURE' end  KPI_NAME,
            SUM(SUBS_AMOUNT*coef_sms) VALUE
        FROM AGG.SPARK_FT_A_SUBSCRIPTION   ud
    left join  (
        select event,
            (nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
            (nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
            (case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
            (case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur,
            nvl(DATA_BUNDLE,0) data
            from dim.dt_services
    ) events on upper(trim(ud.SUBS_BENEFIT_NAME)) = upper(trim(events.EVENT))
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
            r.COMMERCIAL_REGION,
            case when coef_sms<1 then 'SMS COMBO' else 'SMS PURE' end


    UNION ALL
    --GOS SVA ; rachat de validité ; sos credit fees ; trafic crbt ; orange célébrité ; Orange signature.
    select
            b.administrative_region ADMINISTRATIVE_REGION,
            b.commercial_region COMMERCIAL_REGION,
            usage_description kpi_name,
            sum (
            CASE
                WHEN TRANSACTION_DATE LIKE '%-01-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.581
                WHEN TRANSACTION_DATE LIKE '%-02-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.544
                WHEN TRANSACTION_DATE LIKE '%-03-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.581
                WHEN TRANSACTION_DATE LIKE '%-04-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.537
                WHEN TRANSACTION_DATE LIKE '%-05-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.540
                WHEN TRANSACTION_DATE LIKE '%-06-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.534
                WHEN TRANSACTION_DATE LIKE '%-07-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.578
                WHEN TRANSACTION_DATE LIKE '%-08-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.574
                WHEN TRANSACTION_DATE LIKE '%-09-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.573
                WHEN TRANSACTION_DATE LIKE '%-10-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.572
                WHEN TRANSACTION_DATE LIKE '%-11-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.571
                WHEN TRANSACTION_DATE LIKE '%-12-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.569
                ELSE rated_amount
            END ) value
        from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
        left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
        left join dim.dt_usages  on service_code = usage_code
        where transaction_date ='###SLICE_VALUE###' and KPI= 'REVENUE' AND sub_account='MAIN' AND upper(SERVICE_CODE) IN ('NVX_GPRS_SVA', 'NVX_SOS','NVX_VEXT','NVX_RBT','NVX_CEL', 'NVX_SIG')
        group by
        b.administrative_region ,
        b.commercial_region,
        usage_description
    
    union all
    -- Modify Fnf Number
    SELECT
        r.ADMINISTRATIVE_REGION,
        r.COMMERCIAL_REGION,
        'MODIFY FNF NUMBER' KPI_NAME,
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

    --- Emergency data , P2P DATA , VAS RETAIL DATA
    UNION ALL

    select
        b.ADMINISTRATIVE_REGION,
        b.COMMERCIAL_REGION,
        case
            when source_table='FT_EMERGENCY_DATA' then 'Emergency data'
            when source_table='FT_DATA_TRANSFER' then 'P2P DATA'
            else 'VAS RETAIL DATA'
        end  KPI_NAME,
        sum(case when source_table ='FT_SUBS_RETAIL_ZEBRA' then rated_amount*30/100 else rated_amount end) value
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='###SLICE_VALUE###'   and KPI= 'REVENUE' AND sub_account='MAIN' and ( source_table in ('FT_EMERGENCY_DATA','FT_DATA_TRANSFER','FT_SUBS_RETAIL_ZEBRA'))
    group by
        b.administrative_region ,
        b.commercial_region,
        case
            when source_table='FT_EMERGENCY_DATA' then 'Emergency data'
            when source_table='FT_DATA_TRANSFER' then 'P2P DATA'
            else 'VAS RETAIL DATA'
        end

    -- Credit Compte Desactivé, Transfert P2P,VAS RETAIL VOICE
    UNION ALL
    select
            b.ADMINISTRATIVE_REGION,
            b.COMMERCIAL_REGION,
            case
                when source_table='FT_CONTRACT_SNAPSHOT' then 'Credit Compte Desactivé'
                when source_table='FT_CREDIT_TRANSFER' then 'Transfert P2P'
                else 'VAS RETAIL VOICE'
            end  KPI_NAME,
            sum(case when source_table ='FT_SUBS_RETAIL_ZEBRA' then rated_amount*70/100 else rated_amount end) value
        from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
        left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
        where transaction_date ='###SLICE_VALUE###'   and KPI= 'REVENUE' AND sub_account='MAIN' and source_table IN ('FT_SUBS_RETAIL_ZEBRA','FT_CREDIT_TRANSFER','FT_CONTRACT_SNAPSHOT')
        group by
        b.administrative_region ,
        b.commercial_region,
    case
        when source_table='FT_CONTRACT_SNAPSHOT' then 'Credit Compte Desactivé'
        when source_table='FT_CREDIT_TRANSFER' then 'Transfert P2P'
        else 'VAS RETAIL VOICE'
        end

    UNION ALL
    -- SOS CREDIT 
    SELECT
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'SOS CREDIT' KPI_NAME,
        SUM(RATED_AMOUNT) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND 
        SUB_ACCOUNT='MAIN' AND 
        DESTINATION_CODE='SOS_CREDIT'
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION

    UNION ALL
    -- ZEBRA SERVICE 
    SELECT
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'ZEBRA SERVICE' KPI_NAME,
        SUM(RATED_AMOUNT) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        KPI='REVENUE' AND 
        SUB_ACCOUNT='MAIN' AND 
        SOURCE_TABLE = 'FT_SUBS_RETAIL_ZEBRA'
    GROUP BY
        B.ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION

    UNION ALL
    -- ORANGE MONEY 
    SELECT
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'ORANGE MONEY' KPI_NAME,
        SUM(RATED_AMOUNT) VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE ='###SLICE_VALUE###' AND 
        KPI='REVENUE_OM'
    GROUP BY
    B.ADMINISTRATIVE_REGION,
    B.COMMERCIAL_REGION

    UNION ALL
    -- 17. VOICE TRAFFIC
    SELECT
        r.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        r.COMMERCIAL_REGION COMMERCIAL_REGION,
        'VOICE TRAFFIC' KPI_NAME,
        SUM(TRAFFIC_VOICE_DAILY) VALUE
    FROM MON.SPARK_FT_VOICE_SMS_USERS A
    LEFT JOIN (
        SELECT
            A.MSISDN,
            MAX(A.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_A,
            MAX(B.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_B
        FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
        LEFT JOIN (
            SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
        ) B ON A.MSISDN = B.MSISDN
        WHERE A.EVENT_DATE='###SLICE_VALUE###'
        GROUP BY A.MSISDN
    ) SITE ON  SITE.MSISDN = GET_NNP_MSISDN_9DIGITS(A.MSISDN)
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 R ON TRIM(COALESCE(UPPER(SITE.ADMINISTRATIVE_REGION_B),UPPER(SITE.ADMINISTRATIVE_REGION_A), 'INCONNU')) = UPPER(R.ADMINISTRATIVE_REGION)
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    GROUP BY
        R.ADMINISTRATIVE_REGION,
        R.COMMERCIAL_REGION


    UNION ALL
    -- SMS TRAFFIC
    SELECT
        r.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        r.COMMERCIAL_REGION COMMERCIAL_REGION,
        'SMS TRAFFIC' KPI_NAME,
        SUM(NB_SMS_DAILY) VALUE
    FROM MON.SPARK_FT_VOICE_SMS_USERS A
    LEFT JOIN (
        SELECT
            A.MSISDN,
            MAX(A.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_A,
            MAX(B.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_B
        FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
        LEFT JOIN (
            SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
        ) B ON A.MSISDN = B.MSISDN
        WHERE A.EVENT_DATE='###SLICE_VALUE###'
        GROUP BY A.MSISDN
    ) SITE ON  SITE.MSISDN = GET_NNP_MSISDN_9DIGITS(A.MSISDN)
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 R ON TRIM(COALESCE(UPPER(SITE.ADMINISTRATIVE_REGION_B),UPPER(SITE.ADMINISTRATIVE_REGION_A), 'INCONNU')) = UPPER(R.ADMINISTRATIVE_REGION)
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    GROUP BY
        R.ADMINISTRATIVE_REGION,
        R.COMMERCIAL_REGION

    UNION ALL
    -- DATA TRAFFIC
    SELECT
        B.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        B.COMMERCIAL_REGION COMMERCIAL_REGION,
        'DATA TRAFFIC' KPI_NAME,
        cast(sum(rated_amount) as double )/1024/1024 VALUE
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG A
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 B ON A.REGION_ID = B.REGION_ID
    WHERE TRANSACTION_DATE ='###SLICE_VALUE###' AND 
        KPI='USAGE' AND 
        DESTINATION_CODE='USAGE_DATA_GPRS'
    GROUP BY
    B.ADMINISTRATIVE_REGION,
    B.COMMERCIAL_REGION

    UNION ALL
    -- VOICE USERS
    SELECT
        r.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        r.COMMERCIAL_REGION COMMERCIAL_REGION,
        'VOICE USERS' KPI_NAME,
        sum( case when NB_VOICE_DAILY > 0 then 1 else 0 end ) VALUE
    FROM MON.SPARK_FT_VOICE_SMS_USERS A
    LEFT JOIN (
        SELECT
            A.MSISDN MSISDN,
            MAX(A.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_A,
            MAX(B.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_B
        FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
        LEFT JOIN (
            SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
        ) B ON A.MSISDN = B.MSISDN
        WHERE A.EVENT_DATE='###SLICE_VALUE###'
        GROUP BY A.MSISDN
    ) SITE ON  SITE.MSISDN = GET_NNP_MSISDN_9DIGITS(A.MSISDN)
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 R ON TRIM(COALESCE(UPPER(SITE.ADMINISTRATIVE_REGION_B),UPPER(SITE.ADMINISTRATIVE_REGION_A), 'INCONNU')) = UPPER(R.ADMINISTRATIVE_REGION)
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    GROUP BY
        R.ADMINISTRATIVE_REGION,
        R.COMMERCIAL_REGION


    UNION ALL
    -- SMS USERS
    SELECT
        r.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        r.COMMERCIAL_REGION COMMERCIAL_REGION,
        'SMS USERS' KPI_NAME,
        sum( case when NB_SMS_DAILY > 0 then 1 else 0 end ) VALUE
    FROM MON.SPARK_FT_VOICE_SMS_USERS A
    LEFT JOIN (
        SELECT
            A.MSISDN MSISDN,
            MAX(A.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_A,
            MAX(B.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_B
        FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
        LEFT JOIN (
            SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
        ) B ON A.MSISDN = B.MSISDN
        WHERE A.EVENT_DATE='###SLICE_VALUE###'
        GROUP BY A.MSISDN
    ) SITE ON  SITE.MSISDN = GET_NNP_MSISDN_9DIGITS(A.MSISDN)
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 R ON TRIM(COALESCE(UPPER(SITE.ADMINISTRATIVE_REGION_B),UPPER(SITE.ADMINISTRATIVE_REGION_A), 'INCONNU')) = UPPER(R.ADMINISTRATIVE_REGION)
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    GROUP BY
        R.ADMINISTRATIVE_REGION,
        R.COMMERCIAL_REGION  

    UNION ALL
    -- SMS USERS MTD
    SELECT
        r.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        r.COMMERCIAL_REGION COMMERCIAL_REGION,
        'SMS USERS MTD' KPI_NAME,
        sum( case when NB_SMS_MTD > 0 then 1 else 0 end ) VALUE
    FROM MON.SPARK_FT_VOICE_SMS_USERS A
    LEFT JOIN (
        SELECT
            A.MSISDN MSISDN,
            MAX(A.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_A,
            MAX(B.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_B
        FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
        LEFT JOIN (
            SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
        ) B ON A.MSISDN = B.MSISDN
        WHERE A.EVENT_DATE='###SLICE_VALUE###'
        GROUP BY A.MSISDN
    ) SITE ON  SITE.MSISDN = GET_NNP_MSISDN_9DIGITS(A.MSISDN)
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 R ON TRIM(COALESCE(UPPER(SITE.ADMINISTRATIVE_REGION_B),UPPER(SITE.ADMINISTRATIVE_REGION_A), 'INCONNU')) = UPPER(R.ADMINISTRATIVE_REGION)
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    GROUP BY
        R.ADMINISTRATIVE_REGION,
        R.COMMERCIAL_REGION

    UNION ALL
    -- 24 VOICE USERS MTD
    SELECT
        r.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        r.COMMERCIAL_REGION COMMERCIAL_REGION,
        'VOICE USERS MTD' KPI_NAME,
        sum( case when NB_VOICE_MTD > 0 then 1 else 0 end ) VALUE
    FROM MON.SPARK_FT_VOICE_SMS_USERS A
    LEFT JOIN (
        SELECT
            A.MSISDN MSISDN,
            MAX(A.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_A,
            MAX(B.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_B
        FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
        LEFT JOIN (
            SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
        ) B ON A.MSISDN = B.MSISDN
        WHERE A.EVENT_DATE='###SLICE_VALUE###'
        GROUP BY A.MSISDN
    ) SITE ON  SITE.MSISDN = GET_NNP_MSISDN_9DIGITS(A.MSISDN)
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 R ON TRIM(COALESCE(UPPER(SITE.ADMINISTRATIVE_REGION_B),UPPER(SITE.ADMINISTRATIVE_REGION_A), 'INCONNU')) = UPPER(R.ADMINISTRATIVE_REGION)
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    GROUP BY
        R.ADMINISTRATIVE_REGION,
        R.COMMERCIAL_REGION
    
    UNION ALL
    -- VOICE USERS 7 DAYS
    SELECT
        r.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        r.COMMERCIAL_REGION COMMERCIAL_REGION,
        'VOICE USERS_7_DAYS' KPI_NAME,
        sum( case when NB_VOICE_7_DAYS > 0 then 1 else 0 end ) VALUE
    FROM MON.SPARK_FT_VOICE_SMS_USERS A
    LEFT JOIN (
        SELECT
            A.MSISDN MSISDN,
            MAX(A.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_A,
            MAX(B.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_B
        FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
        LEFT JOIN (
            SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
        ) B ON A.MSISDN = B.MSISDN
        WHERE A.EVENT_DATE='###SLICE_VALUE###'
        GROUP BY A.MSISDN
    ) SITE ON  SITE.MSISDN = GET_NNP_MSISDN_9DIGITS(A.MSISDN)
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 R ON TRIM(COALESCE(UPPER(SITE.ADMINISTRATIVE_REGION_B),UPPER(SITE.ADMINISTRATIVE_REGION_A), 'INCONNU')) = UPPER(R.ADMINISTRATIVE_REGION)
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    GROUP BY
        R.ADMINISTRATIVE_REGION,
        R.COMMERCIAL_REGION

    UNION ALL
    -- VOICE USERS 30 DAYS
    SELECT
        r.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        r.COMMERCIAL_REGION COMMERCIAL_REGION,
        'VOICE USERS_30_DAYS' KPI_NAME,
        sum( case when NB_VOICE_30_DAYS > 0 then 1 else 0 end ) VALUE
    FROM MON.SPARK_FT_VOICE_SMS_USERS A
    LEFT JOIN (
        SELECT
            A.MSISDN MSISDN,
            MAX(A.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_A,
            MAX(B.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_B
        FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
        LEFT JOIN (
            SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
        ) B ON A.MSISDN = B.MSISDN
        WHERE A.EVENT_DATE='###SLICE_VALUE###'
        GROUP BY A.MSISDN
    ) SITE ON  SITE.MSISDN = GET_NNP_MSISDN_9DIGITS(A.MSISDN)
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 R ON TRIM(COALESCE(UPPER(SITE.ADMINISTRATIVE_REGION_B),UPPER(SITE.ADMINISTRATIVE_REGION_A), 'INCONNU')) = UPPER(R.ADMINISTRATIVE_REGION)
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    GROUP BY
        R.ADMINISTRATIVE_REGION,
        R.COMMERCIAL_REGION
    
    UNION ALL
    -- SMS USERS 7 DAYS
    SELECT
        r.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        r.COMMERCIAL_REGION COMMERCIAL_REGION,
        'SMS USERS_7_DAYS' KPI_NAME,
        sum( case when NB_SMS_7_DAYS > 0 then 1 else 0 end ) VALUE
    FROM MON.SPARK_FT_VOICE_SMS_USERS A
    LEFT JOIN (
        SELECT
            A.MSISDN MSISDN,
            MAX(A.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_A,
            MAX(B.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_B
        FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
        LEFT JOIN (
            SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
        ) B ON A.MSISDN = B.MSISDN
        WHERE A.EVENT_DATE='###SLICE_VALUE###'
        GROUP BY A.MSISDN
    ) SITE ON  SITE.MSISDN = GET_NNP_MSISDN_9DIGITS(A.MSISDN)
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 R ON TRIM(COALESCE(UPPER(SITE.ADMINISTRATIVE_REGION_B),UPPER(SITE.ADMINISTRATIVE_REGION_A), 'INCONNU')) = UPPER(R.ADMINISTRATIVE_REGION)
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    GROUP BY
        R.ADMINISTRATIVE_REGION,
        R.COMMERCIAL_REGION

    UNION ALL
    -- SMS USERS 30 DAYS
    SELECT
        r.ADMINISTRATIVE_REGION ADMINISTRATIVE_REGION,
        r.COMMERCIAL_REGION COMMERCIAL_REGION,
        'SMS USERS_30_DAYS' KPI_NAME,
        sum( case when NB_SMS_30_DAYS > 0 then 1 else 0 end ) VALUE
    FROM MON.SPARK_FT_VOICE_SMS_USERS A
    LEFT JOIN (
        SELECT
            A.MSISDN MSISDN,
            MAX(A.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_A,
            MAX(B.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_B
        FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
        LEFT JOIN (
            SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
        ) B ON A.MSISDN = B.MSISDN
        WHERE A.EVENT_DATE='###SLICE_VALUE###'
        GROUP BY A.MSISDN
    ) SITE ON  SITE.MSISDN = GET_NNP_MSISDN_9DIGITS(A.MSISDN)
    LEFT JOIN DIM.SPARK_DT_REGIONS_MKT_V2 R ON TRIM(COALESCE(UPPER(SITE.ADMINISTRATIVE_REGION_B),UPPER(SITE.ADMINISTRATIVE_REGION_A), 'INCONNU')) = UPPER(R.ADMINISTRATIVE_REGION)
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    GROUP BY
        R.ADMINISTRATIVE_REGION,
        R.COMMERCIAL_REGION

    -- les produits indéterminés (bundle)
    union all 

    SELECT
        r.ADMINISTRATIVE_REGION
        ,r.COMMERCIAL_REGION
        ,'UNKNOWN BUNDLE' kpi_name
        ,SUM(SUBS_AMOUNT) value
    FROM AGG.SPARK_FT_A_SUBSCRIPTION  ud
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
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###' and  (upper(trim(subs_benefit_name))='RP DATA SHAPE_5120K' or  subs_benefit_name  is  null )
    GROUP BY 
        r.ADMINISTRATIVE_REGION
        ,r.COMMERCIAL_REGION

    UNION ALL

    -- TOTAL VAS = GOS SVA ; rachat de validité ; sos credit fees ; trafic crbt ; orange célébrité ; Orange signature.
    select 
        ADMINISTRATIVE_REGION,
        COMMERCIAL_REGION,
        'TOTAL VAS' KPI_NAME,
        SUM(VALUE) VALUE
    from
    (
        --GOS SVA ; rachat de validité ; sos credit fees ; trafic crbt ; orange célébrité ; Orange signature.
        select
        b.administrative_region ADMINISTRATIVE_REGION,
        b.commercial_region COMMERCIAL_REGION,
        sum (
        CASE
            WHEN TRANSACTION_DATE LIKE '%-01-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.581
            WHEN TRANSACTION_DATE LIKE '%-02-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.544
            WHEN TRANSACTION_DATE LIKE '%-03-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.581
            WHEN TRANSACTION_DATE LIKE '%-04-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.537
            WHEN TRANSACTION_DATE LIKE '%-05-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.540
            WHEN TRANSACTION_DATE LIKE '%-06-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.534
            WHEN TRANSACTION_DATE LIKE '%-07-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.578
            WHEN TRANSACTION_DATE LIKE '%-08-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.574
            WHEN TRANSACTION_DATE LIKE '%-09-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.573
            WHEN TRANSACTION_DATE LIKE '%-10-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.572
            WHEN TRANSACTION_DATE LIKE '%-11-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.571
            WHEN TRANSACTION_DATE LIKE '%-12-%' and upper(SERVICE_CODE) in ('NVX_GPRS_SVA','NVX_CEL','NVX_RBT','NVX_VEXT','NVX_SIG' ) THEN rated_amount*0.569
            ELSE rated_amount 
        END ) value
        from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
        left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
        left join dim.dt_usages  on service_code = usage_code
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