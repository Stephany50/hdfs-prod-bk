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


--- SMS COMBO AND SMS PURE
UNION ALL

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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
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


--Vas : GOS SVA ; modif FnF ;rachat de validité ; sos credit fees ; trafic crbt ; orange célébrité ; Orange signature.
UNION ALL

 select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
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
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    left join dim.dt_usages  on service_code = usage_code
    where transaction_date ='###SLICE_VALUE###' and KPI= 'REVENUE' AND sub_account='MAIN' AND upper(SERVICE_CODE) IN ('NVX_GPRS_SVA', 'NVX_SOS','NVX_VEXT','NVX_RBT','NVX_CEL','NVX_FBO')
    group by
    b.administrative_region ,
    b.commercial_region,
    usage_description


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
       
