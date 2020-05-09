
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_CELL
SELECT
    (Case
          when REFILL_MEAN='SCRATCH' then 'VALEUR_FACIALE'
          when REFILL_TYPE='RETURN' then 'RETRAIT_VALEUR_FACIALE'
          when SENDER_MSISDN = '694010631' then 'RECHARGE_WHA'
          when RECEIVER_MSISDN ='694010631' then 'TRANSFERT_WHA'
          when REFILL_MEAN='C2S' and REFILL_TYPE='RC' and SENDER_MSISDN <>'694010631' and RECEIVER_MSISDN <>'694010631' then 'VALEUR_FACIALE'
          else REFILL_MEAN
    end) DESTINATION_CODE
     , RECEIVER_PROFILE PROFILE_CODE
     , (Case
            when (SENDER_MSISDN = '694010631' or RECEIVER_MSISDN = '694010631') then 'NVX_WHA'
            else ( CASE REFILL_MEAN
                 WHEN 'C2S' THEN 'NVX_C2S'
                 WHEN 'C2C' THEN 'NVX_C2C'
                 WHEN 'O2C' THEN 'NVX_O2C'
                 WHEN 'SCRATCH' THEN 'NVX_TOPUP'
                 ELSE REFILL_MEAN
             END )
         end   )   SERVICE_CODE
     , 'VALEUR_AIRTIME' KPI
     , (case
            when REFILL_MEAN='SCRATCH' then 'MAIN'
            when REFILL_MEAN='C2S' then 'MAIN'
            when REFILL_MEAN in ('C2C','O2C') and RECEIVER_MSISDN = '694010631' then 'WHA'
            else 'ZEBRA'
        end)  SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'FT_REFILL' SOURCE_TABLE
     , RECEIVER_OPERATOR_CODE OPERATOR_CODE
     , SUM (IF(refill_type='RETURN', -REFILL_AMOUNT, REFILL_AMOUNT)) TOTAL_AMOUNT
     , SUM (IF(refill_type='RETURN', -REFILL_AMOUNT, REFILL_AMOUNT)) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     ,  REGION_ID
     , REFILL_DATE TRANSACTION_DATE
FROM  (
    select * from MON.SPARK_FT_REFILL WHERE REFILL_DATE ='2020-04-29' and
    REFILL_TYPE  IN ('PVAS', 'RC', 'REFILL') and REFILL_MEAN IN ('C2S', 'SCRATCH') AND
    SENDER_CATEGORY in ('TN','TNT', 'WHA', 'ODSA','ODS', 'PS','PT','POS', 'INHSM','INSM','NPOS','ORNGPTNR','PPOS')
)a
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-04-29'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-04-29'
    group by a.msisdn
) site on a.RECEIVER_MSISDN = site.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
REFILL_DATE
 ,(Case
      when REFILL_MEAN='SCRATCH' then 'VALEUR_FACIALE'
      when REFILL_TYPE='RETURN' then 'RETRAIT_VALEUR_FACIALE'
      when SENDER_MSISDN = '694010631' then 'RECHARGE_WHA'
      when RECEIVER_MSISDN ='694010631' then 'TRANSFERT_WHA'
      when REFILL_MEAN='C2S' and REFILL_TYPE='RC' and SENDER_MSISDN <>'694010631' and RECEIVER_MSISDN <>'694010631' then 'VALEUR_FACIALE'
      else REFILL_MEAN
end)
 , RECEIVER_PROFILE
 , (Case
        when (SENDER_MSISDN = '694010631' or RECEIVER_MSISDN = '694010631') then 'NVX_WHA'
         else ( CASE REFILL_MEAN
             WHEN 'C2S' THEN 'NVX_C2S'
             WHEN 'C2C' THEN 'NVX_C2C'
             WHEN 'O2C' THEN 'NVX_O2C'
             WHEN 'SCRATCH' THEN 'NVX_TOPUP'
             ELSE REFILL_MEAN
         END )
     end   )
 , (case
      when REFILL_MEAN='SCRATCH' then 'MAIN'
      when REFILL_MEAN='C2S' then 'MAIN'
      when REFILL_MEAN in ('C2C','O2C') and RECEIVER_MSISDN = '694010631' then 'WHA'
      else 'ZEBRA'
    end)
, RECEIVER_OPERATOR_CODE
,REGION_ID
union  all
SELECT
    (Case
          when REFILL_MEAN='SCRATCH' then 'VALEUR_FACIALE'
          when REFILL_TYPE='RETURN' then 'RETRAIT_VALEUR_FACIALE'
          when SENDER_MSISDN = '694010631' then 'RECHARGE_WHA'
          when RECEIVER_MSISDN ='694010631' then 'TRANSFERT_WHA'
          when REFILL_MEAN='C2S' and REFILL_TYPE='RC' and SENDER_MSISDN <>'694010631' and RECEIVER_MSISDN <>'694010631' then 'VALEUR_FACIALE'
          else REFILL_MEAN
    end) DESTINATION_CODE
     , RECEIVER_PROFILE PROFILE_CODE
     , (Case
            when (SENDER_MSISDN = '694010631' or RECEIVER_MSISDN = '694010631') then 'NVX_WHA'
            else ( CASE REFILL_MEAN
                 WHEN 'C2S' THEN 'NVX_C2S'
                 WHEN 'C2C' THEN 'NVX_C2C'
                 WHEN 'O2C' THEN 'NVX_O2C'
                 WHEN 'SCRATCH' THEN 'NVX_TOPUP'
                 ELSE REFILL_MEAN
             END )
         end   )   SERVICE_CODE
     , 'VOLUME_AIRTIME' KPI
     , (case
            when REFILL_MEAN='SCRATCH' then 'MAIN'
            when REFILL_MEAN='C2S' then 'MAIN'
            when REFILL_MEAN in ('C2C','O2C') and RECEIVER_MSISDN = '694010631' then 'WHA'
            else 'ZEBRA'
        end)  SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'FT_REFILL' SOURCE_TABLE
     , RECEIVER_OPERATOR_CODE OPERATOR_CODE
     , SUM(1)  TOTAL_AMOUNT
     , SUM(1)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     ,  REGION_ID
     , REFILL_DATE TRANSACTION_DATE
FROM  (
    select * from MON.SPARK_FT_REFILL WHERE REFILL_DATE ='2020-04-29' and
    REFILL_TYPE  IN ('PVAS', 'RC', 'REFILL') and REFILL_MEAN IN ('C2S', 'SCRATCH') AND
    SENDER_CATEGORY in ('TN','TNT', 'WHA', 'ODSA','ODS', 'PS','PT','POS', 'INHSM','INSM','NPOS','ORNGPTNR','PPOS')
)a
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-04-29'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-04-29'
    group by a.msisdn
) site on a.RECEIVER_MSISDN = site.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
REFILL_DATE
 ,(Case
      when REFILL_MEAN='SCRATCH' then 'VALEUR_FACIALE'
      when REFILL_TYPE='RETURN' then 'RETRAIT_VALEUR_FACIALE'
      when SENDER_MSISDN = '694010631' then 'RECHARGE_WHA'
      when RECEIVER_MSISDN ='694010631' then 'TRANSFERT_WHA'
      when REFILL_MEAN='C2S' and REFILL_TYPE='RC' and SENDER_MSISDN <>'694010631' and RECEIVER_MSISDN <>'694010631' then 'VALEUR_FACIALE'
      else REFILL_MEAN
end)
 , RECEIVER_PROFILE
 , (Case
        when (SENDER_MSISDN = '694010631' or RECEIVER_MSISDN = '694010631') then 'NVX_WHA'
         else ( CASE REFILL_MEAN
             WHEN 'C2S' THEN 'NVX_C2S'
             WHEN 'C2C' THEN 'NVX_C2C'
             WHEN 'O2C' THEN 'NVX_O2C'
             WHEN 'SCRATCH' THEN 'NVX_TOPUP'
             ELSE REFILL_MEAN
         END )
     end   )
 , (case
      when REFILL_MEAN='SCRATCH' then 'MAIN'
      when REFILL_MEAN='C2S' then 'MAIN'
      when REFILL_MEAN in ('C2C','O2C') and RECEIVER_MSISDN = '694010631' then 'WHA'
      else 'ZEBRA'
    end)
, RECEIVER_OPERATOR_CODE
,REGION_ID
union  all
select
    'RECHARGE_OM' DESTINATION_CODE
    ,COMMERCIAL_OFFER PROFILE_CODE
    ,'NVX_O2C' SERVICE_CODE
    ,'VALEUR_AIRTIME' KPI
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    , 'FT_SUBSCRIPTION' SOURCE_TABLE
    ,OPERATOR_CODE
    , sum(rated_amount)  TOTAL_AMOUNT
    , sum(rated_amount)  RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,TRANSACTION_DATE
from (
    select * from mon.spark_ft_subscription where transaction_date ='2020-04-29'  and rated_amount>0 and subscription_channel = '32'
)a
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-04-29'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-04-29'
    group by a.msisdn
) site on a.served_party_msisdn = site.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
COMMERCIAL_OFFER,
REGION_ID,
OPERATOR_CODE,
TRANSACTION_DATE


UNION ALL
select
    'RECHARGE_OM' DESTINATION_CODE
    ,COMMERCIAL_OFFER PROFILE_CODE
    ,'NVX_O2C' SERVICE_CODE
    ,'VOLUME_AIRTIME' KPI
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    , 'FT_SUBSCRIPTION' SOURCE_TABLE
    ,OPERATOR_CODE
    , sum(1)  TOTAL_AMOUNT
    , sum(1)  RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,TRANSACTION_DATE
from (
    select * from mon.spark_ft_subscription where transaction_date ='2020-04-29'  and rated_amount>0 and subscription_channel = '32'
)a
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-04-29'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-04-29'
    group by a.msisdn
) site on a.served_party_msisdn = site.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
COMMERCIAL_OFFER,
REGION_ID,
OPERATOR_CODE,
TRANSACTION_DATE


union all
------------------ POS AIRTIME ACTIF ------------
SELECT
    (Case
          when REFILL_MEAN='SCRATCH' then 'VALEUR_FACIALE'
          when REFILL_TYPE='RETURN' then 'RETRAIT_VALEUR_FACIALE'
          when SENDER_MSISDN = '694010631' then 'RECHARGE_WHA'
          when RECEIVER_MSISDN ='694010631' then 'TRANSFERT_WHA'
          when REFILL_MEAN='C2S' and REFILL_TYPE='RC' and SENDER_MSISDN <>'694010631' and RECEIVER_MSISDN <>'694010631' then 'VALEUR_FACIALE'
          else REFILL_MEAN
    end) DESTINATION_CODE
     , RECEIVER_PROFILE PROFILE_CODE
     , (Case
            when (SENDER_MSISDN = '694010631' or RECEIVER_MSISDN = '694010631') then 'NVX_WHA'
            else ( CASE REFILL_MEAN
                 WHEN 'C2S' THEN 'NVX_C2S'
                 WHEN 'C2C' THEN 'NVX_C2C'
                 WHEN 'O2C' THEN 'NVX_O2C'
                 WHEN 'SCRATCH' THEN 'NVX_TOPUP'
                 ELSE REFILL_MEAN
             END )
         end   )   SERVICE_CODE
     , 'POS_AIRTIME' KPI
     , (case
            when REFILL_MEAN='SCRATCH' then 'MAIN'
            when REFILL_MEAN='C2S' then 'MAIN'
            when REFILL_MEAN in ('C2C','O2C') and RECEIVER_MSISDN = '694010631' then 'WHA'
            else 'ZEBRA'
        end)  SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'FT_REFILL' SOURCE_TABLE
     , RECEIVER_OPERATOR_CODE OPERATOR_CODE
     , count(Distinct Sender_MSISDN)  TOTAL_AMOUNT
     , count(Distinct Sender_MSISDN)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     ,  REGION_ID
     , REFILL_DATE TRANSACTION_DATE
FROM  (
    select * from MON.SPARK_FT_REFILL WHERE REFILL_DATE ='2020-04-29' and
    refill_amount>0 and REFILL_TYPE in('RC', 'PVAS') and SENDER_CATEGORY IN ('NPOS','PPOS','PS','PT','POS')
)a
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-04-29'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-04-29'
    group by a.msisdn
) site on a.RECEIVER_MSISDN = site.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
REFILL_DATE
 ,(Case
      when REFILL_MEAN='SCRATCH' then 'VALEUR_FACIALE'
      when REFILL_TYPE='RETURN' then 'RETRAIT_VALEUR_FACIALE'
      when SENDER_MSISDN = '694010631' then 'RECHARGE_WHA'
      when RECEIVER_MSISDN ='694010631' then 'TRANSFERT_WHA'
      when REFILL_MEAN='C2S' and REFILL_TYPE='RC' and SENDER_MSISDN <>'694010631' and RECEIVER_MSISDN <>'694010631' then 'VALEUR_FACIALE'
      else REFILL_MEAN
end)
 , RECEIVER_PROFILE
 , (Case
        when (SENDER_MSISDN = '694010631' or RECEIVER_MSISDN = '694010631') then 'NVX_WHA'
         else ( CASE REFILL_MEAN
             WHEN 'C2S' THEN 'NVX_C2S'
             WHEN 'C2C' THEN 'NVX_C2C'
             WHEN 'O2C' THEN 'NVX_O2C'
             WHEN 'SCRATCH' THEN 'NVX_TOPUP'
             ELSE REFILL_MEAN
         END )
     end   )
 , (case
      when REFILL_MEAN='SCRATCH' then 'MAIN'
      when REFILL_MEAN='C2S' then 'MAIN'
      when REFILL_MEAN in ('C2C','O2C') and RECEIVER_MSISDN = '694010631' then 'WHA'
      else 'ZEBRA'
    end)
, RECEIVER_OPERATOR_CODE
,REGION_ID