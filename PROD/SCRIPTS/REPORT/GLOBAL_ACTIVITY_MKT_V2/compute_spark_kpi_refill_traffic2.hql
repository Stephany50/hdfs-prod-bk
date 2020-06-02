
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V2
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
    select * from MON.SPARK_FT_REFILL WHERE REFILL_DATE ='###SLICE_VALUE###' and
    refill_amount>0 and REFILL_TYPE in('RC', 'PVAS') and SENDER_CATEGORY IN ('NPOS','PPOS','PS','PT','POS')
)a
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
    select * from MON.SPARK_FT_REFILL WHERE REFILL_DATE ='###SLICE_VALUE###' and
    refill_amount>0 and REFILL_TYPE in('RC', 'PVAS') and SENDER_CATEGORY IN ('NPOS','PPOS','PS','PT','POS')
)a
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

union all
------- VOLUME  STOCK SNAPSHOT DISTRIBUTEUR

SELECT
    'SNAPSHOT_STOCK_DIST' DESTINATION_CODE
    ,'UNKNOWN' PROFILE_CODE
    ,'UNKNOWN' SERVICE_CODE
    ,'SNAPSHOT_STOCK_DIST' KPI
    ,'UNKNOWN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    , 'IT_ZEBRA_MASTER_BALANCE' SOURCE_TABLE
    ,'UNKNOWN' OPERATOR_CODE
    , sum(available_balance/100)  TOTAL_AMOUNT
    , sum(available_balance/100)  RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,event_date TRANSACTION_DATE

FROM (
    SELECT
    *
    FROM
    CDR.SPARK_IT_ZEBRA_MASTER_BALANCE
    where event_date ='###SLICE_VALUE###' and event_time in (select max(event_time) from cdr.spark_IT_ZEBRA_MASTER_BALANCE where event_date ='###SLICE_VALUE###') and CATEGORY='Orange Partner'AND
    USER_STATUS = 'Y'
)a
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
) site on a.mobile_number = site.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
REGION_ID,
event_date



union all
------- VOLUME  STOCK SNAPSHOT detail

SELECT
    'SNAPSHOT_STOCK_CLIENT' DESTINATION_CODE
    ,'UNKNOWN' PROFILE_CODE
    ,'UNKNOWN' SERVICE_CODE
    ,'SNAPSHOT_STOCK_CLIENT' KPI
    ,'UNKNOWN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    , 'IT_ZEBRA_MASTER_BALANCE' SOURCE_TABLE
    ,'UNKNOWN' OPERATOR_CODE
    , sum(available_balance/100)  TOTAL_AMOUNT
    , sum(available_balance/100)  RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,event_date TRANSACTION_DATE

FROM (
    SELECT
    *
    FROM
    CDR.SPARK_IT_ZEBRA_MASTER_BALANCE
    where event_date ='###SLICE_VALUE###' and event_time in (select max(event_time) from cdr.spark_IT_ZEBRA_MASTER_BALANCE where event_date ='###SLICE_VALUE###') and CATEGORY in ('Partner POS','New POS') AND
    USER_STATUS = 'Y'
)a
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
) site on a.mobile_number = site.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
REGION_ID,
event_date

union all

------- la vente moyenne DIST

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
     , 'AVG_REFILL_DIST' KPI
     , (case
            when REFILL_MEAN='SCRATCH' then 'MAIN'
            when REFILL_MEAN='C2S' then 'MAIN'
            when REFILL_MEAN in ('C2C','O2C') and RECEIVER_MSISDN = '694010631' then 'WHA'
            else 'ZEBRA'
        end)  SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'FT_REFILL' SOURCE_TABLE
     , RECEIVER_OPERATOR_CODE OPERATOR_CODE
     , sum(Refill_Amount)  TOTAL_AMOUNT
     , sum(Refill_Amount)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     ,  REGION_ID
     , REFILL_DATE TRANSACTION_DATE
FROM  (
    SELECT * FROM
     MON.SPARK_FT_REFILL
     WHERE REFILL_DATE='###SLICE_VALUE###' AND TERMINATION_IND='200' AND REFILL_MEAN ='C2S' AND REFILL_TYPE  ='RC' AND SENDER_CATEGORY in ('ORNGPTNR')

)a
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

union all


------- la vente moyenne detaillant

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
     , 'AVG_REFILL_CLIENT' KPI
     , (case
            when REFILL_MEAN='SCRATCH' then 'MAIN'
            when REFILL_MEAN='C2S' then 'MAIN'
            when REFILL_MEAN in ('C2C','O2C') and RECEIVER_MSISDN = '694010631' then 'WHA'
            else 'ZEBRA'
        end)  SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'FT_REFILL' SOURCE_TABLE
     , RECEIVER_OPERATOR_CODE OPERATOR_CODE
     , sum(Refill_Amount)  TOTAL_AMOUNT
     , sum(Refill_Amount)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     ,  REGION_ID
     , REFILL_DATE TRANSACTION_DATE
FROM  (
    SELECT * FROM
     MON.SPARK_FT_REFILL
     WHERE REFILL_DATE='###SLICE_VALUE###' AND TERMINATION_IND='200' AND REFILL_MEAN ='C2S' AND REFILL_TYPE  ='RC' AND SENDER_CATEGORY in ('NPOS','PPOS')
)a
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



union all
------- refill self top

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
     , 'REFILL_SELF_TOP' KPI
     , (case
            when REFILL_MEAN='SCRATCH' then 'MAIN'
            when REFILL_MEAN='C2S' then 'MAIN'
            when REFILL_MEAN in ('C2C','O2C') and RECEIVER_MSISDN = '694010631' then 'WHA'
            else 'ZEBRA'
        end)  SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'FT_REFILL' SOURCE_TABLE
     , RECEIVER_OPERATOR_CODE OPERATOR_CODE
     , sum(Refill_Amount)  TOTAL_AMOUNT
     , sum(Refill_Amount)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     ,  REGION_ID
     , REFILL_DATE TRANSACTION_DATE
FROM  (
    SELECT * FROM
     MON.SPARK_FT_REFILL
     WHERE  REFILL_DATE='###SLICE_VALUE###'  AND TERMINATION_IND='200' AND REFILL_MEAN ='C2S' AND REFILL_TYPE  ='RC' AND SENDER_CATEGORY ='TNT'
)a
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
