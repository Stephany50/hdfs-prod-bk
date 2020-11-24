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
    select * from MON.SPARK_FT_REFILL WHERE REFILL_DATE ='2020-10-01' and
    refill_amount>0 and REFILL_TYPE in('RC', 'PVAS') and SENDER_CATEGORY IN ('NPOS','PPOS','PS','PT','POS')
)a
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-10-01'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-10-01'
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

