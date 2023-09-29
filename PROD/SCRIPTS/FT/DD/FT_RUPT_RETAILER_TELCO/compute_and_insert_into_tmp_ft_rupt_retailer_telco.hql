INSERT INTO TMP.FT_RUPT_RETAILER_TELCO
SELECT 
  distinct A.event_date EVENT_DATE,
  A.event_time EVENT_TIME,
  B.msisdn MOBILE_NUMBER,
  max(A.available_balance)  STOCK,
  max(B.avg_amount)/8 AVG_AMOUNT_HOUR,
  (
    case 
      when max(A.available_balance) <= max(B.avg_amount)/8 then 1
      else 0
    end 
  ) RUPT_HOUR_MSISDN, 
  CURRENT_TIMESTAMP() as INSERT_DATE,
  '' SITE_NAME,
  A.CANAL
  FROM 
  (
    SELECT
      distinct event_date,
      event_time,
      mobile_number,
      sum(available_balance) available_balance,
      'TELCO' CANAL
    FROM
      (
        select
          distinct event_date,
          date_format(CONCAT_WS(' ', event_date, event_time),'yyyy-MM-dd HH') event_time,
          mobile_number_1 as mobile_number,
          max(available_balance)/100 available_balance,
          'TELCO' CANAL
        from CDR.SPARK_IT_ZEBRA_MASTER_BALANCE
        where event_date = '###SLICE_VALUE###' and event_time in ('10', '12', '14', '16')
        group by event_date, event_time, mobile_number_1

        union distinct

        select
          distinct event_date,
          date_format(CONCAT_WS(' ', event_date, substr(event_time,1,2)),'yyyy-MM-dd HH') event_time,
          mobile_number,
          stock available_balance,
          'OM' CANAL
        from DD.SPARK_FT_RUPT_RETAILER_OM
        where event_date= '###SLICE_VALUE###' and substr(event_time,1,2) in ('10', '12', '14', '16')
        --group by event_date, event_time, mobile_number, stock
      ) telOM     
      group by event_date, event_time, mobile_number
) A
left join
(
  select event_month, msisdn, event, canal_event, amount, transaction_count, amount/works_days  avg_amount 
  from DD.TT_RECHARGE_BY_RETAILLER_MONTH_TELCO
  where event_month = SUBSTR(REPLACE(ADD_MONTHS('###SLICE_VALUE###', -1), '-',''), 0, 6)    --'202307'
  and event = 'C2S'
  and canal_type IN ('C2S_OCM_RC','C2S_VAS_RC')
  and canal_event = 'C2S_ND_B_CUST'
) B
on A.mobile_number = B.msisdn
group by event_date, event_time, msisdn, CANAL