INSERT INTO TABLE MON.SPARK_TT_PICO_KPIS PARTITION(EVENT_MONTH)
SELECT
    split(valeur_colonne,'[|]')[0] KPI
    ,served_party MSISDN
    ,NVL(split(valeur_colonne,'[|]')[1], 0) VAL
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,'###SLICE_VALUE###' EVENT_MONTH
FROM
(
  select
    served_party,
    sum(
    case when call_destination_type='ONNET' and service_code='TEL'
    then 1
    else 0 end
    ) CALLS_ON_N,
    sum(
    case when call_destination_type='OFFNET' and service_code='TEL'
    then 1
    else 0 end
    ) CALLS_OFF_N,
    sum(
    case when call_destination_type='ONNET' and service_code='TEL'
    then rated_duration
    else 0 end
    ) CALLS_ON_D,
    sum(
    case when call_destination_type='OFFNET' and service_code='TEL'
    then rated_duration
    else 0 end
    ) CALLS_OFF_D,
    sum(
    case when call_destination_type='ONNET' and service_code='SMS'
    then 1
    else 0 end
    ) SMS_ON,
    sum(
    case when call_destination_type='OFFNET' and service_code='SMS'
    then 1
    else 0 end
    ) SMS_OFF
  from mon.spark_ft_billed_transaction_prepaid
  where transaction_date like '###SLICE_VALUE###%'
  group by served_party
) T lateral view explode(split(concat_ws(',', concat('CALLS_ON_N|', CALLS_ON_N), concat('CALLS_OFF_N|', CALLS_OFF_N), concat('CALLS_ON_D|', CALLS_ON_D), concat('CALLS_OFF_D|', CALLS_OFF_D), concat('SMS_ON|', SMS_ON), concat('SMS_OFF|', SMS_OFF)),',')) valeur_colonne AS valeur_colonne
