INSERT INTO TABLE MON.SPARK_TT_PICO_KPIS PARTITION(EVENT_MONTH)
SELECT
    split(valeur_colonne,'[|]')[0] KPI
    ,receiver_msisdn MSISDN
    ,NVL(split(valeur_colonne,'[|]')[1], 0) VAL
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,'###SLICE_VALUE###' EVENT_MONTH
FROM
(
  select
    receiver_msisdn,
    sum(
    case when service_type='CASHIN'
    then transaction_amount
    else 0 end
    ) OM_DEPOSIT_AMT,
    sum(
    case when service_type='CASHIN'
    then 1
    else 0 end
    ) OM_DEPOSIT_QTY,
    sum(
    case when (service_type='CASHIN' or service_type='P2P')
    then transaction_amount
    else 0 end
    ) OM_RECV_AMT,
    sum(
    case when service_type='P2P'
    then 1
    else 0 end
    ) OM_RECV_QTY,
    sum(
    case when service_type='ENT2REG'
    then transaction_amount
    else 0 end
    ) OM_AMT_SALARY,
    sum(
    case when service_type='CASHIN' and (substr(sender_category_code, 0, 3)='B2W' or substr(sender_domain_code, 0, 3)='B2W')
    then transaction_amount
    else 0 end
    ) BANK_OM_TRF_AMT,
    sum(
    case when service_type='CASHIN' and (substr(sender_category_code, 0, 3)='B2W' or substr(sender_domain_code, 0, 3)='B2W')
    then 1
    else 0 end
    ) BANK_OM_TRF_QTY
  from cdr.spark_it_omny_transactions
  where transfer_datetime like '###SLICE_VALUE###%'
  group by receiver_msisdn
) T lateral view explode(split(concat_ws(',', concat('OM_DEPOSIT_AMT|', OM_DEPOSIT_AMT), concat('OM_DEPOSIT_QTY|', OM_DEPOSIT_QTY), concat('OM_RECV_AMT|', OM_RECV_AMT), concat('OM_RECV_QTY|', OM_RECV_QTY), concat('OM_AMT_SALARY|', OM_AMT_SALARY), concat('BANK_OM_TRF_AMT|', BANK_OM_TRF_AMT), concat('BANK_OM_TRF_QTY|', BANK_OM_TRF_QTY)),',')) valeur_colonne AS valeur_colonne
