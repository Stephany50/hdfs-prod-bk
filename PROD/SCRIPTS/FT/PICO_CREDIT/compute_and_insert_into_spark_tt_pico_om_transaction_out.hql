INSERT INTO TABLE MON.SPARK_TT_PICO_KPIS PARTITION(EVENT_MONTH)
SELECT
    split(valeur_colonne,'[|]')[0] KPI
    ,sender_msisdn MSISDN
    ,NVL(split(valeur_colonne,'[|]')[1], 0) VAL
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,'###SLICE_VALUE###' EVENT_MONTH
FROM
(
  select
    sender_msisdn,
    sum(
    case when service_type='CASHOUT' and (substr(sender_category_code, 0, 3)='B2W' or substr(sender_domain_code, 0, 3)='B2W')
    then transaction_amount
    else 0 end
    ) OM_BANK_TRF_AMT,
    sum(
    case when service_type='CASHOUT' and (substr(sender_category_code, 0, 3)='B2W' or substr(sender_domain_code, 0, 3)='B2W')
    then 1
    else 0 end
    ) OM_BANK_TRF_QTY,
    sum(
    case when service_type='RC'
    then transaction_amount
    else 0 end
    ) OM_INC_AMT,
    sum(
    case when service_type='RC'
    then 1
    else 0 end
    ) OM_INC_QTY,
    sum(
    case when service_type='MERCHPAY' and receiver_msisdn in ('698066666', '658101010', '658121212', '691724895', '693801855', '691110998', '694308925', '691720687', '691940454', '691622688', '691212249', '691211457', '691211426', '691211407', '691211384', '691211300', '691211294', '691210767', '691203011', '691202290', '691212442', '691300807', '691633320', '691724958', '691017480', '691673659', '691013822', '691972383', '691961236', '691724967')
    then transaction_amount
    else 0 end
    ) OM_B2C_RECV_AMT,
    sum(
    case when service_type='MERCHPAY' and receiver_msisdn in ('698066666', '658101010', '658121212', '691724895', '693801855', '691110998', '694308925', '691720687', '691940454', '691622688', '691212249', '691211457', '691211426', '691211407', '691211384', '691211300', '691211294', '691210767', '691203011', '691202290', '691212442', '691300807', '691633320', '691724958', '691017480', '691673659', '691013822', '691972383', '691961236', '691724967')
    then 1
    else 0 end
    ) OM_B2C_RECV_QTY
  from cdr.spark_it_omny_transactions
  where transfer_datetime like '###SLICE_VALUE###%'
  group by sender_msisdn
) T lateral view explode(split(concat_ws(',', concat('OM_BANK_TRF_AMT|', OM_BANK_TRF_AMT), concat('OM_BANK_TRF_QTY|', OM_BANK_TRF_QTY), concat('OM_INC_AMT|', OM_INC_AMT), concat('OM_INC_QTY|', OM_INC_QTY), concat('OM_B2C_RECV_AMT|', OM_B2C_RECV_AMT), concat('OM_B2C_RECV_QTY|', OM_B2C_RECV_QTY)),',')) valeur_colonne AS valeur_colonne
