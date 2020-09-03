INSERT INTO TABLE MON.SPARK_TT_PICO_KPIS PARTITION(EVENT_MONTH)
SELECT
    split(valeur_colonne,'[|]')[0] KPI
    ,ACCT_ID_MSISDN MSISDN
    ,NVL(split(valeur_colonne,'[|]')[1], 0) VAL
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,'###SLICE_VALUE###' EVENT_MONTH
FROM
(
  select
    ACCT_ID_MSISDN,
    sum(
    case when loan_credit > 0
    then loan_credit
    else 0 end
    ) ATC_LOANS_AMT,
    sum(
    case when loan_credit > 0
    then 1
    else 0 end
    ) ATC_LOANS_QTY,
    sum(
    case when main_credit > 0
    then main_credit
    else 0 end
    ) TELCO_CR,
    sum(
    case when main_credit > 0
    then 1
    else 0 end
    ) TELCO_CR_N
  from MON.SPARK_FT_EDR_PRPD_EQT
  where EVENT_DATE like '###SLICE_VALUE###%'
  group by ACCT_ID_MSISDN
) T lateral view explode(split(concat_ws(',', concat('ATC_LOANS_AMT|', ATC_LOANS_AMT), concat('ATC_LOANS_QTY|', ATC_LOANS_QTY), concat('TELCO_CR|', TELCO_CR), concat('TELCO_CR_N|', TELCO_CR_N)),',')) valeur_colonne AS valeur_colonne
