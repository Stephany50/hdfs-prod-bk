create table tmp.pico_sample as
select
    T1.msisdn,
    T1.SUBS_EXPERIENCE,
    T1.AGE,
    T1.OM_EXPERIENCE,
    sum (
    case when kpi='TELCO_LOANS_DELAY'
    then val
    else 0 end
    ) TELCO_LOANS_DELAY,
    sum (
    case when kpi='OM_DEPOSIT_AMT' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) OM_DEPOSIT_AMT_1M,
    sum (
    case when kpi='OM_DEPOSIT_AMT' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) OM_DEPOSIT_AMT_2M,
    sum (
    case when kpi='OM_DEPOSIT_AMT' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) OM_DEPOSIT_AMT_3M,
    sum (
    case when kpi='OM_DEPOSIT_AMT' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) OM_DEPOSIT_AMT_4M,
    sum (
    case when kpi='OM_DEPOSIT_AMT' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) OM_DEPOSIT_AMT_5M,
    sum (
    case when kpi='OM_DEPOSIT_AMT' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) OM_DEPOSIT_AMT_6M,
    sum (
    case when kpi='OM_DEPOSIT_QTY' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) OM_DEPOSIT_QTY_1M,
    sum (
    case when kpi='OM_DEPOSIT_QTY' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) OM_DEPOSIT_QTY_2M,
    sum (
    case when kpi='OM_DEPOSIT_QTY' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) OM_DEPOSIT_QTY_3M,
    sum (
    case when kpi='OM_DEPOSIT_QTY' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) OM_DEPOSIT_QTY_4M,
    sum (
    case when kpi='OM_DEPOSIT_QTY' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) OM_DEPOSIT_QTY_5M,
    sum (
    case when kpi='OM_DEPOSIT_QTY' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) OM_DEPOSIT_QTY_6M,
    sum (
    case when kpi='OM_RECV_AMT' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) OM_RECV_AMT_1M,
    sum (
    case when kpi='OM_RECV_AMT' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) OM_RECV_AMT_2M,
    sum (
    case when kpi='OM_RECV_AMT' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) OM_RECV_AMT_3M,
    sum (
    case when kpi='OM_RECV_AMT' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) OM_RECV_AMT_4M,
    sum (
    case when kpi='OM_RECV_AMT' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) OM_RECV_AMT_5M,
    sum (
    case when kpi='OM_RECV_AMT' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) OM_RECV_AMT_6M,
    sum (
    case when kpi='OM_RECV_QTY' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) OM_RECV_QTY_1M,
    sum (
    case when kpi='OM_RECV_QTY' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) OM_RECV_QTY_2M,
    sum (
    case when kpi='OM_RECV_QTY' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) OM_RECV_QTY_3M,
    sum (
    case when kpi='OM_RECV_QTY' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) OM_RECV_QTY_4M,
    sum (
    case when kpi='OM_RECV_QTY' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) OM_RECV_QTY_5M,
    sum (
    case when kpi='OM_RECV_QTY' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) OM_RECV_QTY_6M,
    sum (
    case when kpi='OM_AMT_SALARY' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) OM_AMT_SALARY_1M,
    sum (
    case when kpi='OM_AMT_SALARY' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) OM_AMT_SALARY_2M,
    sum (
    case when kpi='OM_AMT_SALARY' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) OM_AMT_SALARY_3M,
    sum (
    case when kpi='OM_AMT_SALARY' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) OM_AMT_SALARY_4M,
    sum (
    case when kpi='OM_AMT_SALARY' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) OM_AMT_SALARY_5M,
    sum (
    case when kpi='OM_AMT_SALARY' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) OM_AMT_SALARY_6M,
    sum (
    case when kpi='BANK_OM_TRF_AMT' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) BANK_OM_TRF_AMT_1M,
    sum (
    case when kpi='BANK_OM_TRF_AMT' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) BANK_OM_TRF_AMT_2M,
    sum (
    case when kpi='BANK_OM_TRF_AMT' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) BANK_OM_TRF_AMT_3M,
    sum (
    case when kpi='BANK_OM_TRF_AMT' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) BANK_OM_TRF_AMT_4M,
    sum (
    case when kpi='BANK_OM_TRF_AMT' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) BANK_OM_TRF_AMT_5M,
    sum (
    case when kpi='BANK_OM_TRF_AMT' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) BANK_OM_TRF_AMT_6M,
    sum (
    case when kpi='BANK_OM_TRF_QTY' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) BANK_OM_TRF_QTY_1M,
    sum (
    case when kpi='BANK_OM_TRF_QTY' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) BANK_OM_TRF_QTY_2M,
    sum (
    case when kpi='BANK_OM_TRF_QTY' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) BANK_OM_TRF_QTY_3M,
    sum (
    case when kpi='BANK_OM_TRF_QTY' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) BANK_OM_TRF_QTY_4M,
    sum (
    case when kpi='BANK_OM_TRF_QTY' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) BANK_OM_TRF_QTY_5M,
    sum (
    case when kpi='BANK_OM_TRF_QTY' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) BANK_OM_TRF_QTY_6M,
    sum (
    case when kpi='OM_BANK_TRF_AMT' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) OM_BANK_TRF_AMT_1M,
    sum (
    case when kpi='OM_BANK_TRF_AMT' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) OM_BANK_TRF_AMT_2M,
    sum (
    case when kpi='OM_BANK_TRF_AMT' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) OM_BANK_TRF_AMT_3M,
    sum (
    case when kpi='OM_BANK_TRF_AMT' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) OM_BANK_TRF_AMT_4M,
    sum (
    case when kpi='OM_BANK_TRF_AMT' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) OM_BANK_TRF_AMT_5M,
    sum (
    case when kpi='OM_BANK_TRF_AMT' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) OM_BANK_TRF_AMT_6M,
    sum (
    case when kpi='OM_BANK_TRF_QTY' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) OM_BANK_TRF_QTY_1M,
    sum (
    case when kpi='OM_BANK_TRF_QTY' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) OM_BANK_TRF_QTY_2M,
    sum (
    case when kpi='OM_BANK_TRF_QTY' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) OM_BANK_TRF_QTY_3M,
    sum (
    case when kpi='OM_BANK_TRF_QTY' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) OM_BANK_TRF_QTY_4M,
    sum (
    case when kpi='OM_BANK_TRF_QTY' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) OM_BANK_TRF_QTY_5M,
    sum (
    case when kpi='OM_BANK_TRF_QTY' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) OM_BANK_TRF_QTY_6M,
    sum (
    case when kpi='OM_INC_AMT' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) OM_INC_AMT_1M,
    sum (
    case when kpi='OM_INC_AMT' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) OM_INC_AMT_2M,
    sum (
    case when kpi='OM_INC_AMT' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) OM_INC_AMT_3M,
    sum (
    case when kpi='OM_INC_AMT' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) OM_INC_AMT_4M,
    sum (
    case when kpi='OM_INC_AMT' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) OM_INC_AMT_5M,
    sum (
    case when kpi='OM_INC_AMT' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) OM_INC_AMT_6M,
    sum (
    case when kpi='OM_INC_QTY' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) OM_INC_QTY_1M,
    sum (
    case when kpi='OM_INC_QTY' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) OM_INC_QTY_2M,
    sum (
    case when kpi='OM_INC_QTY' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) OM_INC_QTY_3M,
    sum (
    case when kpi='OM_INC_QTY' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) OM_INC_QTY_4M,
    sum (
    case when kpi='OM_INC_QTY' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) OM_INC_QTY_5M,
    sum (
    case when kpi='OM_INC_QTY' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) OM_INC_QTY_6M,
    sum (
    case when kpi='OM_B2C_RECV_AMT' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) OM_B2C_RECV_AMT_1M,
    sum (
    case when kpi='OM_B2C_RECV_AMT' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) OM_B2C_RECV_AMT_2M,
    sum (
    case when kpi='OM_B2C_RECV_AMT' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) OM_B2C_RECV_AMT_3M,
    sum (
    case when kpi='OM_B2C_RECV_AMT' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) OM_B2C_RECV_AMT_4M,
    sum (
    case when kpi='OM_B2C_RECV_AMT' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) OM_B2C_RECV_AMT_5M,
    sum (
    case when kpi='OM_B2C_RECV_AMT' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) OM_B2C_RECV_AMT_6M,
    sum (
    case when kpi='OM_B2C_RECV_QTY' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) OM_B2C_RECV_QTY_1M,
    sum (
    case when kpi='OM_B2C_RECV_QTY' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) OM_B2C_RECV_QTY_2M,
    sum (
    case when kpi='OM_B2C_RECV_QTY' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) OM_B2C_RECV_QTY_3M,
    sum (
    case when kpi='OM_B2C_RECV_QTY' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) OM_B2C_RECV_QTY_4M,
    sum (
    case when kpi='OM_B2C_RECV_QTY' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) OM_B2C_RECV_QTY_5M,
    sum (
    case when kpi='OM_B2C_RECV_QTY' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) OM_B2C_RECV_QTY_6M,
    sum (
    case when kpi='ATC_LOANS_AMT' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) ATC_LOANS_AMT_1M,
    sum (
    case when kpi='ATC_LOANS_AMT' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) ATC_LOANS_AMT_2M,
    sum (
    case when kpi='ATC_LOANS_AMT' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) ATC_LOANS_AMT_3M,
    sum (
    case when kpi='ATC_LOANS_AMT' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) ATC_LOANS_AMT_4M,
    sum (
    case when kpi='ATC_LOANS_AMT' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) ATC_LOANS_AMT_5M,
    sum (
    case when kpi='ATC_LOANS_AMT' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) ATC_LOANS_AMT_6M,
    sum (
    case when kpi='ATC_LOANS_QTY' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) ATC_LOANS_QTY_1M,
    sum (
    case when kpi='ATC_LOANS_QTY' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) ATC_LOANS_QTY_2M,
    sum (
    case when kpi='ATC_LOANS_QTY' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) ATC_LOANS_QTY_3M,
    sum (
    case when kpi='ATC_LOANS_QTY' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) ATC_LOANS_QTY_4M,
    sum (
    case when kpi='ATC_LOANS_QTY' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) ATC_LOANS_QTY_5M,
    sum (
    case when kpi='ATC_LOANS_QTY' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) ATC_LOANS_QTY_6M,
    sum (
    case when kpi='TELCO_CR' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) TELCO_CR_1M,
    sum (
    case when kpi='TELCO_CR' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) TELCO_CR_2M,
    sum (
    case when kpi='TELCO_CR' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) TELCO_CR_3M,
    sum (
    case when kpi='TELCO_CR' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) TELCO_CR_4M,
    sum (
    case when kpi='TELCO_CR' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) TELCO_CR_5M,
    sum (
    case when kpi='TELCO_CR' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) TELCO_CR_6M,
     sum (
     case when kpi='TELCO_CR_N' and event_month = substr(add_months(current_date, -1), 0, 7)
     then val
     else 0 end
     ) TELCO_CR_N_1M,
     sum (
     case when kpi='TELCO_CR_N' and event_month = substr(add_months(current_date, -2), 0, 7)
     then val
     else 0 end
     ) TELCO_CR_N_2M,
     sum (
     case when kpi='TELCO_CR_N' and event_month = substr(add_months(current_date, -3), 0, 7)
     then val
     else 0 end
     ) TELCO_CR_N_3M,
     sum (
     case when kpi='TELCO_CR_N' and event_month = substr(add_months(current_date, -4), 0, 7)
     then val
     else 0 end
     ) TELCO_CR_N_4M,
     sum (
     case when kpi='TELCO_CR_N' and event_month = substr(add_months(current_date, -5), 0, 7)
     then val
     else 0 end
     ) TELCO_CR_N_5M,
     sum (
     case when kpi='TELCO_CR_N' and event_month = substr(add_months(current_date, -6), 0, 7)
     then val
     else 0 end
     ) TELCO_CR_N_6M,
    sum (
    case when kpi='CALLS_ON_N' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) CALLS_ON_N_1M,
    sum (
    case when kpi='CALLS_ON_N' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) CALLS_ON_N_2M,
    sum (
    case when kpi='CALLS_ON_N' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) CALLS_ON_N_3M,
    sum (
    case when kpi='CALLS_ON_N' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) CALLS_ON_N_4M,
    sum (
    case when kpi='CALLS_ON_N' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) CALLS_ON_N_5M,
    sum (
    case when kpi='CALLS_ON_N' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) CALLS_ON_N_6M,
    sum (
    case when kpi='CALLS_OFF_N' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) CALLS_OFF_N_1M,
    sum (
    case when kpi='CALLS_OFF_N' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) CALLS_OFF_N_2M,
    sum (
    case when kpi='CALLS_OFF_N' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) CALLS_OFF_N_3M,
    sum (
    case when kpi='CALLS_OFF_N' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) CALLS_OFF_N_4M,
    sum (
    case when kpi='CALLS_OFF_N' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) CALLS_OFF_N_5M,
    sum (
    case when kpi='CALLS_OFF_N' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) CALLS_OFF_N_6M,
    sum (
    case when kpi='CALLS_ON_D' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) CALLS_ON_D_1M,
    sum (
    case when kpi='CALLS_ON_D' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) CALLS_ON_D_2M,
    sum (
    case when kpi='CALLS_ON_D' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) CALLS_ON_D_3M,
    sum (
    case when kpi='CALLS_ON_D' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) CALLS_ON_D_4M,
    sum (
    case when kpi='CALLS_ON_D' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) CALLS_ON_D_5M,
    sum (
    case when kpi='CALLS_ON_D' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) CALLS_ON_D_6M,
    sum (
    case when kpi='CALLS_OFF_D' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) CALLS_OFF_D_1M,
    sum (
    case when kpi='CALLS_OFF_D' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) CALLS_OFF_D_2M,
    sum (
    case when kpi='CALLS_OFF_D' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) CALLS_OFF_D_3M,
    sum (
    case when kpi='CALLS_OFF_D' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) CALLS_OFF_D_4M,
    sum (
    case when kpi='CALLS_OFF_D' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) CALLS_OFF_D_5M,
    sum (
    case when kpi='CALLS_OFF_D' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) CALLS_OFF_D_6M,
    sum (
    case when kpi='SMS_ON' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) SMS_ON_1M,
    sum (
    case when kpi='SMS_ON' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) SMS_ON_2M,
    sum (
    case when kpi='SMS_ON' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) SMS_ON_3M,
    sum (
    case when kpi='SMS_ON' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) SMS_ON_4M,
    sum (
    case when kpi='SMS_ON' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) SMS_ON_5M,
    sum (
    case when kpi='SMS_ON' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) SMS_ON_6M,
    sum (
    case when kpi='SMS_OFF' and event_month = substr(add_months(current_date, -1), 0, 7)
    then val
    else 0 end
    ) SMS_OFF_1M,
    sum (
    case when kpi='SMS_OFF' and event_month = substr(add_months(current_date, -2), 0, 7)
    then val
    else 0 end
    ) SMS_OFF_2M,
    sum (
    case when kpi='SMS_OFF' and event_month = substr(add_months(current_date, -3), 0, 7)
    then val
    else 0 end
    ) SMS_OFF_3M,
    sum (
    case when kpi='SMS_OFF' and event_month = substr(add_months(current_date, -4), 0, 7)
    then val
    else 0 end
    ) SMS_OFF_4M,
    sum (
    case when kpi='SMS_OFF' and event_month = substr(add_months(current_date, -5), 0, 7)
    then val
    else 0 end
    ) SMS_OFF_5M,
    sum (
    case when kpi='SMS_OFF' and event_month = substr(add_months(current_date, -6), 0, 7)
    then val
    else 0 end
    ) SMS_OFF_6M,
    CURRENT_TIMESTAMP INSERT_DATE
from
(
    select
        snap_telco.access_key msisdn,
        max(abs(cast(months_between(snap_telco.activation_date, CURRENT_DATE)as int))) SUBS_EXPERIENCE,
        cast(max(abs(cast(months_between(snap_om.birth_date, CURRENT_DATE)as int)))/12 as int) AGE,
        max(abs(cast(months_between(snap_om.created_on, CURRENT_DATE)as int))) OM_EXPERIENCE
    from
        (
        select access_key, osp_status, activation_date
        from mon.spark_ft_contract_snapshot
        where event_date=CURRENT_DATE
        and abs(cast(months_between(activation_date, CURRENT_DATE)as int)) > 6
        and osp_status='ACTIVE'
        ) snap_telco
        inner join
        (
        select msisdn, is_active, birth_date, created_on
        from cdr.spark_it_omny_account_snapshot
        where original_file_date=CURRENT_DATE
        and abs(cast(months_between(birth_date, CURRENT_DATE)as int))/12 between 21 and 80
        and is_active='Y'
        ) snap_om on (snap_telco.access_key = snap_om.msisdn)
        inner join
        (
        select T.*
        from
            (
            select
                receiver_msisdn,
                sum(
                case when service_type='CASHIN' and transfer_datetime between date_add(last_day(add_months(current_date, -2)),1) and last_day(add_months(current_date, -1))
                then transaction_amount
                else 0 end
                ) montant_cashin
            from cdr.spark_it_omny_transactions
            where
            transfer_datetime > date_sub(current_date, 180)
            group by receiver_msisdn
            ) T
        where T.montant_cashin > 3000
        ) transactions on (snap_telco.access_key = transactions.receiver_msisdn)
    group by snap_telco.access_key
    order by rand() limit 10000
) T1
left join
(
    select * from MON.SPARK_TT_PICO_KPIS
    where event_month between substr(add_months(current_date, -6), 0, 7) and substr(add_months(current_date, -1), 0, 7)
) T2 on (T1.msisdn = T2.msisdn)
group by T1.msisdn, T1.SUBS_EXPERIENCE, T1.AGE, T1.OM_EXPERIENCE
