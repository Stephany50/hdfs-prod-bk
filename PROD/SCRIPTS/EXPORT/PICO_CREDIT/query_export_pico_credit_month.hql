select
    T5.USER_ID USER_ID,
    T5.SCORE_AGE AGE,
    T5.SCORE_AVERAGE_CALLS_ON_D AVERAGE_CALLS_ON_D,
    T5.SCORE_AVERAGE_CALLS_ON_N AVERAGE_CALLS_ON_N,
    T5.SCORE_AVERAGE_DEPOSIT_AMT_1M_3M AVERAGE_DEPOSIT_AMT_1M_3M,
    T5.SCORE_AVERAGE_DEPOSIT_AMT_4M_6M AVERAGE_DEPOSIT_AMT_4M_6M,
    T5.SCORE_INCOME_STREAM_1M INCOME_STREAM_1M,
    T5.SCORE_INCOME_STREAM_2M INCOME_STREAM_2M,
    T5.SCORE_INCOME_STREAM_3M INCOME_STREAM_3M,
    T5.SCORE_AVERAGE_INCOME_STREAM AVERAGE_INCOME_STREAM,
    T5.SCORE_OM_EXPERIENCE OM_EXPERIENCE,
    T5.SCORE_ON_TO_OFF_CALLS ON_TO_OFF_CALLS,
    T5.SCORE_SUBS_EXPERIENCE SUBS_EXPERIENCE,
    T5.SCORE_AVERAGE_ATC_LOANS_QTY_1M_6M AVERAGE_ATC_LOANS_QTY_1M_6M
from
(
    select
        T4.USER_ID USER_ID,
        (
            case 
                when T4.AGE <= 26 then 0
                when T4.AGE > 26 and T4.AGE <= 31 then 12
                when T4.AGE > 31 and T4.AGE <= 38 then 22
                when T4.AGE > 38 and T4.AGE <= 48 then 28
                else 46
            end
        ) SCORE_AGE,
        (
            case
                when T4.AVERAGE_CALLS_ON_D <= 5821 then 0
                when T4.AVERAGE_CALLS_ON_D > 5821 and T4.AVERAGE_CALLS_ON_D <= 13681 then 78
                when T4.AVERAGE_CALLS_ON_D > 13681 and T4.AVERAGE_CALLS_ON_D <= 31290 then 167
                else 216
            end
        ) SCORE_AVERAGE_CALLS_ON_D,
        (
            case
                when T4.AVERAGE_CALLS_ON_N <= 61 then 0
                when T4.AVERAGE_CALLS_ON_N > 61 and T4.AVERAGE_CALLS_ON_N <= 118 then 44
                when T4.AVERAGE_CALLS_ON_N > 118 and T4.AVERAGE_CALLS_ON_N <= 228 then 66
                else 88
            end
        ) SCORE_AVERAGE_CALLS_ON_N,
        (
            case
                when T4.AVERAGE_DEPOSIT_AMT_1M_3M <= 10600 then 0
                when T4.AVERAGE_DEPOSIT_AMT_1M_3M > 10600 and T4.AVERAGE_DEPOSIT_AMT_1M_3M <= 21450 then 18 
                when T4.AVERAGE_DEPOSIT_AMT_1M_3M > 21450 and T4.AVERAGE_DEPOSIT_AMT_1M_3M <= 40500 then 32 
                when T4.AVERAGE_DEPOSIT_AMT_1M_3M > 40500 and T4.AVERAGE_DEPOSIT_AMT_1M_3M <= 95275 then 42
                else 50 
            end
        ) SCORE_AVERAGE_DEPOSIT_AMT_1M_3M,
        (
            case 
                when T4.AVERAGE_DEPOSIT_AMT_4M_6M <= 5000 then 0
                when T4.AVERAGE_DEPOSIT_AMT_4M_6M > 5000 and T4.AVERAGE_DEPOSIT_AMT_4M_6M <= 13750 then 14
                when T4.AVERAGE_DEPOSIT_AMT_4M_6M > 13750 and T4.AVERAGE_DEPOSIT_AMT_4M_6M <= 30612 then 24
                when T4.AVERAGE_DEPOSIT_AMT_4M_6M > 30612 and T4.AVERAGE_DEPOSIT_AMT_4M_6M <= 77750 then 36
                else 42
            end
        ) SCORE_AVERAGE_DEPOSIT_AMT_4M_6M,
        (
            case
                when T4.INCOME_STREAM_1M <= 8538 then 0
                when T4.INCOME_STREAM_1M > 8538 and T4.INCOME_STREAM_1M <= 25000 then 11
                when T4.INCOME_STREAM_1M > 25000 and T4.INCOME_STREAM_1M <= 52257 then 27
                when T4.INCOME_STREAM_1M > 52257 and T4.INCOME_STREAM_1M <= 103116 then 48
                when T4.INCOME_STREAM_1M > 103116 and T4.INCOME_STREAM_1M <= 204564 then 65
                else 74
            end
        ) SCORE_INCOME_STREAM_1M,
        (
            case
                when T4.INCOME_STREAM_2M <= 12375 then 0
                when T4.INCOME_STREAM_2M > 12375 and T4.INCOME_STREAM_2M <= 21750 then 14
                when T4.INCOME_STREAM_2M > 21750 and T4.INCOME_STREAM_2M <= 48200 then 20
                when T4.INCOME_STREAM_2M > 48200 and T4.INCOME_STREAM_2M <= 97350 then 31
                else 38
            end
        ) SCORE_INCOME_STREAM_2M,
        (
            case
                when T4.INCOME_STREAM_3M <= 7000 then 0
                when T4.INCOME_STREAM_3M > 7000 and T4.INCOME_STREAM_3M <= 10525 then 12
                when T4.INCOME_STREAM_3M > 10525 and T4.INCOME_STREAM_3M <= 41290 then 22
                when T4.INCOME_STREAM_3M > 41290 and T4.INCOME_STREAM_3M <= 85000 then 34
                when T4.INCOME_STREAM_3M > 85000 and T4.INCOME_STREAM_3M <= 205895 then 42
                else 48
            end
        ) SCORE_INCOME_STREAM_3M,
        (
            case
                when T4.AVERAGE_INCOME_STREAM <= 9245 then 0
                when T4.AVERAGE_INCOME_STREAM > 9245 and T4.AVERAGE_INCOME_STREAM <= 15710 then 12.1
                when T4.AVERAGE_INCOME_STREAM > 15710 and T4.AVERAGE_INCOME_STREAM <= 28641 then 24.2
                when T4.AVERAGE_INCOME_STREAM > 28641 and T4.AVERAGE_INCOME_STREAM <= 54739 then 36.3
                when T4.AVERAGE_INCOME_STREAM > 54739 and T4.AVERAGE_INCOME_STREAM <= 99966 then 48.4
                when T4.AVERAGE_INCOME_STREAM > 99966 and T4.AVERAGE_INCOME_STREAM <= 218490 then 62.5
                else 96.6
            end
        ) SCORE_AVERAGE_INCOME_STREAM,
        (
            case
                when T4.OM_EXPERIENCE <= 25 then 0
                when T4.OM_EXPERIENCE > 25 and T4.OM_EXPERIENCE <= 37 then 8
                when T4.OM_EXPERIENCE > 37 and T4.OM_EXPERIENCE <= 45 then 21
                when T4.OM_EXPERIENCE > 45 and T4.OM_EXPERIENCE <= 63 then 36
                else 49
            end
        ) SCORE_OM_EXPERIENCE,
        (
            case
                when T4.ON_TO_OFF_CALLS <= 18.9286 then 0
                when T4.ON_TO_OFF_CALLS > 18.9286 and T4.ON_TO_OFF_CALLS <= 34.4863 then 22
                when T4.ON_TO_OFF_CALLS > 34.4863 and T4.ON_TO_OFF_CALLS <= 106 then 44
                else 66
            end
        ) SCORE_ON_TO_OFF_CALLS,
        (
            case
                when T4.SUBS_EXPERIENCE < 36 then 0
                when T4.SUBS_EXPERIENCE >= 36 and T4.SUBS_EXPERIENCE < 63 then 98
                when T4.SUBS_EXPERIENCE >= 63 and T4.SUBS_EXPERIENCE < 96 then 146
                when T4.SUBS_EXPERIENCE >= 96 and T4.SUBS_EXPERIENCE < 142 then 180
                else 212
            end
        ) SCORE_SUBS_EXPERIENCE,
        (
            case
                when T4.AVERAGE_ATC_LOANS_QTY_1M_6M <= 12 then 0
                else -500
            end
        ) SCORE_AVERAGE_ATC_LOANS_QTY_1M_6M
    from
    (
        select
            T3.USER_ID USER_ID,
            T3.AGE AGE,
            T3.CALLS_ON_D_1M_6M/6 AVERAGE_CALLS_ON_D,
            T3.CALLS_ON_N_1M_6M/6 AVERAGE_CALLS_ON_N,
            T3.DEPOSIT_AMT_1M_3M/3 AVERAGE_DEPOSIT_AMT_1M_3M,
            T3.DEPOSIT_AMT_4M_6M/3 AVERAGE_DEPOSIT_AMT_4M_6M,
            T3.INCOME_STREAM_1M INCOME_STREAM_1M,
            T3.INCOME_STREAM_2M INCOME_STREAM_2M,
            T3.INCOME_STREAM_3M INCOME_STREAM_3M,
            (T3.INCOME_STREAM_1M + T3.INCOME_STREAM_2M + T3.INCOME_STREAM_3M + T3.INCOME_STREAM_4M + T3.INCOME_STREAM_5M + T3.INCOME_STREAM_6M)/6 AVERAGE_INCOME_STREAM,
            T3.OM_EXPERIENCE OM_EXPERIENCE,
            (case when T3.CALLS_OFF_N_1M_6M != 0 then T3.CALLS_ON_N_1M_6M/T3.CALLS_OFF_N_1M_6M else 0 end) ON_TO_OFF_CALLS,
            T3.SUBS_EXPERIENCE SUBS_EXPERIENCE,
            T3.ATC_LOANS_QTY_1M_6M/6 AVERAGE_ATC_LOANS_QTY_1M_6M 
        from
        (
            select
                T2.USER_ID USER_ID,
                T2.AGE AGE,
                T2.OM_EXPERIENCE OM_EXPERIENCE,
                T2.SUBS_EXPERIENCE SUBS_EXPERIENCE,
                (T2.CALLS_ON_D_1M + T2.CALLS_ON_D_2M + T2.CALLS_ON_D_3M + T2.CALLS_ON_D_4M + T2.CALLS_ON_D_5M + T2.CALLS_ON_D_6M) CALLS_ON_D_1M_6M,
                (T2.CALLS_ON_N_1M + T2.CALLS_ON_N_2M + T2.CALLS_ON_N_3M + T2.CALLS_ON_N_4M + T2.CALLS_ON_N_5M + T2.CALLS_ON_N_6M) CALLS_ON_N_1M_6M,
                (T2.CALLS_OFF_N_1M + T2.CALLS_OFF_N_2M + T2.CALLS_OFF_N_3M + T2.CALLS_OFF_N_4M + T2.CALLS_OFF_N_5M + T2.CALLS_OFF_N_6M) CALLS_OFF_N_1M_6M,
                (T2.OM_DEPOSIT_AMT_1M + T2.OM_DEPOSIT_AMT_2M + T2.OM_DEPOSIT_AMT_3M ) DEPOSIT_AMT_1M_3M,
                (T2.OM_DEPOSIT_AMT_4M + T2.OM_DEPOSIT_AMT_5M + T2.OM_DEPOSIT_AMT_6M ) DEPOSIT_AMT_4M_6M,
                (T2.OM_RECV_AMT_1M + T2.OM_AMT_SALARY_1M + T2.OM_INC_AMT_1M + T2.OM_B2C_RECV_AMT_1M) INCOME_STREAM_1M,
                (T2.OM_RECV_AMT_2M + T2.OM_AMT_SALARY_2M + T2.OM_INC_AMT_2M + T2.OM_B2C_RECV_AMT_2M) INCOME_STREAM_2M,
                (T2.OM_RECV_AMT_3M + T2.OM_AMT_SALARY_3M + T2.OM_INC_AMT_3M + T2.OM_B2C_RECV_AMT_3M) INCOME_STREAM_3M,
                (T2.OM_RECV_AMT_4M + T2.OM_AMT_SALARY_4M + T2.OM_INC_AMT_4M + T2.OM_B2C_RECV_AMT_4M) INCOME_STREAM_4M,
                (T2.OM_RECV_AMT_5M + T2.OM_AMT_SALARY_5M + T2.OM_INC_AMT_5M + T2.OM_B2C_RECV_AMT_5M) INCOME_STREAM_5M,
                (T2.OM_RECV_AMT_6M + T2.OM_AMT_SALARY_6M + T2.OM_INC_AMT_6M + T2.OM_B2C_RECV_AMT_6M) INCOME_STREAM_6M,
                (T2.ATC_LOANS_QTY_1M + T2.ATC_LOANS_QTY_2M + T2.ATC_LOANS_QTY_3M + T2.ATC_LOANS_QTY_4M + T2.ATC_LOANS_QTY_5M + T2.ATC_LOANS_QTY_6M) ATC_LOANS_QTY_1M_6M
            from
            (
                select
                    T1.user_id USER_ID,
                    T1.SUBS_EXPERIENCE SUBS_EXPERIENCE,
                    T1.AGE AGE,
                    T1.OM_EXPERIENCE OM_EXPERIENCE,
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
                        max(snap_om.user_id) user_id,
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
                        select msisdn, is_active, birth_date, user_id, created_on
                        from cdr.spark_it_omny_account_snapshot
                        where original_file_date=CURRENT_DATE
                        and abs(cast(months_between(birth_date, CURRENT_DATE)as int))/12 between 18 and 80
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
                    order by rand()
                ) T1
                left join
                (
                    select * from MON.SPARK_TT_PICO_KPIS
                    where event_month between substr(add_months(current_date, -6), 0, 7) and substr(add_months(current_date, -1), 0, 7)
                ) T2 on (T1.msisdn = T2.msisdn)
                group by T1.msisdn, T1.SUBS_EXPERIENCE, T1.AGE, T1.OM_EXPERIENCE, T1.user_id
            ) T2
        ) T3
    ) T4
)T5