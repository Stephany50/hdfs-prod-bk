insert into mon.spark_ft_msisdn_da_status
select
    DISTINCT acc_nbr msisdn
    , ACCT_RES_NAME da_id
    , total_in_NB_or_FCFA_or_MB volume_remaining
    , ACCT_RES_RATING_SERVICE_CODE da_type
    , acct_res_rating_unit da_unit
    , EXP_DATE expiry_date
    , EFF_DATE
    , UPDATE_DATE
    , current_timestamp insert_date
    , '2021-03-27' event_date
from 
(
    select 
        acc_nbr
        , ACCT_RES_NAME
        , case 
            when upper(ACCT_RES_RATING_SERVICE_CODE)='TEL' THEN -(gross_bal+reserve_bal+consume_bal)/(100) 
            WHEN upper(ACCT_RES_RATING_SERVICE_CODE)='DATA' THEN -(gross_bal+reserve_bal+consume_bal)/(1024*1024)
            ELSE -(gross_bal+reserve_bal+consume_bal)/(1) 
        END total_in_NB_or_FCFA_or_MB
        , bal.EFF_DATE EFF_DATE
        , bal.EXP_DATE EXP_DATE
        , bal.UPDATE_DATE UPDATE_DATE
        , ACCT_RES_RATING_SERVICE_CODE
        , acct_res_rating_unit
    from
    (
        select * from cdr.spark_IT_ZTE_BAL_EXTRACT where original_file_date = '2021-03-27'
    ) bal --obtention des différents sousc comptes à un moment donné
    left join
    (
        select * from CDR.spark_IT_ZTE_SUBS_EXTRACT where original_file_date  = '2021-03-27'
    ) subs on bal.acct_id = subs.acct_id -- jointure pour l'obtention des numéros
    left join DIM.DT_BALANCE_TYPE_ITEM dim on bal.acct_res_id = dim.acct_res_id
) a

