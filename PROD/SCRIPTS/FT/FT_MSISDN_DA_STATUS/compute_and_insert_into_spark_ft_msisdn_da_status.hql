insert into mon.spark_ft_msisdn_da_status
select
    DISTINCT acc_nbr msisdn
    , bal_id
    , ACCT_RES_NAME da_name
    , total_in_NB_or_FCFA_or_MB volume_remaining
    , ACCT_RES_RATING_SERVICE_CODE da_type
    , acct_res_rating_unit da_unit
    , EXP_DATE expiry_date
    , EFF_DATE
    , UPDATE_DATE
    , current_timestamp insert_date
    , date_sub('###SLICE_VALUE###', 1) event_date
from 
(
    select 
        acc_nbr
        , bal_id
        , ACCT_RES_NAME
        , case 
            when upper(ACCT_RES_RATING_SERVICE_CODE)='TEL' THEN -(nvl(gross_bal, 0)+nvl(reserve_bal, 0)+nvl(consume_bal, 0))/(100) 
            WHEN upper(ACCT_RES_RATING_SERVICE_CODE)='DATA' THEN -(nvl(gross_bal, 0)+nvl(reserve_bal, 0)+nvl(consume_bal, 0))/(1024*1024)
            ELSE -(nvl(gross_bal, 0)+nvl(reserve_bal, 0)+nvl(consume_bal, 0))/(1) 
        END total_in_NB_or_FCFA_or_MB
        , bal.EFF_DATE EFF_DATE
        , bal.EXP_DATE EXP_DATE
        , bal.UPDATE_DATE UPDATE_DATE
        , ACCT_RES_RATING_SERVICE_CODE
        , acct_res_rating_unit
    from
    (
        --select * from cdr.spark_IT_ZTE_BAL_EXTRACT where original_file_date = '###SLICE_VALUE###'
        SELECT
            *
            --, row_number() over (partition by acct_id, acct_res_id order by update_date desc) rn
        FROM CDR.SPARK_IT_ZTE_BAL_EXTRACT
        WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###'
    ) bal --obtention des différents sous comptes à un moment donné
    left join
    (
        select * from CDR.spark_IT_ZTE_SUBS_EXTRACT where original_file_date = '###SLICE_VALUE###'
    ) subs on bal.acct_id = subs.acct_id -- jointure pour l'obtention des numéros
    left join DIM.DT_BALANCE_TYPE_ITEM dim on bal.acct_res_id = dim.acct_res_id
    --WHERE bal.RN = 1
) a

