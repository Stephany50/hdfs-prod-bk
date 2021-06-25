

insert into mon.spark_FT_A_REFILL_SENDER
select
    SENDER_MSISDN
    , REFILL_TYPE
    , REFILL_MEAN
    , SENDER_PROFILE
    , sum(REFILL_AMOUNT) REFILL_AMOUNT
    , sum(REFILL_BONUS) REFILL_BONUS
    , NULL REFILL_DISCOUNT
    , count(*) REFILL_COUNT
    , SENDER_OPERATOR_CODE OPERATOR_CODE
    , current_timestamp insert_date
    , substr('###SLICE_VALUE###', 1, 7) REFILL_MONTH
from MON.spark_FT_REFILL
where refill_date between '###SLICE_VALUE###'||'-01' AND LAST_DAY('###SLICE_VALUE###'||'-01')
    and termination_ind='200' 
    --and REFILL_MEAN<>'SCRATCH'
group by substr('###SLICE_VALUE###', 1, 7)
    , SENDER_MSISDN
    , REFILL_TYPE
    , REFILL_MEAN
    , SENDER_PROFILE  
    , SENDER_OPERATOR_CODE
