insert into TT.TMP_TRUNCK_SIP_TRANSACT
select 
    b.PARTNER, 
    TRUNCK_IN faisceaux, 
    trunck_out, 
    TRANSACTION_DATE, 
    transaction_time, 
    OLD_CALLING_NUMBER, 
    regexp_replace(OLD_CALLED_NUMBER, '^9900959', '') OLD_CALLED_NUMBER, 
    fn_get_nnp_msisdn_simple_destn(regexp_replace(OLD_CALLED_NUMBER, '^9900959', '')) DESTINATION, 
    TRANSACTION_DURATION, 
    SERVED_MSISDN, 
    OTHER_PARTY
from MON.SPARK_FT_MSC_TRANSACTION a 
join DIM.DT_B2B_MSISDN b ON (OLD_CALLING_NUMBER = b.MSISDN  or substr(OLD_CALLING_NUMBER, -9) = b.MSISDN)
where date_format(transaction_date, 'yyyy-MM') = '###SLICE_VALUE###'
