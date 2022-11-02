insert into dim.msisdn_sos_credit_non_conformes

select 
nvl(.msisdn, A.msisdn)
from
(
    select distinct msisdn 
    from mon.spark_ft_sos_credit_non_conformes 
    where event_date = date_sub('###SLICE_VALUE###', 1)
) A 
full outer join dim.msisdn_sos_credit_non_conformes B 
on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(B.msisdn)
where 
