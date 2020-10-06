select *
from mon.ft_billed_transaction_prepaid
where transaction_date = '###SLICE_VALUE###'
limit 100