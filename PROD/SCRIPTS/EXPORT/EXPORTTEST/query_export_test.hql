select *
from mon.spark_ft_billed_transaction_prepaid
where transaction_date = '###SLICE_VALUE###'
limit 1000