select
substr(served_party, -9) served_party,
substr(served_party, -9) other_party,
transaction_time,
transaction_date,
nvl(location_ci_decimal,'N/A') location_ci_decimal,
rated_duration
from mon.spark_ft_billed_transaction_prepaid
where transaction_date='###SLICE_VALUE###' and upper(service_code) like '%TEL%'