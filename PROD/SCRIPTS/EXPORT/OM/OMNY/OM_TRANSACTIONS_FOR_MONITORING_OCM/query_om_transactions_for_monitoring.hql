SELECT 
distinct registered_on,
user_id,
msisdn,
CREATED_BY_MSISDN,
USER_TYPE
from cdr.spark_it_om_subscribers
where modification_date = '###SLICE_VALUE###'