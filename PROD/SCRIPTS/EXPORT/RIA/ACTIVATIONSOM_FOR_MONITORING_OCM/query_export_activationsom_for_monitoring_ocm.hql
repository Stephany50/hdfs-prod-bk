SELECT 
distinct date_format(registered_on,'dd.MM.yyyy hh:mm:ss') registered_on,
user_id,
msisdn,
CREATED_BY_MSISDN identificateur,
USER_TYPE
from cdr.spark_it_om_subscribers
where modification_date = '###SLICE_VALUE###'