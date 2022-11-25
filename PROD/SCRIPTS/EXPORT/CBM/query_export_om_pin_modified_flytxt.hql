select
    msisdn, 
    transaction_on pin_modification_date, 
    original_file_date 
from CDR.SPARK_IT_OMNY_PIN_MODIFIED 
where original_file_date = '###SLICE_VALUE###'
