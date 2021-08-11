select substr(transactionsn, 1, 19) 
from cdr.spark_it_zte_subscription 
where createddate = '###SLICE_VALUE###' and channel_id=11
