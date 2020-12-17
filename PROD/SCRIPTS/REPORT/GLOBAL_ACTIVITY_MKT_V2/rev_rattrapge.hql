select category,sum(available_balance/100)  TOTAL_AMOUNT from CDR.SPARK_IT_ZEBRA_MASTER_BALANCE
    where event_date ='2019-11-27' and event_time in (select max(event_time) from cdr.spark_IT_ZEBRA_MASTER_BALANCE where event_date ='2019-11-27') and CATEGORY in ('POS','New POS','Partner POS') AND
    USER_STATUS = 'Y' group by category