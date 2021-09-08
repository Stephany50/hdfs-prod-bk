INSERT INTO MON.SPARK_FT_SVA_SIM_BACKUP
select  count(*) NBRE_SOUSCRIP, SUM(charge)/100  REVENUE, current_timestamp  INSERT_DATE, CREATE_DATE
from cdr.spark_it_zte_adjustment
where create_date ='###SLICE_VALUE###' and channel_id ='109'
group by create_date
order by create_date

